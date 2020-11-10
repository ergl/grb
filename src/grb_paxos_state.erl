-module(grb_paxos_state).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(leader, leader).
-define(follower, follower).
-define(heartbeat, heartbeat).
-type status() :: ?leader | ?follower.

-define(abort_conflict, {abort, conflict}).
-define(abort_stale_dec, {abort, stale_decided}).
-define(abort_stale_comm, {abort, stale_committed}).

%% How many commit times to delete at once
%% If this is too low, we could spend quite a bit of time deleting
%% If the value is too high, we could hurt performance from copying too much
%% todo(borja, red): Make config for this
-define(PRUNE_LIMIT, 50).

%% We don't care about the structure too much, as long as the client ensures identifiers
%% are unique (or at least, unique to the point of not reusing them before they call
%% `prune_decided_before/2`
-type record_id() :: {?heartbeat, term()} | term().
-record(tx_data, {
    read_keys = [] :: readset() | undefined,
    write_keys = [] :: [key()] | undefined,
    writeset = #{} :: #{} | undefined,
    red_ts :: grb_time:ts(),
    clock :: vclock() | grb_time:ts(),
    state :: prepared | decided,
    vote :: red_vote()
}).
-type tx_data() :: #tx_data{}.

%% The compound key disallows commit time clashes
%% between different transactions
-record(index_entry, {
    key :: {grb_time:ts(), record_id()},
    state :: prepared | decided,
    vote :: red_vote()
}).

-record(state, {
    status :: status(),
    ballot = 0 :: ballot(),
    entries = #{} :: #{record_id() => tx_data()},
    index :: cache_id(),

    %% A pair of inverted indices over the transactions read/write sets
    %% Used to compute write-write and read-write conflicts in a quick way
    pending_reads :: cache_id(),
    writes_cache :: cache_id(),

    %% An ETS table for buffered DECISION messages
    %% The client can "reserve" a spot in the commit order using `reserve_decision/4`
    pending_commit_ts :: cache_id()
}).

-type prepare_hb_result() :: {red_vote(), ballot(), grb_time:ts()}| {already_decided, red_vote(), grb_time:ts()}.
-type prepare_result() :: {red_vote(), ballot(), vclock()} | {already_decided, red_vote(), vclock()}.
-type decision_error() :: bad_ballot | not_prepared | not_ready.
-type t() :: #state{}.

-export_type([t/0, prepare_result/0, prepare_hb_result/0, decision_error/0]).

%% constructors
-export([leader/0,
         follower/0,
         delete/1]).

%% ready / prune api
-export([get_next_ready/2,
         prune_decided_before/2]).

%% heartbeat api
-export([prepare_hb/2,
         accept_hb/4,
         decision_hb/4]).

%% transactional api
-export([prepare/6,
         accept/7,
         decision/5]).

%% Buffer decision spot
-export([reserve_decision/4]).

%%%===================================================================
%%% constructors
%%%===================================================================

-spec leader() -> t().
leader() -> fresh_state(?leader).

-spec follower() -> t().
follower() -> fresh_state(?follower).

-spec fresh_state(status()) -> t().
fresh_state(Status) ->
    #state{status=Status,
           index=ets:new(state_entries_index, [ordered_set, {keypos, #index_entry.key}]),
           pending_reads=ets:new(state_pending_reads, [bag]),
           writes_cache=ets:new(state_pending_writes, [ordered_set]),
           pending_commit_ts=ets:new(state_pending_decisions, [ordered_set])}.

-spec delete(t()) -> ok.
delete(#state{index=Index, pending_reads=PendingReads, writes_cache=Writes, pending_commit_ts=PendingData}) ->
    true = ets:delete(Index),
    true = ets:delete(PendingReads),
    true = ets:delete(Writes),
    true = ets:delete(PendingData),
    ok.

%%%===================================================================
%%% ready / prune
%%%===================================================================

-spec get_next_ready(grb_time:ts(), t()) -> false | {grb_time:ts(), [ ?heartbeat | {#{}, vclock()} ]}.
-dialyzer({nowarn_function, get_next_ready/2}).
get_next_ready(LastDelivered, #state{entries=EntryMap, index=Idx, pending_commit_ts=PendingData}) ->
    Res = ets:select(Idx, [{ #index_entry{key={'$1', '$2'}, state=decided, vote=ok},
                                  [{'>', '$1', LastDelivered}],
                                  [{{'$1', '$2'}}] }],
                               1),
    case Res of
        {[{CommitTime, FirstCommitId}], _} ->
            IsBlocked = buffered_between(LastDelivered, CommitTime, PendingData)
                orelse prep_committed_between(LastDelivered, CommitTime, Idx),
            case IsBlocked of
                true ->
                    false;
                false ->
                    %% get the rest of committed transactions with this commit time
                    %% since they have the same committed time, they all pass the check above
                    MoreIds = ets:select(Idx, [{ #index_entry{key={CommitTime, '$1'}, state=decided, vote=ok},
                                                 [{'=/=', '$1', {const, FirstCommitId}}],
                                                 ['$1'] }]),
                    Ready = lists:map(fun(Id) -> data_for_id(Id, EntryMap) end, [FirstCommitId | MoreIds]),
                    {CommitTime, Ready}
            end;
        _Other ->
            false
    end.

-spec buffered_between(grb_time:ts(), grb_time:ts(), cache_id()) -> boolean().
-dialyzer({no_unused, buffered_between/3}).
buffered_between(From, To, Table) ->
    case ets:next(Table, {{From, 0}}) of
        '$end_of_table' -> false;
        {MarkerTime, _} -> MarkerTime < To
    end.

-spec prep_committed_between(grb_time:ts(), grb_time:ts(), cache_id()) -> boolean().
-dialyzer({no_unused, prep_committed_between/3}).
prep_committed_between(From, To, Table) ->
    0 =/= ets:select_count(Table,
                          [{ #index_entry{key={'$1', '_'}, state=prepared, vote=ok, _='_'},
                             [{'andalso', {'>', '$1', {const, From}},
                              {'<', '$1', {const, To}}}],
                             [true]  }]).

-spec data_for_id(record_id(), #{record_id() => tx_data()}) -> ?heartbeat | {#{}, vclock()}.
-dialyzer({no_unused, data_for_id/2}).
data_for_id({?heartbeat, _}, _) -> ?heartbeat;
data_for_id(Id, EntryMap) ->
    #tx_data{writeset=WS, clock=CommitVC} = maps:get(Id, EntryMap),
    {WS, CommitVC}.

%% @doc Remove all transactions in decidedRed with a commitVC[red] timestamp lower than the given one
%%
%%      This should only be done when the caller is sure that _all_ replicas have delivered all
%%      transactions below the supplied time.
%%
%%      It will clean up all pending data, and after this, it will be possible to re-prepare a
%%      transaction with the same transaction identifier.
%%
-spec prune_decided_before(grb_time:ts(), t()) -> t().
-dialyzer({nowarn_function, prune_decided_before/2}).
prune_decided_before(MinLastDelivered, State=#state{index=Idx}) ->
    Result = ets:select(Idx,
                        [{ #index_entry{key={'$1', '$2'}, state=decided, vote='$3', _='_'},
                           [{'<', '$1', {const, MinLastDelivered}}],
                           [{{'$1', '$2', '$3'}}] }],
                        ?PRUNE_LIMIT),

    prune_decided_before_continue(State, Result).

-spec prune_decided_before_continue(State :: t(),
                                    Match :: ( {[{grb_time:ts(), record_id()}], ets:continuation()}
                                               | '$end_of_table') ) -> t().

-dialyzer({no_unused, prune_decided_before_continue/2}).
prune_decided_before_continue(S, '$end_of_table') -> S;
prune_decided_before_continue(S=#state{index=Idx, entries=EntryMap0, writes_cache=Writes}, {Objects, Cont}) ->
    EntryMap = lists:foldl(fun({Time, Id, Decision}, AccMap) ->
        {#tx_data{write_keys=WrittenKeys}, NewAcc} = maps:take(Id, AccMap),
        ok = clear_pending(Id, Decision, WrittenKeys, Writes),
        true = ets:delete(Idx, {Time, Id}),
        NewAcc
    end, EntryMap0, Objects),
    prune_decided_before_continue(S#state{entries=EntryMap}, ets:select(Cont)).

-spec clear_pending(record_id(), red_vote(), [key()] | undefined, cache_id()) -> ok.
-dialyzer({no_unused, clear_pending/4}).
clear_pending(_Id, {abort, _}, _WrittenKeys, _Writes) -> ok;
clear_pending({?heartbeat, _}, ok, _WrittenKeys, _Writes) -> ok;
clear_pending(Id, ok, Keys, Writes) ->
    lists:foreach(fun(K) -> ets:delete(Writes, {K, Id}) end, Keys).

%%%===================================================================
%%% heartbeat api
%%%===================================================================

-spec prepare_hb({?heartbeat, _}, t()) -> {prepare_hb_result(), t()}.
-dialyzer({nowarn_function, prepare_hb/2}).
prepare_hb(Id, State) ->
    prepare(Id, ignore, ignore, ignore, ignore, State).

-spec accept_hb(ballot(), record_id(), grb_time:ts(), t()) -> {ok, t()} | bad_ballot.
-dialyzer({nowarn_function, accept_hb/4}).
accept_hb(Ballot, Id, Ts, State) ->
    accept(Ballot, Id, ignore, ignore, ok, Ts, State).

-spec decision_hb(ballot(), record_id(), grb_time:ts(), t()) -> {ok, t()} | decision_error().
-dialyzer({nowarn_function, decision_hb/4}).
decision_hb(Ballot, Id, Ts, State) ->
    decision(Ballot, Id, ok, Ts, State).

%%%===================================================================
%%% transaction api
%%%===================================================================

-spec prepare(TxId :: record_id(),
              RS :: readset(),
              WS :: writeset(),
              SnapshotVC :: vclock(),
              LastVC :: grb_oplog_vnode:last_vc(),
              State :: t()) -> {prepare_result(), t()}.

prepare(Id, RS, WS, SnapshotVC, LastVC, State=#state{status=?leader,
                                                     ballot=Ballot,
                                                     entries=EntryMap,
                                                     pending_reads=PendingReads,
                                                     writes_cache=PendingWrites}) ->

    case maps:get(Id, EntryMap, empty) of
        #tx_data{state=decided, vote=Decision, clock=CommitVC} ->
            {{already_decided, Decision, CommitVC}, State};

        #tx_data{state=prepared, vote=Decision, clock=PrepareVC} ->
            {{Decision, Ballot, PrepareVC}, State};

        empty ->
            Decision = check_transaction(Id, RS, WS, SnapshotVC, LastVC, PendingReads, PendingWrites),
            {RedTs, PrepareVC} = assign_timestamp(SnapshotVC),
            Record = make_prepared(Id, RS, WS, RedTs, PrepareVC, Decision, State),
            {{Decision, Ballot, PrepareVC}, State#state{entries=EntryMap#{Id => Record}}}
    end.

%% todo(borja, red): make idempotent, ignore if the transaction has been prepared/decided
%% although the leader should make it impossible for us to receive a prepare for a received
%% transaction (replying with already_prepared), it's better to avoid it.
%% if it's already prepared, don't do anything.
-spec accept(ballot(), record_id(), readset(), writeset(), red_vote(), vclock(), t()) -> {ok, t()} | bad_ballot.
accept(InBallot, _, _, _, _, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept(Ballot, Id, RS, WS, Vote, PrepareVC, S=#state{ballot=Ballot, entries=EntryMap}) ->
    RedTs = get_timestamp(Id, PrepareVC),
    Record = make_prepared(Id, RS, WS, RedTs, PrepareVC, Vote, S),
    {ok, S#state{entries=EntryMap#{Id => Record}}}.

-dialyzer({no_opaque, assign_timestamp/1}).
assign_timestamp(ignore) ->
    Time = grb_time:timestamp(),
    {Time, Time};

assign_timestamp(VC) ->
    Red = grb_time:timestamp(),
    {Red, grb_vclock:set_time(?RED_REPLICA, Red, VC)}.

get_timestamp({?heartbeat, _}, RedTs) -> RedTs;
get_timestamp(_, PrepareVC) -> grb_vclock:get_time(?RED_REPLICA, PrepareVC).


-spec make_prepared(record_id(), readset(), writeset(), grb_time:ts(), vclock(), red_vote(), t()) -> tx_data().
make_prepared(Id, RS, WS, RedTs, VC, Decision, #state{index=Idx,
                                                      pending_reads=Reads,
                                                      writes_cache=Writes}) ->

    true = ets:insert(Idx, #index_entry{key={RedTs, Id}, state=prepared, vote=Decision}),
    case Decision of
        ok ->
            make_commit_record(Id, RS, WS, RedTs, VC, Decision, Reads, Writes);
        _ ->
            %% don't store read keys, waste of space
            #tx_data{writeset=WS, red_ts=RedTs, clock=VC, state=prepared, vote=Decision}
    end.

-spec make_commit_record(record_id(), readset(), writeset(), grb_time:ts(), _, red_vote(), cache_id(), cache_id()) -> tx_data().
make_commit_record({?heartbeat, _}, _, _, RedTs, _, Decision, _, _) ->
    #tx_data{read_keys=undefined, write_keys=undefined, writeset=undefined,
             red_ts=RedTs, clock=RedTs, state=prepared, vote=Decision};

make_commit_record(TxId, RS, WS, RedTs, VC, Decision, Reads, Writes) ->
    %% only add the keys if the transaction is committed, otherwise, don't bother
    %% We can't do the same for reads, because we need to distinguish between prepared
    %% and decided writes. We can't do update_element or select_repace over bag tables,
    %% so use a normal ordered_set with a double key of {Key, Identifier}, which should
    %% be unique
    true = ets:insert(Reads, [{K, TxId} || K <- RS]),
    WriteKeys = maps:keys(WS),
    true = ets:insert(Writes, [ {{K, TxId}, prepared, VC} || K <- WriteKeys]),
    #tx_data{read_keys=RS, write_keys=WriteKeys, writeset=WS,
             red_ts=RedTs, clock=VC, state=prepared, vote=Decision}.

-spec decision_pre(ballot(), record_id(), grb_time:ts(), grb_time:ts(), t()) -> {ok, tx_data()}
                                                                              | already_decided
                                                                              | decision_error().

decision_pre(InBallot, _, _, _, #state{ballot=Ballot}) when InBallot > Ballot ->
    bad_ballot;

decision_pre(_, Id, _, _, #state{status=?follower, entries=EntryMap}) ->
    case maps:get(Id, EntryMap, not_prepared) of
        not_prepared ->
            not_prepared;

        #tx_data{state=decided} ->
            ?LOG_WARNING("duplicate DECIDE(~p)", [Id]),
            already_decided;

        Data=#tx_data{state=prepared} ->
            {ok, Data}
    end;

decision_pre(_, Id, CommitRed, ClockTime, #state{status=?leader, entries=EntryMap}) ->
    case maps:get(Id, EntryMap, not_prepared) of
        not_prepared ->
            not_prepared;

        #tx_data{state=decided} ->
            ?LOG_WARNING("duplicate DECIDE(~p)", [Id]),
            already_decided;

        Data=#tx_data{state=prepared} ->
            case ClockTime >= CommitRed of
                true -> {ok, Data};
                false -> not_ready
            end
    end.

-spec decision(ballot(), record_id(), red_vote(), vclock(), t()) -> {ok, t()} | decision_error().
decision(Ballot, Id, Vote, CommitVC, S=#state{entries=EntryMap, index=Idx,
                                              pending_reads=PrepReads, writes_cache=WriteCache,
                                              pending_commit_ts=PendingData}) ->

    RedTs = get_timestamp(Id, CommitVC),
    case decision_pre(Ballot, Id, RedTs, grb_time:timestamp(), S) of
        already_decided ->
            {ok, S};

        {ok, Data=#tx_data{red_ts=OldTs, vote=OldVote, read_keys=ReadKeys, write_keys=WriteKeys}} ->
            true = ets:delete(Idx, {OldTs, Id}),
            %% if we voted commit during prepare, we need to clean up / move pending data
            %% if we voted abort, we didn't store anything
            case OldVote of
                ok ->
                    true = ets:delete(PendingData, {RedTs, Id}),
                    move_pending_data(Id, CommitVC, Vote, ReadKeys, WriteKeys, PrepReads, WriteCache);
                _ ->
                    ok
            end,
            true = ets:insert(Idx, #index_entry{key={RedTs, Id}, state=decided, vote=Vote}),
            NewData = Data#tx_data{red_ts=RedTs, clock=CommitVC, state=decided, vote=Vote},
            {ok, S#state{entries=EntryMap#{Id => NewData}}};

        Other -> Other
    end.

-spec reserve_decision(record_id(), red_vote(), vclock(), t()) -> ok.
reserve_decision(_, {abort, _}, _, _) -> ok;
reserve_decision(Id, ok, CommitVC, #state{pending_commit_ts=PendingDataIdx})  ->
    Marker = get_timestamp(Id, CommitVC),
    true = ets:insert(PendingDataIdx, {{Marker, Id}}),
    ok.

-spec move_pending_data(Id :: record_id(),
                        CommitVC :: vclock(),
                        Vote :: red_vote(),
                        RKeys :: readset() | undefined,
                        WKeys :: [key()] | undefined,
                        PendingReads :: cache_id(),
                        WriteCache :: cache_id()) -> ok.

%% for a heartbeat, we never stored anything, so there's no need to move any data
move_pending_data({?heartbeat, _}, _, _, _, _, _, _) -> ok;
move_pending_data(Id, CommitVC, Vote, RKeys, WKeys, PendingReads, WriteCache) ->
    %% even if the new vote is abort, we clean up the reads,
    %% since we're moving out of preparedRed
    lists:foreach(fun(ReadKey) ->
        ets:delete_object(PendingReads, {ReadKey, Id})
    end, RKeys),

    case Vote of
        {abort, _} ->
            %% if the vote is abort, don't bother to move, simply delete
            lists:foreach(fun(K) -> ets:delete(WriteCache, {K, Id}) end, WKeys);
        ok ->
            %% if the vote is commit, we should mark all the entries as decided,
            %% and add the commit vector
            lists:foreach(fun(K) ->
                true = ets:update_element(WriteCache, {K, Id}, [{2, decided}, {3, CommitVC}])
            end, WKeys)
    end.

-spec check_transaction(TxId :: record_id(),
                        RS :: readset(),
                        WS :: writeset(),
                        SnapshotVC :: vclock(),
                        LastVC :: grb_oplog_vnode:last_vc(),
                        PrepReads :: cache_id(),
                        Writes :: cache_id()) -> red_vote().

check_transaction({?heartbeat, _}, _, _, ignore, _, _, _) -> ok;
check_transaction(_, RS, WS, SnapshotVC, LastVC, PrepReads, Writes) ->
    case check_prepared(RS, WS, PrepReads, Writes) of
        {abort, _}=Abort ->
            Abort;
        ok ->
            check_committed(RS, SnapshotVC, Writes, LastVC)
    end.

-spec check_prepared(RS :: readset(),
                     WS :: writeset(),
                     PendingReads :: cache_id(),
                     PendingWrites :: cache_id()) -> red_vote().

check_prepared(RS, WS, PendingReads, PendingWrites) ->
    RWConflict = lists:any(fun(Key) ->
        0 =/= ets:select_count(PendingWrites, [{ {{Key, '_'}, prepared, '_'}, [], [true] }])
    end, RS),
    case RWConflict of
        true -> ?abort_conflict;
        false ->
            WRConflict = lists:any(fun(Key) ->
                true =:= ets:member(PendingReads, Key)
            end, maps:keys(WS)),
            case WRConflict of
                true -> ?abort_conflict;
                false -> ok
            end
    end.

-spec check_committed(readset(), vclock(), cache_id(), grb_oplog_vnode:last_vc()) -> red_vote().
check_committed(RS, SnapshotVC, PendingWrites, LastVC) ->
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    try
        lists:foreach(fun(Key) ->
            case stale_decided(Key, SnapshotVC, AllReplicas, PendingWrites) of
                true ->
                    throw(?abort_stale_dec);
                false ->
                    case stale_committed(Key, SnapshotVC, AllReplicas, LastVC) of
                        true ->
                            throw(?abort_stale_comm);
                        false ->
                            ok
                    end
            end
        end, RS)
    catch AbortReason ->
        AbortReason
    end.

-spec stale_decided(key(), vclock(), [replica_id()], cache_id()) -> boolean().
stale_decided(Key, PrepareVC, AtReplicas, PendingWrites) ->
    Match = ets:select(PendingWrites, [{ {{Key, '_'}, decided, '$1'}, [], ['$1'] }]),
    case Match of
        [] ->
            false;
        Vectors ->
            lists:any(fun(CommitVC) ->
                not (grb_vclock:leq_at_keys(AtReplicas, CommitVC, PrepareVC))
            end, Vectors)
    end.

-spec stale_committed(key(), vclock(), [replica_id()], grb_oplog_vnode:last_vc()) -> boolean().
stale_committed(Key, PrepareVC, AtReplicas, LastVC) ->
    case ets:lookup(LastVC, Key) of
        [{Key, LastCommitVC}] ->
            not (grb_vclock:leq_at_keys(AtReplicas, LastCommitVC, PrepareVC));
        [] ->
            false
    end.

-ifdef(TEST).

-spec with_states(grb_paxos_state:t() | [grb_paxos_state:t()],
                  fun((grb_paxos_state:t() | [grb_paxos_state:t()]) -> _)) -> ok.

with_states(States, Fun) when is_list(States) ->
    _ = Fun(States),
    lists:foreach(fun delete/1, States);

with_states(State, Fun) ->
    _ = Fun(State),
    delete(State).

grb_paxos_state_heartbeat_test() ->
    with_states([grb_paxos_state:leader(),
                 grb_paxos_state:follower()], fun([L0, F0]) ->

        Id = {?heartbeat, 0},
        {{ok, Ballot, Ts}, L1} = grb_paxos_state:prepare_hb(Id, L0),
        {ok, F1} = grb_paxos_state:accept_hb(Ballot, Id, Ts, F0),

        {ok, L2} = grb_paxos_state:decision_hb(Ballot, Id, Ts, L1),
        {ok, F2} = grb_paxos_state:decision_hb(Ballot, Id, Ts, F1),

        {Ts, [?heartbeat]} = grb_paxos_state:get_next_ready(0, L2),
        ?assertMatch({Ts, [?heartbeat]}, grb_paxos_state:get_next_ready(0, F2)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, L2)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, F2))
    end).

%% todo(borja, test): figure out if we can setup / teardown this
grb_paxos_state_transaction_test() ->
    ok = persistent_term:put({grb_dc_manager, my_replica}, ignore),
    LastVC = ets:new(dummy, [set]),
    with_states([grb_paxos_state:leader(),
                 grb_paxos_state:follower()], fun([L0, F0]) ->

        TxId = tx_1, RS = [], WS = #{}, PVC = #{},

        {Res={ok, Ballot, CVC}, L1} = grb_paxos_state:prepare(TxId, RS, WS, PVC, LastVC, L0),

        %% preparing again should result in the same result
        {OtherRes, L2} = grb_paxos_state:prepare(TxId, RS, WS, PVC, LastVC, L1),
        ?assertEqual(Res, OtherRes),
        ?assertEqual(L1, L2),

        {ok, F1} = grb_paxos_state:accept(Ballot, TxId, RS, WS, ok, CVC, F0),

        {ok, L3} = grb_paxos_state:decision(Ballot, TxId, ok, CVC, L2),
        {ok, F2} = grb_paxos_state:decision(Ballot, TxId, ok, CVC, F1),

        %% preparing after decision should return early
        {PrepRes, L4} = grb_paxos_state:prepare(TxId, RS, WS, PVC, LastVC, L3),
        ?assertEqual({already_decided, ok, CVC} , PrepRes),
        ?assertEqual(L3 , L4),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC),
        Ready={CommitTime, [{WS, Clock}]} = grb_paxos_state:get_next_ready(0, L4),
        ?assertEqual(Ready, grb_paxos_state:get_next_ready(0, F2)),
        ?assertEqual(Expected, CommitTime),
        ?assertEqual(CVC, Clock),

        ?assertEqual(false, grb_paxos_state:get_next_ready(CommitTime, L4)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(CommitTime, F2))
    end),
    true = ets:delete(LastVC),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_get_next_ready_test() ->
    VC = fun(I) -> #{?RED_REPLICA => I} end,
    T = fun(C) -> grb_vclock:get_time(?RED_REPLICA, C) end,

    %% simple example of a tx being blocked by a previous one
    with_states(grb_paxos_state:follower(), fun(F0) ->
        [V0, V1, V2] = [ VC(1), VC(5), VC(7)],
        {ok, F1} = grb_paxos_state:accept(0, tx_0, [], #{}, ok, V0, F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_1, [], #{}, ok, V1, F1),
        {ok, F3} = grb_paxos_state:accept(0, tx_2, [], #{}, ok, V2, F2),

        {ok, F4} = grb_paxos_state:decision(0, tx_1, ok, V1, F3),
        {ok, F5} = grb_paxos_state:decision(0, tx_2, ok, V2, F4),

        %% false, still have to decide tx_0, and it was prepared with a lower time
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F5)),
        %% now decide tx_0, marking tx_1 ready for delivery
        {ok, F6} = grb_paxos_state:decision(0, tx_0, ok, V0, F5),

        ?assertEqual({T(V0), [{#{}, V0}]}, grb_paxos_state:get_next_ready(0, F6)),
        ?assertEqual({T(V1), [{#{}, V1}]}, grb_paxos_state:get_next_ready(T(V0), F6)),
        ?assertEqual({T(V2), [{#{}, V2}]}, grb_paxos_state:get_next_ready(T(V1), F6))
    end),

    %% simple example of a tx being blocked by a heartbeat
    with_states(grb_paxos_state:follower(), fun(F0) ->
        [T0, V1] = [ 1, VC(5)],

        {ok, F1} = grb_paxos_state:accept_hb(0, {?heartbeat, 0}, T0, F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_1, [], #{}, ok, V1, F1),

        {ok, F3} = grb_paxos_state:decision(0, tx_1, ok, V1, F2),

        %% false, still have to decide the heartbeat
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F3)),

        %% now decide the heartbeat, marking tx_1 ready for delivery
        {ok, F4} = grb_paxos_state:decision_hb(0, {?heartbeat, 0}, T0, F3),

        ?assertEqual({T0, [?heartbeat]}, grb_paxos_state:get_next_ready(0, F4)),
        ?assertEqual({T(V1), [{#{}, V1}]}, grb_paxos_state:get_next_ready(T0, F4))
    end),

    %% more complex with transactions moving places
    with_states(grb_paxos_state:follower(), fun(F0) ->
        [V0, V1, V2, V3, V4] = [ VC(1), VC(5), VC(7), VC(10), VC(15)],

        {ok, F1} = grb_paxos_state:accept(0, tx_0, [], #{}, {abort, ignore}, V0, F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_1, [], #{}, ok, V1, F1),
        {ok, F3} = grb_paxos_state:accept(0, tx_2, [], #{}, ok, V2, F2),
        {ok, F4} = grb_paxos_state:accept(0, tx_3, [], #{}, ok, V3, F3),

        {ok, F5} = grb_paxos_state:decision(0, tx_1, ok, V1, F4),

        %% we can skip tx_0, since it was aborted, no need to wait for decision (because decision will be abort)
        ?assertEqual({T(V1), [{#{}, V1}]}, grb_paxos_state:get_next_ready(0, F5)),

        %% now we decide tx_2 with a higher time, placing it the last on the queue
        {ok, F6} = grb_paxos_state:decision(0, tx_2, ok, V4, F5),

        %% and decide tx_3, making it ready for delivery
        {ok, F7} = grb_paxos_state:decision(0, tx_3, ok, V3, F6),

        ?assertEqual({T(V3), [{#{}, V3}]}, grb_paxos_state:get_next_ready(T(V1), F7)),
        ?assertEqual({T(V4), [{#{}, V4}]}, grb_paxos_state:get_next_ready(T(V3), F7))
    end).

grb_paxos_state_hb_clash_test() ->
    with_states(grb_paxos_state:leader(), fun(L0) ->
        {{ok, B, Ts0}, L1} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L0),
        {ok, L2} = grb_paxos_state:decision_hb(B, {?heartbeat, 0}, Ts0, L1),

        ?assertEqual({Ts0, [?heartbeat]}, grb_paxos_state:get_next_ready(0, L2)),
        {PrepRes, L3} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L2),
        ?assertMatch({already_decided, ok, Ts0}, PrepRes),

        ?assertEqual({Ts0, [?heartbeat]}, grb_paxos_state:get_next_ready(0, L3)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts0, L3))
    end).

grb_paxos_state_tx_clash_test() ->
    VC = fun(I) -> #{?RED_REPLICA => I} end,

    with_states(grb_paxos_state:follower(), fun(F0) ->
        {ok, F1} = grb_paxos_state:accept(0, tx_1, [], #{a => 0}, ok, VC(0), F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_2, [], #{a => 1}, ok, VC(5), F1),

        {ok, F3} = grb_paxos_state:decision(0, tx_1, ok, VC(5), F2),
        {ok, F4} = grb_paxos_state:decision(0, tx_2, ok, VC(5), F3),

        ?assertEqual({5, [ {#{a => 0}, VC(5)},
                           {#{a => 1}, VC(5)} ]}, grb_paxos_state:get_next_ready(0, F4) )
    end).

grb_paxos_state_check_prepared_test() ->
    PendingReads = ets:new(dummy_reads, [bag]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),

    ?assertEqual(ok, check_prepared([], #{}, PendingReads, PendingWrites)),

    true = ets:insert(PendingReads, [{a, ignore}, {b, ignore}, {c, ignore}]),
    ?assertMatch(?abort_conflict, check_prepared([], #{a => <<>>}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared([], #{d => <<"hey">>}, PendingReads, PendingWrites)),

    %% The first aborts over a read-write conflict, while the second one doesn't, because `b` is decided
    true = ets:insert(PendingWrites, [{{a, tx_1}, prepared, 0}, {{b, tx_2}, decided, 0}]),
    ?assertMatch(?abort_conflict, check_prepared([a], #{}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared([b], #{}, PendingReads, PendingWrites)),

    %% aborts due to write-read conflict
    ?assertMatch(?abort_conflict, check_prepared([a, b], #{a => <<>>}, PendingReads, PendingWrites)),

    true = ets:delete(PendingReads),
    true = ets:delete(PendingWrites).

grb_paxos_state_check_committed_test() ->
    LastVC = ets:new(dummy_vc, [set]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),
    ok = persistent_term:put({grb_dc_manager, my_replica}, undefined),

    ?assertMatch(ok, check_committed([], #{}, PendingWrites, LastVC)),

    true = ets:insert(PendingWrites, [{{a, tx_1}, prepared, #{?RED_REPLICA => 10}},
                                      {{b, tx_2}, decided, #{?RED_REPLICA => 0}},
                                      {{c, tx_3}, decided, #{?RED_REPLICA => 10}}]),

    %% this won't clash with a, even if it has a higher Red time, because it's in prepared, so we don't care
    ?assertMatch(ok, check_committed([a], #{}, PendingWrites, LastVC)),

    %% this commits too, since we're not stale
    ?assertMatch(ok, check_committed([b], #{?RED_REPLICA => 5}, PendingWrites, LastVC)),

    %% this will abort, since tx_3 will be delivered with a higher red timestamp
    ?assertMatch(?abort_stale_dec, check_committed([c], #{?RED_REPLICA => 8}, PendingWrites, LastVC)),

    %% none of these are affected by the above, since they don't share any keys
    true = ets:insert(LastVC, {0, #{?RED_REPLICA => 10}}),
    ?assertMatch(?abort_stale_comm, check_committed([0], #{?RED_REPLICA => 9}, PendingWrites, LastVC)),
    ?assertMatch(ok, check_committed([0], #{?RED_REPLICA => 11}, PendingWrites, LastVC)),

    true = ets:delete(LastVC),
    true = ets:delete(PendingWrites),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_prune_decided_before_test() ->
    ok = persistent_term:put({grb_dc_manager, my_replica}, ignore),
    LastVC = ets:new(dummy, [set]),

    with_states(grb_paxos_state:leader(), fun(L0) ->

        TxId1 = tx_1, RS1 = [a], WS1 = #{a => <<"hello">>}, PVC1 = #{?RED_REPLICA => 5},

        {{ok, Ballot, CVC1}, L1} = grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastVC, L0),
        {ok, L2} = grb_paxos_state:decision(Ballot, TxId1, ok, CVC1, L1),

        %% preparing after decision should return early
        {PrepRes0, L3} = grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastVC, L2),
        ?assertEqual({already_decided, ok, CVC1}, PrepRes0),
        ?assertEqual(L2, L3),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC1),
        ?assertEqual({Expected, [{WS1, CVC1}]}, grb_paxos_state:get_next_ready(0, L3)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Expected, L3)),

        %% preparing even after delivery should return early
        {PrepRes1, L4} = grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastVC, L3),
        ?assertEqual({already_decided, ok, CVC1}, PrepRes1),
        ?assertEqual(L3, L4),

        %% let's do a heartbeat now
        {{ok, Ballot, Ts}, L5} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L4),
        {ok, L6} = grb_paxos_state:decision_hb(Ballot, {?heartbeat, 0}, Ts, L5),

        ?assertEqual({Ts, [?heartbeat]}, grb_paxos_state:get_next_ready(Expected, L5)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, L5)),

        %% prune, now we should only have the heartbeat
        L7 = grb_paxos_state:prune_decided_before(Expected + 1, L6),
        ?assertMatch({Ts, [?heartbeat]}, grb_paxos_state:get_next_ready(0, L7)),

        %% but if we prune past that, then the heartbeat should be gone
        L8 = grb_paxos_state:prune_decided_before(Ts + 1, L7),
        ?assertMatch(false, grb_paxos_state:get_next_ready(0, L6)),

        %% now we can prepare this transaction again
        {PrepRes2, _} = grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastVC, L8),
        ?assertMatch({ok, Ballot, #{}}, PrepRes2)
    end),

    true = ets:delete(LastVC),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_decision_reserve_test() ->
    with_states(grb_paxos_state:follower(), fun(F0) ->
        TxId1 = tx_1, RS1 = [a], WS1 = #{a => <<"hello">>}, CVC1 = #{?RED_REPLICA => 1},
        HB = {?heartbeat, 0}, Ts = 2,
        TxId2 = tx_2, RS2 = [b], WS2 = #{b => <<"another update">>}, CVC2 = #{?RED_REPLICA => 5},

        %% If we get a commit decision for a transaction that hasn't been accepted,
        %% it should "reserve its spot" in the commit order, or another transaction could be
        %% decided and delivered while we wait for the corresponding ACCEPT
        ?assertMatch(not_prepared, grb_paxos_state:decision(0, TxId1, ok, CVC1, F0)),
        ok = grb_paxos_state:reserve_decision(TxId1, ok, CVC1, F0),

        %% Same with a heartbeat
        ?assertMatch(not_prepared, grb_paxos_state:decision_hb(0, HB, Ts, F0)),
        ok = grb_paxos_state:reserve_decision(HB, ok, Ts, F0),

        {ok, F1} = grb_paxos_state:accept(0, TxId2, RS2, WS2, ok, CVC2, F0),
        {ok, F2} = grb_paxos_state:decision(0, TxId2, ok, CVC2, F1),

        %% Although we tx_2 is ready to deliver, we have a pending ACCEPT for tx_1
        ?assertMatch(false, grb_paxos_state:get_next_ready(0, F2)),

        {ok, F3} = grb_paxos_state:accept(0, TxId1, RS1, WS1, ok, CVC1, F2),
        {ok, F4} = grb_paxos_state:decision(0, TxId1, ok, CVC1, F3),

        %% Now that tx_1 is complete, it can be delivered
        ?assertMatch({1, [{WS1, CVC1}]}, grb_paxos_state:get_next_ready(0, F4)),
        %% but trying to deliver tx_2 will fail as we still have the heartbeat in the queue
        ?assertMatch(false, grb_paxos_state:get_next_ready(1, F4)),

        {ok, F5} = grb_paxos_state:accept_hb(0, HB, Ts, F4),
        {ok, F6} = grb_paxos_state:decision_hb(0, HB, Ts, F5),

        %% Finally, we can deliver the heartbeat and tx_2
        ?assertMatch({2, [?heartbeat]}, grb_paxos_state:get_next_ready(1, F6)),
        ?assertMatch({5, [{WS2, CVC2}]}, grb_paxos_state:get_next_ready(2, F6))
    end).

-endif.
