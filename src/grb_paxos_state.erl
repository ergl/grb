-module(grb_paxos_state).
-include("grb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(leader, leader).
-define(follower, follower).
-define(heartbeat, heartbeat).
-type status() :: ?leader | ?follower.

%% How many commit times to delete at once
%% If this is too low, we could spend quite a bit of time deleting
%% If the value is too high, we could hurt performance from copying too much
%% todo(borja, red): Make config for this
-define(PRUNE_LIMIT, 50).

%% We don't care about the structure too much, as long as the client ensures identifiers
%% are unique (or at least, unique to the point of not reusing them before they call
%% `prune_decided_before/2`
-type record_id() :: {?heartbeat, term()} | term().
-record(record, {
    id :: record_id(),
    read_keys = [] :: [key()],
    write_keys = [] :: [key()],
    writeset = #{} :: #{},
    red :: grb_time:ts(),
    clock = undefined :: vclock() | undefined,
    state :: prepared | decided,
    vote :: red_vote()
}).

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
    entries :: cache_id(),
    index :: cache_id(),

    %% A pair of inverted indices over the transactions read/write sets
    %% Used to compute write-write and read-write conflicts in a quick way
    pending_reads :: cache_id(),
    writes_cache :: cache_id()
}).

-type decision_error() :: bad_ballot | not_prepared | not_ready.
-type t() :: #state{}.

-export_type([t/0, decision_error/0]).

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
           entries=ets:new(state_entries, [set, {keypos, #record.id}]),
           index=ets:new(state_entries_index, [ordered_set, {keypos, #index_entry.key}]),
           pending_reads=ets:new(state_pending_reads, [bag]),
           writes_cache=ets:new(state_pending_writes, [ordered_set])}.

-spec delete(t()) -> ok.
delete(#state{entries=Entries, index=Index, pending_reads=PendingReads, writes_cache=Writes}) ->
    true = ets:delete(Entries),
    true = ets:delete(Index),
    true = ets:delete(PendingReads),
    true = ets:delete(Writes),
    ok.

%%%===================================================================
%%% ready / prune
%%%===================================================================

-spec get_next_ready(grb_time:ts(), t()) -> false | {grb_time:ts(), [ ?heartbeat | {#{}, vclock()} ]}.
-dialyzer({nowarn_function, get_next_ready/2}).
get_next_ready(LastDelivered, #state{entries=Entries, index=Idx}) ->
    Result = ets:select(Idx,
                        [{ #index_entry{key={'$1', '$2'}, state=decided, vote=ok},
                           [{'>', '$1', {const, LastDelivered}}],
                           [{{'$1', '$2'}}] }],
                        1),
    case Result of
        '$end_of_table' -> false;
        {[{CommitTime, FirstId}] , _} ->
            case prep_committed_between(LastDelivered, CommitTime, Idx) of
                true -> false;
                false ->
                    %% get the rest of committed transactions with this commit time
                    %% since they have the same committed time, they all pass the check above
                    MoreIds = ets:select(Idx, [{ #index_entry{key={CommitTime, '$1'}, state=decided, vote=ok},
                                                 [{'=/=', '$1', {const, FirstId}}],
                                                 ['$1'] }]),
                    Ready = lists:map(fun(Id) -> data_for_id(Id, Entries) end, [FirstId | MoreIds]),
                    {CommitTime, Ready}
            end
    end.

-spec prep_committed_between(grb_time:ts(), grb_time:ts(), cache_id()) -> boolean().
-dialyzer({no_unused, prep_committed_between/3}).
prep_committed_between(From, To, Table) ->
    0 =/= ets:select_count(Table,
                          [{ #index_entry{key={'$1', '_'}, state=prepared, vote=ok, _='_'},
                             [{'andalso', {'>', '$1', {const, From}},
                              {'<', '$1', {const, To}}}],
                             [true]  }]).

-spec data_for_id(record_id(), cache_id()) -> ?heartbeat | {#{}, vclock()}.
data_for_id({?heartbeat, _}, _) -> ?heartbeat;
data_for_id(Id, Entries) ->
    hd(ets:select(Entries, [{ #record{id=Id, writeset='$1', clock='$2', _='_'}, [], [{{'$1', '$2'}}] }])).

%% @doc Remove all transactions in decidedRed with a commitVC[red] timestamp lower than the given one
%%
%%      This should only be done when the caller is sure that _all_ replicas have delivered all
%%      transactions below the supplied time.
%%
%%      It will clean up all pending data, and after this, it will be possible to re-prepare a
%%      transaction with the same transaction identifier.
%%
-spec prune_decided_before(grb_time:ts(), t()) -> ok.
-dialyzer({nowarn_function, prune_decided_before/2}).
prune_decided_before(MinLastDelivered, #state{entries=Entries, index=Idx, writes_cache=WriteKeys}) ->
    Result = ets:select(Idx,
                        [{ #index_entry{key={'$1', '$2'}, state=decided, vote='$3', _='_'},
                           [{'<', '$1', {const, MinLastDelivered}}],
                           [{{'$1', '$2', '$3'}}] }],
                        ?PRUNE_LIMIT),

    prune_decided_before_continue(Entries, Idx, WriteKeys, Result).

-spec prune_decided_before_continue(Entries :: cache_id(),
                                    Idx :: cache_id(),
                                    Writes :: cache_id(),
                                    Match :: ( {[{grb_time:ts(), record_id()}], ets:continuation()}
                                               | '$end_of_table') ) -> ok.

prune_decided_before_continue(_Entries, _Idx, _Writes, '$end_of_table') -> ok;
prune_decided_before_continue(Entries, Idx, Writes, {Objects, Cont}) ->
    lists:foreach(fun({Time, Id, Decision}) ->
        ok = clear_pending(Id, Decision, Entries, Writes),
        true = ets:delete(Idx, {Time, Id}),
        true = ets:delete(Entries, Id)
    end, Objects),
    prune_decided_before_continue(Entries, Idx, Writes, ets:select(Cont)).

-spec clear_pending(record_id(), red_vote(), cache_id(), cache_id()) -> ok.
clear_pending(_Id, {abort, _}, _Entries, _Writes) -> ok;
clear_pending({?heartbeat, _}, ok, _Entries, _Writes) -> ok;
clear_pending(Id, ok, Entries, Writes) ->
    Keys = ets:lookup_element(Entries, Id, #record.write_keys),
    lists:foreach(fun(K) -> ets:delete(Writes, {K, Id}) end, Keys).

%%%===================================================================
%%% heartbeat api
%%%===================================================================

-spec prepare_hb({?heartbeat, _}, t()) -> {red_vote(), ballot(), grb_time:ts()}
                                        | {already_decided, red_vote(), grb_time:ts()}.
prepare_hb(Id, State) ->
    prepare(Id, ignore, ignore, ignore, ignore, State).

-spec accept_hb(ballot(), record_id(), grb_time:ts(), t()) -> ok | bad_ballot.
accept_hb(Ballot, Id, Ts, State) ->
    accept(Ballot, Id, ignore, ignore, ok, Ts, State).


-spec decision_hb(ballot(), record_id(), grb_time:ts(), t()) -> ok | decision_error().
decision_hb(Ballot, Id, Ts, State) ->
    decision(Ballot, Id, ok, Ts, State).

%%%===================================================================
%%% transaction api
%%%===================================================================

-spec prepare(TxId :: record_id(),
              RS :: #{key() => grb_time:ts()},
              WS :: #{key() => val()},
              SnapshotVC :: vclock(),
              LastRed :: cache(key(), grb_time:ts()),
              State :: t()) -> {red_vote(), ballot(), vclock()}
                             | {already_decided, red_vote(), vclock()}.

-dialyzer({no_match, prepare/6}).
prepare(Id, RS, WS, SnapshotVC, LastRed, S=#state{status=?leader,
                                                    ballot=Ballot,
                                                    entries=Entries,
                                                    pending_reads=PendingReads,
                                                    writes_cache=PendingWrites}) ->

    Status =
        try ets:lookup_element(Entries, Id, #record.state)
        catch _:_ -> empty end,

    case Status of
        decided ->
            [{Decision, CommitVC}] =
                ets:select(Entries,
                           [{ #record{id=Id, vote='$1', clock='$2', _='_'}, [], [{{'$1', '$2'}}] }]),

            {already_decided, Decision, CommitVC};

        prepared ->
            [{Decision, PrepareVC}] =
                ets:select(Entries,
                           [{ #record{id=Id, vote='$1', clock='$2', _='_'}, [], [{{'$1', '$2'}}] }]),

            {Decision, Ballot, PrepareVC};

        empty ->
            {Decision, VC0} = check_transaction(Id, RS, WS, SnapshotVC, LastRed, PendingReads, PendingWrites),
            {RedTs, PrepareVC} = assign_timestamp(VC0),
            ok = insert_prepared(Id, RS, WS, RedTs, PrepareVC, Decision, S),

            {Decision, Ballot, PrepareVC}
    end.

%% todo(borja, red): make idempotent, ignore if the transaction has been prepared/decided
%% although the leader should make it impossible for us to receive a prepare for a received
%% transaction (replying with already_prepared), it's better to avoid it.
%% if it's already prepared, don't do anything.
-spec accept(ballot(), record_id(), #{}, #{}, red_vote(), vclock(), t()) -> ok | bad_ballot.
accept(InBallot, _, _, _, _, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept(Ballot, Id, RS, WS, Vote, PrepareVC, S=#state{ballot=Ballot}) ->
    RedTs = get_timestamp(Id, PrepareVC),
    ok = insert_prepared(Id, RS, WS, RedTs, PrepareVC, Vote, S),
    ok.

assign_timestamp(ignore) ->
    Time = grb_time:timestamp(),
    {Time, Time};

assign_timestamp(VC) ->
    Red = grb_time:timestamp(),
    {Red, grb_vclock:set_time(?RED_REPLICA, Red, VC)}.

get_timestamp({?heartbeat, _}, RedTs) -> RedTs;
get_timestamp(_, PrepareVC) -> grb_vclock:get_time(?RED_REPLICA, PrepareVC).


-spec insert_prepared(record_id(), #{}, #{}, grb_time:ts(), vclock(), red_vote(), t()) -> ok.
insert_prepared(Id, RS, WS, RedTs, VC, Decision, #state{index=Idx,
                                                        entries=Entries,
                                                        pending_reads=Reads,
                                                        writes_cache=Writes}) ->

    Record = case Decision of
        ok ->
            make_commit_record(Id, RS, WS, RedTs, VC, Decision, Reads, Writes);
        _ ->
            %% don't store read keys, waste of space
            #record{id=Id, writeset=WS,
                    red=RedTs, clock=VC, state=prepared, vote=Decision}
    end,
    true = ets:insert(Entries, Record),
    true = ets:insert(Idx, #index_entry{key={RedTs, Id}, state=prepared, vote=Decision}),
    ok.

make_commit_record({?heartbeat, _}=Id, _, _, RedTs, _, Decision, _, _) ->
    #record{id=Id, read_keys=undefined, write_keys=undefined, writeset=undefined,
            red=RedTs, clock=RedTs, state=prepared, vote=Decision};

make_commit_record(TxId, RS, WS, RedTs, VC, Decision, Reads, Writes) ->
    %% only add the keys if the transaction is committed, otherwise, don't bother
    ReadKeys = maps:keys(RS),
    true = ets:insert(Reads, [{K, TxId} || K <- ReadKeys]),
    %% We can't do the same for reads, because we need to distinguish between prepared
    %% and decided writes. We can't do update_element or select_repace over bag tables,
    %% so use a normal ordered_set with a double key of {Key, Identifier}, which should
    %% be unique
    WriteKeys = maps:keys(WS),
    true = ets:insert(Writes, [ {{K, TxId}, prepared, RedTs} || K <- WriteKeys]),
    #record{id=TxId, read_keys=ReadKeys, write_keys=WriteKeys, writeset=WS,
            red=RedTs, clock=VC, state=prepared, vote=Decision}.

-spec decision_pre(ballot(), record_id(), grb_time:ts(), grb_time:ts(), t()) -> ok | already_decided | decision_error().
decision_pre(InBallot, _, _, _, #state{ballot=Ballot}) when InBallot > Ballot ->
    bad_ballot;

decision_pre(_, Id, _, _, #state{status=?follower, entries=Entries}) ->
    try
        Status = ets:lookup_element(Entries, Id, #record.state),
        case Status of
            prepared ->
                ok;
            decided ->
                already_decided
        end
    catch _:_ ->
        not_prepared
    end;

decision_pre(_, Id, CommitRed, ClockTime, #state{status=?leader, entries=Entries}) ->
    try
        Status = ets:lookup_element(Entries, Id, #record.state),
        case Status of
            decided ->
                already_decided;
            prepared ->
                case ClockTime >= CommitRed of
                    true -> ok;
                    false -> not_ready
                end
        end
    catch _:_ ->
        not_prepared
    end.

-spec decision(ballot(), record_id(), red_vote(), vclock(), t()) -> ok | decision_error().
-dialyzer({no_match, decision/5}).
decision(Ballot, Id, Vote, CommitVC, S=#state{entries=Entries, index=Idx,
                                              pending_reads=PrepReads, writes_cache=WriteCache}) ->

    RedTs = get_timestamp(Id, CommitVC),
    case decision_pre(Ballot, Id, RedTs, grb_time:timestamp(), S) of
        already_decided -> ok;
        ok ->
            [{OldTs, OldVote}] =
                ets:select(Entries,
                           [{ #record{id=Id, red='$1', vote='$2', _='_'}, [], [{{'$1', '$2'}}] }]),

            true = ets:delete(Idx, {OldTs, Id}),
            %% if we voted commit during prepare, we need to clean up / move pending data
            %% if we voted abort, we didn't store anything
            case OldVote of
                ok -> move_pending_data(Id, RedTs, Vote, Entries, PrepReads, WriteCache);
                _ -> ok
            end,

            true = ets:update_element(Entries, Id, [{#record.red, RedTs},
                                                    {#record.clock, CommitVC},
                                                    {#record.state, decided},
                                                    {#record.vote, Vote}]),

            true = ets:insert(Idx, #index_entry{key={RedTs, Id}, state=decided, vote=Vote}),
            ok;

        Other -> Other
    end.

-spec move_pending_data(Id :: record_id(),
                        RedTs :: grb_time:ts(),
                        Vote :: red_vote(),
                        Entries :: cache_id(),
                        PendingReads :: cache_id(),
                        WriteCache :: cache_id()) -> ok.

-dialyzer({no_unused, move_pending_data/6}).
%% for a heartbeat, we never stored anything, so there's no need to move any data
move_pending_data({?heartbeat, _}, _, _, _, _, _) -> ok;
move_pending_data(Id, RedTs, Vote, Entries, PendingReads, WriteCache) ->
    [{RKeys, WKeys}] =
        ets:select(Entries,
                   [{ #record{id=Id, read_keys='$1', write_keys='$2', _='_'}, [],
                       [{{'$1', '$2'}}] }]),

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
            %% and add the red timestamp
            lists:foreach(fun(K) ->
                true = ets:update_element(WriteCache, {K, Id}, [{2, decided}, {3, RedTs}])
            end, WKeys)
    end.

-spec check_transaction(TxId :: record_id(),
                        RS :: #{key() => grb_time:ts()},
                        WS :: #{key() => val()},
                        CommitVC :: vclock(),
                        LastRed :: grb_main_vnode:last_red(),
                        PrepReads :: cache_id(),
                        Writes :: cache_id()) -> {red_vote(), vclock()}.

check_transaction({?heartbeat, _}, _, _, ignore, _, _, _) -> {ok, ignore};
check_transaction(_, RS, WS, CommitVC, LastRed, PrepReads, Writes) ->
    case check_prepared(RS, WS, PrepReads, Writes) of
        {abort, _}=Abort ->
            {Abort, CommitVC};
        ok ->
            check_committed(RS, CommitVC, Writes, LastRed)
    end.

-spec check_prepared(RS :: #{key() => grb_time:ts()},
                     WS :: #{key() => val()},
                     PendingReads :: cache_id(),
                     PendingWrites :: cache_id()) -> red_vote().

check_prepared(RS, WS, PendingReads, PendingWrites) ->
    RWConflict = lists:any(fun(Key) ->
        0 =/= ets:select_count(PendingWrites, [{ {{Key, '_'}, prepared, '_'}, [], [true] }])
    end, maps:keys(RS)),
    case RWConflict of
        true -> {abort, conflict};
        false ->
            WRConflict = lists:any(fun(Key) ->
                true =:= ets:member(PendingReads, Key)
            end, maps:keys(WS)),
            case WRConflict of
                true -> {abort, conflict};
                false -> ok
            end
    end.

-spec check_committed(#{key() => grb_time:ts()}, vclock(), cache_id(), cache_id()) -> {red_vote(), vclock()}.
check_committed(RS, CommitVC, PendingWrites, LastRed) ->
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    try
        FinalVC = maps:fold(fun(Key, Version, AccVC) ->
            case stale_decided(Key, Version, PendingWrites) of
                true ->
                    throw({{abort, stale_decided}, CommitVC});
                false ->
                    Res = stale_committed(Key, Version, AllReplicas, AccVC, LastRed),
                    case Res of
                        false ->
                            throw({{abort, stale_committed}, CommitVC});

                        PrepareVC ->
                            PrepareVC
                    end
            end
        end, CommitVC, RS),
        {ok, FinalVC}
    catch Decision ->
        Decision
    end.

-spec stale_decided(key(), grb_time:ts(), cache_id()) -> boolean().
stale_decided(Key, Version, PendingWrites) ->
    Match = ets:select(PendingWrites, [{ {{Key, '_'}, decided, '$1'}, [], ['$1'] }]),
    case Match of
        [] -> false;
        Times -> lists:any(fun(T) -> T > Version end, Times)
    end.

-spec stale_committed(key(), grb_time:ts(), [replica_id()], vclock(), cache_id()) -> false | vclock().
stale_committed(Key, Version, AtReplicas, CommitVC, LastRed) ->
    try
        LastRedVsn = ets:lookup_element(LastRed, Key, #last_red_record.red),
        case LastRedVsn =< Version of
            false -> false;
            true ->
                compact_clocks(AtReplicas, CommitVC,
                               ets:lookup_element(LastRed, Key, #last_red_record.clocks))
        end
    catch _:_ ->
        CommitVC
    end.

-spec compact_clocks([replica_id()], vclock(), [vclock()]) -> vclock().
compact_clocks(AtReplicas, Init, Clocks) ->
    lists:foldl(fun(VC, Acc) ->
        grb_vclock:max_at_keys(AtReplicas, VC, Acc)
    end, Init, Clocks).

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
                 grb_paxos_state:follower()], fun([L, F]) ->

        Id = {?heartbeat, 0},
        {ok, Ballot, Ts} = grb_paxos_state:prepare_hb(Id, L),
        ok = grb_paxos_state:accept_hb(Ballot, Id, Ts, F),

        ok = grb_paxos_state:decision_hb(Ballot, Id, Ts, L),
        ok = grb_paxos_state:decision_hb(Ballot, Id, Ts, F),

        {Ts, [?heartbeat]} = grb_paxos_state:get_next_ready(0, L),
        ?assertMatch({Ts, [?heartbeat]}, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, F))
    end).

%% todo(borja, test): figure out if we can setup / teardown this
grb_paxos_state_transaction_test() ->
    ok = persistent_term:put({grb_dc_manager, my_replica}, ignore),
    LastRed = ets:new(dummy, [set, {keypos, #last_red_record.key}]),
    with_states([grb_paxos_state:leader(),
                 grb_paxos_state:follower()], fun([L, F]) ->

        TxId = tx_1, RS = #{}, WS = #{}, PVC = #{},

        Res={ok, Ballot, CVC} = grb_paxos_state:prepare(TxId, RS, WS, PVC, LastRed, L),
        %% preparing again should result in the same result
        ?assertEqual(Res, grb_paxos_state:prepare(TxId, RS, WS, PVC, LastRed, L)),

        ok = grb_paxos_state:accept(Ballot, TxId, RS, WS, ok, CVC, F),

        ok = grb_paxos_state:decision(Ballot, TxId, ok, CVC, L),
        ok = grb_paxos_state:decision(Ballot, TxId, ok, CVC, F),

        %% preparing after decision should return early
        ?assertEqual({already_decided, ok, CVC} , grb_paxos_state:prepare(TxId, RS, WS, PVC, LastRed, L)),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC),
        Ready={CommitTime, [{WS, Clock}]} = grb_paxos_state:get_next_ready(0, L),
        ?assertEqual(Ready, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual(Expected, CommitTime),
        ?assertEqual(CVC, Clock),

        ?assertEqual(false, grb_paxos_state:get_next_ready(CommitTime, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(CommitTime, F))
    end),
    true = ets:delete(LastRed),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_get_next_ready_test() ->
    VC = fun(I) -> #{?RED_REPLICA => I} end,
    T = fun(C) -> grb_vclock:get_time(?RED_REPLICA, C) end,

    %% simple example of a tx being blocked by a previous one
    with_states(grb_paxos_state:follower(), fun(F) ->
        [V0, V1, V2] = [ VC(1), VC(5), VC(7)],
        ok = grb_paxos_state:accept(0, tx_0, #{}, #{}, ok, V0, F),
        ok = grb_paxos_state:accept(0, tx_1, #{}, #{}, ok, V1, F),
        ok = grb_paxos_state:accept(0, tx_2, #{}, #{}, ok, V2, F),

        ok = grb_paxos_state:decision(0, tx_1, ok, V1, F),
        ok = grb_paxos_state:decision(0, tx_2, ok, V2, F),

        %% false, still have to decide tx_0, and it was prepared with a lower time
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F)),
        %% now decide tx_0, marking tx_1 ready for delivery
        ok = grb_paxos_state:decision(0, tx_0, ok, V0, F),

        ?assertEqual({T(V0), [{#{}, V0}]}, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual({T(V1), [{#{}, V1}]}, grb_paxos_state:get_next_ready(T(V0), F)),
        ?assertEqual({T(V2), [{#{}, V2}]}, grb_paxos_state:get_next_ready(T(V1), F))
    end),

    %% simple example of a tx being blocked by a heartbeat
    with_states(grb_paxos_state:follower(), fun(F) ->
        [T0, V1] = [ 1, VC(5)],

        ok = grb_paxos_state:accept_hb(0, {?heartbeat, 0}, T0, F),
        ok = grb_paxos_state:accept(0, tx_1, #{}, #{}, ok, V1, F),

        ok = grb_paxos_state:decision(0, tx_1, ok, V1, F),

        %% false, still have to decide the heartbeat
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F)),

        %% now decide the heartbeat, marking tx_1 ready for delivery
        ok = grb_paxos_state:decision_hb(0, {?heartbeat, 0}, T0, F),

        ?assertEqual({T0, [?heartbeat]}, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual({T(V1), [{#{}, V1}]}, grb_paxos_state:get_next_ready(T0, F))
    end),

    %% more complex with transactions moving places
    with_states(grb_paxos_state:follower(), fun(F) ->
        [V0, V1, V2, V3, V4] = [ VC(1), VC(5), VC(7), VC(10), VC(15)],

        ok = grb_paxos_state:accept(0, tx_0, #{}, #{}, {abort, ignore}, V0, F),
        ok = grb_paxos_state:accept(0, tx_1, #{}, #{}, ok, V1, F),
        ok = grb_paxos_state:accept(0, tx_2, #{}, #{}, ok, V2, F),
        ok = grb_paxos_state:accept(0, tx_3, #{}, #{}, ok, V3, F),

        ok = grb_paxos_state:decision(0, tx_1, ok, V1, F),

        %% we can skip tx_0, since it was aborted, no need to wait for decision (because decision will be abort)
        ?assertEqual({T(V1), [{#{}, V1}]}, grb_paxos_state:get_next_ready(0, F)),

        %% now we decide tx_2 with a higher time, placing it the last on the queue
        ok = grb_paxos_state:decision(0, tx_2, ok, V4, F),

        %% and decide tx_3, making it ready for delivery
        ok = grb_paxos_state:decision(0, tx_3, ok, V3, F),

        ?assertEqual({T(V3), [{#{}, V3}]}, grb_paxos_state:get_next_ready(T(V1), F)),
        ?assertEqual({T(V4), [{#{}, V4}]}, grb_paxos_state:get_next_ready(T(V3), F))
    end).

grb_paxos_state_hb_clash_test() ->
    with_states(grb_paxos_state:leader(), fun(L) ->
        {ok, B, Ts0} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L),
        grb_paxos_state:decision_hb(B, {?heartbeat, 0}, Ts0, L),

        ?assertEqual({Ts0, [?heartbeat]}, grb_paxos_state:get_next_ready(0, L)),
        ?assertMatch({already_decided, ok, Ts0}, grb_paxos_state:prepare_hb({?heartbeat, 0}, L)),

        ?assertEqual({Ts0, [?heartbeat]}, grb_paxos_state:get_next_ready(0, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts0, L))
    end).

%%%% fixme(borja, red): This shouldn't fail
grb_paxos_state_tx_clash_test() ->
    VC = fun(I) -> #{?RED_REPLICA => I} end,

    with_states(grb_paxos_state:follower(), fun(F) ->
        ok = grb_paxos_state:accept(0, tx_1, #{}, #{a => 0}, ok, VC(0), F),
        ok = grb_paxos_state:accept(0, tx_2, #{}, #{a => 1}, ok, VC(5), F),

        ok = grb_paxos_state:decision(0, tx_1, ok, VC(5), F),
        ok = grb_paxos_state:decision(0, tx_2, ok, VC(5), F),

        ?assertEqual({5, [ {#{a => 0}, VC(5)},
                           {#{a => 1}, VC(5)} ]}, grb_paxos_state:get_next_ready(0, F) )
    end).

grb_paxos_state_check_prepared_test() ->
    PendingReads = ets:new(dummy_reads, [bag]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),

    ?assertEqual(ok, check_prepared(#{}, #{}, PendingReads, PendingWrites)),

    true = ets:insert(PendingReads, [{a, ignore}, {b, ignore}, {c, ignore}]),
    ?assertMatch({abort, conflict}, check_prepared(#{}, #{a => <<>>}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(#{}, #{d => <<"hey">>}, PendingReads, PendingWrites)),

    %% The first aborts over a read-write conflict, while the second one doesn't, because `b` is decided
    true = ets:insert(PendingWrites, [{{a, tx_1}, prepared, 0}, {{b, tx_2}, decided, 0}]),
    ?assertMatch({abort, conflict}, check_prepared(#{a => 0}, #{}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(#{b => 0}, #{}, PendingReads, PendingWrites)),

    %% aborts due to write-read conflict
    ?assertMatch({abort, conflict}, check_prepared(#{a => 0, b => 0}, #{a => <<>>}, PendingReads, PendingWrites)),

    true = ets:delete(PendingReads),
    true = ets:delete(PendingWrites).

grb_paxos_state_check_committed_test() ->
    LastRed = ets:new(dummy_red, [set, {keypos, #last_red_record.key}]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),
    ok = persistent_term:put({grb_dc_manager, my_replica}, undefined),

    ?assertEqual({ok, #{}}, check_committed(#{}, #{}, PendingWrites, LastRed)),

    true = ets:insert(PendingWrites, [{{a, tx_1}, prepared, 10},
                                      {{b, tx_2}, decided, 0},
                                      {{c, tx_3}, decided, 10}]),

    %% this won't clash with a, even if it has a higher Red time, because it's in prepared, so we don't care
    ?assertEqual({ok, #{}}, check_committed(#{a => 0}, #{}, PendingWrites, LastRed)),

    %% this commits too, since we're not stale
    ?assertEqual({ok, #{}}, check_committed(#{b => 5}, #{}, PendingWrites, LastRed)),

    %% this will abort, since tx_3 will be delivered with a higher red timestamp
    ?assertEqual({{abort, stale_decided}, #{}}, check_committed(#{c => 8}, #{}, PendingWrites, LastRed)),

    %% none of these are affected by the above, since they don't share any keys
    ets:insert(LastRed, #last_red_record{key=0, red=10, length=1, clocks=[#{?RED_REPLICA => 10}]}),
    ?assertEqual({{abort, stale_committed}, #{}}, check_committed(#{0 => 9}, #{}, PendingWrites, LastRed)),
    ?assertMatch({ok, #{?RED_REPLICA := 10}}, check_committed(#{0 => 11}, #{}, PendingWrites, LastRed)),

    true = ets:delete(LastRed),
    true = ets:delete(PendingWrites),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_prune_decided_before_test() ->
    ok = persistent_term:put({grb_dc_manager, my_replica}, ignore),
    LastRed = ets:new(dummy, [set, {keypos, #last_red_record.key}]),

    with_states(grb_paxos_state:leader(), fun(L) ->

        TxId1 = tx_1, RS1 = #{a => 5}, WS1 = #{a => <<"hello">>}, PVC1 = #{},

        {ok, Ballot, CVC1} = grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastRed, L),
        ok = grb_paxos_state:decision(Ballot, TxId1, ok, CVC1, L),

        %% preparing after decision should return early
        ?assertEqual({already_decided, ok, CVC1} , grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastRed, L)),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC1),
        ?assertEqual({Expected, [{WS1, CVC1}]}, grb_paxos_state:get_next_ready(0, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Expected, L)),

        %% preparing even after delivery should return early
        ?assertEqual({already_decided, ok, CVC1} , grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastRed, L)),

        %% let's do a heartbeat now
        {ok, Ballot, Ts} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L),
        ok = grb_paxos_state:decision_hb(Ballot, {?heartbeat, 0}, Ts, L),

        ?assertEqual({Ts, [?heartbeat]}, grb_paxos_state:get_next_ready(Expected, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, L)),

        %% prune, now we should only have the heartbeat
        ok = grb_paxos_state:prune_decided_before(Expected + 1, L),
        ?assertMatch({Ts, [?heartbeat]}, grb_paxos_state:get_next_ready(0, L)),

        %% but if we prune past that, then the heartbeat should be gone
        ok = grb_paxos_state:prune_decided_before(Ts + 1, L),
        ?assertMatch(false, grb_paxos_state:get_next_ready(0, L)),

        %% now we can prepare this transaction again
        ?assertMatch({ok, Ballot, #{}} , grb_paxos_state:prepare(TxId1, RS1, WS1, PVC1, LastRed, L))
    end),

    true = ets:delete(LastRed),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

-endif.
