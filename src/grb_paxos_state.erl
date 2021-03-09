-module(grb_paxos_state).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(heartbeat, heartbeat).
-define(abort_conflict, {abort, conflict}).
-define(abort_stale_dec, {abort, stale_decided}).
-define(abort_stale_comm, {abort, stale_committed}).

%% We don't care about the structure too much, as long as the client ensures identifiers
%% are unique (or at least, unique to the point of not reusing them before they call
%% `prune_decided_before/2`
-type record_id() :: {?heartbeat, term()} | term().
-record(tx_data, {
    label = undefined :: tx_label() | undefined,
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
    ballot = 0 :: ballot(),
    entries = #{} :: #{record_id() => tx_data()},
    index :: cache_id(),

    %% A pair of inverted indices over the transactions read/write sets
    %% Used to compute write-write and read-write conflicts in a quick way
    pending_reads :: cache_id(),
    writes_cache :: cache_id()
}).

-type prepare_hb_result() :: {red_vote(), ballot(), grb_time:ts()}| {already_decided, red_vote(), grb_time:ts()}.
-type prepare_result() :: {red_vote(), ballot(), vclock()} | {already_decided, red_vote(), vclock()}.
-type decision_error() :: bad_ballot | not_prepared.
-type t() :: #state{}.

-type ready_tx() :: [ {?heartbeat, term()} | {term(), tx_label(), writeset(), vclock()}, ... ].

-export_type([t/0, prepare_result/0, prepare_hb_result/0, decision_error/0, ready_tx/0]).

%% constructors
-export([new/0,
         delete/1]).

%% ready / prune api
-export([get_next_ready/2,
         prune_decided_before/2]).

%% heartbeat api
-export([prepare_hb/2,
         accept_hb/4,
         decision_hb/4]).

%% transactional api
-export([prepare/8,
         accept/8,
         decision/5]).

%% util
-export([current_ballot/1,
         get_decided_data/2,
         deliver_is_valid_ballot/2]).

%%%===================================================================
%%% constructors
%%%===================================================================

-spec new() -> t().
new() ->
    #state{%% A tree table maintains an ordered queue of prepare/commit records. This allows
           %% simple computation of the precondition of get_next_ready/2, which delivers transactions
           %% in commitVC[red] order, but checks against prepared transactions with prepareVC[red]
           %% lower than commitVC[red]
           index=ets:new(state_entries_index, [ordered_set, {keypos, #index_entry.key}]),
           %% pending_reads is a bag, since we want to store {{Key, Label}, TxId} without clashes.
           %% we could also do a set / ordered_set with a compound key {{Key, Label, TxId}}.
           %% However, we want two operations over this table:
           %% 1. Is this {Key, Label} in the table? (don't care which TxId is associated with it)
           %% 2. Given a TxId, Label and a Key, delete {{Key, Label} TxId} as fast as possible
           %% If we went the set / ordered_set route, we would have to use select_count
           %% to perform (1), whereas by using a `bag`, we can simply use member/2
           pending_reads=ets:new(state_pending_reads, [bag]),
           %% For writes_cache, we store tuples {{Key, Label, TxId}, prepared/decided, VC}, so that
           %% we can re-use the table for certification_check against prepared and decided
           %% transactions.
           writes_cache=ets:new(state_pending_writes, [ordered_set])}.

-spec delete(t()) -> ok.
delete(#state{index=Index, pending_reads=PendingReads, writes_cache=Writes}) ->
    true = ets:delete(Index),
    true = ets:delete(PendingReads),
    true = ets:delete(Writes),
    ok.

%%%===================================================================
%%% util
%%%===================================================================

-spec current_ballot(t()) -> ballot().
current_ballot(#state{ballot=B}) ->
    B.

-spec get_decided_data(record_id(), t()) -> writeset().
get_decided_data(Id, #state{entries=EntryMap}) ->
    (maps:get(Id, EntryMap))#tx_data.writeset.

%%%===================================================================
%%% ready / prune
%%%===================================================================

-spec get_next_ready(LastDelivered :: grb_time:ts(),
                     State :: t()) -> false | {grb_time:ts(), ready_tx()}.

get_next_ready(LastDelivered, #state{entries=EntryMap, index=Idx}) ->
    %% We use an empty list here, since we know that record ids are tuples,
    %% and the built-in Erlang term ordering ensures that tuples < lists,
    %% for any tuple and list. An empty list will always compare larger than
    %% a record_id, so this basically allows us to skip any transactions with
    %% LastDelivered as its commit timestamp. Another option would be to use
    %% {(LastDelivered + 1), 0} to have the first key with Ts > LastDelivered.
    get_next_ready_continue(ets:next(Idx, {LastDelivered, []}), Idx, EntryMap).

-spec get_next_ready_continue(Key :: {grb_time:ts(), record_id()} | '$end_of_table',
                              IdxTable ::  cache_id(),
                              EntryMap :: #{record_id() => tx_data()}) -> false | {grb_time:ts(), ready_tx()}.

get_next_ready_continue('$end_of_table', _IdxTable, _EntryMap) ->
    false;

get_next_ready_continue(Key={CommitTime, Id}, IdxTable, EntryMap) ->
    case ets:lookup(IdxTable, Key) of
        [#index_entry{state=prepared, vote=ok}] ->
            %% We have found a prepared commit before we ever reached a matching transaction,
            %% which means we're not ready to deliver
            false;
        [#index_entry{state=decided, vote=ok}] ->
            %% Found a commit with no previous prepares,
            %% continue with this commit time.
            get_next_ready_continue(ets:next(IdxTable, Key), CommitTime,
                                    IdxTable, EntryMap, [data_for_id(Id, EntryMap)]);
        _ ->
            %% the transaction might be aborted, continue
            get_next_ready_continue(ets:next(IdxTable, Key), IdxTable, EntryMap)
    end.

-spec get_next_ready_continue(Key :: {grb_time:ts(), record_id()} | '$end_of_table',
                              CommitTime :: grb_time:ts(),
                              IdxTable :: cache_id(),
                              EntryMap :: #{record_id() => tx_data()},
                              Acc :: ready_tx()) -> false | {grb_time:ts(), ready_tx()}.

get_next_ready_continue(Key={CommitTime, Id}, CommitTime, IdxTable, EntryMap, Acc) ->
    case ets:lookup(IdxTable, Key) of
        [#index_entry{state=prepared, vote=ok}] ->
            %% Found a prepared transaction with our commit time, we're not ready
            false;
        [#index_entry{state=decided, vote=ok}] ->
            %% Good transaction, add it to the pile
            get_next_ready_continue(ets:next(IdxTable, Key), CommitTime, IdxTable, EntryMap, [data_for_id(Id, EntryMap) | Acc]);
        _ ->
            %% the transaction might be aborted, continue
            get_next_ready_continue(ets:next(IdxTable, Key), CommitTime, IdxTable, EntryMap, Acc)
    end;

get_next_ready_continue(_, CommitTime, _, _, Acc) ->
    %% As soon as we go out of range, return the accumulator.
    {CommitTime, Acc}.

-spec data_for_id(record_id(), #{record_id() => tx_data()}) -> {?heartbeat, term()} | {term(), tx_label(), writeset(), vclock()}.
data_for_id({?heartbeat, Id}, _) -> {?heartbeat, Id};
data_for_id(Id, EntryMap) ->
    #tx_data{label=Label, writeset=WS, clock=CommitVC} = maps:get(Id, EntryMap),
    {Id, Label, WS, CommitVC}.

%% @doc Remove all transactions in decidedRed with a commitVC[red] timestamp lower than the given one
%%
%%      This should only be done when the caller is sure that _all_ replicas have delivered all
%%      transactions below the supplied time.
%%
%%      It will clean up all pending data, and after this, it will be possible to re-prepare a
%%      transaction with the same transaction identifier.
%%
-spec prune_decided_before(grb_time:ts(), t()) -> t().
prune_decided_before(MinLastDelivered, State=#state{index=Idx}) ->
    prune_decided_before(ets:first(Idx), MinLastDelivered, State).

-spec prune_decided_before(record_id() | '$end_of_table', grb_time:ts(), t()) -> t().
prune_decided_before(Key={CommitTime, Id}, MinLastDelivered, S=#state{index=Idx})
    when CommitTime < MinLastDelivered ->
        case ets:lookup_element(Idx, Key, #index_entry.state) of
            prepared ->
                %% Ignore prepared transactions
                prune_decided_before(ets:next(Idx, Key), MinLastDelivered, S);
            decided ->
                Decision = ets:lookup_element(Idx, Key, #index_entry.vote),
                prune_decided_before(
                    ets:next(Idx, Key),
                    MinLastDelivered,
                    prune_decided_transaction(Id, CommitTime, Decision, S)
                )
        end;

prune_decided_before(_Key, _MinLastDelivered, S) ->
    S.

-spec prune_decided_transaction(record_id(), grb_time:ts(), red_vote(), t()) -> t().
prune_decided_transaction(Id, Time, Decision, S=#state{index=Idx, entries=EntryMap0, writes_cache=Writes}) ->
    {#tx_data{label=Label, write_keys=WrittenKeys}, EntryMap} = maps:take(Id, EntryMap0),
    ok = clear_pending(Id, Label, Decision, WrittenKeys, Writes),
    true = ets:delete(Idx, {Time, Id}),
    S#state{entries=EntryMap}.

-spec clear_pending(record_id(), tx_label(), red_vote(), [key()] | undefined, cache_id()) -> ok.
clear_pending(_Id, _, {abort, _}, _WrittenKeys, _Writes) -> ok;
clear_pending({?heartbeat, _}, _, ok, _WrittenKeys, _Writes) -> ok;
clear_pending(Id, Label, ok, Keys, Writes) ->
    lists:foreach(fun(K) -> ets:delete(Writes, {K, Label, Id}) end, Keys).

%%%===================================================================
%%% heartbeat api
%%%===================================================================

-spec prepare_hb({?heartbeat, _}, t()) -> {prepare_hb_result(), t()}.
-dialyzer({nowarn_function, prepare_hb/2}).
prepare_hb(Id, State) ->
    prepare(Id, ignore, ignore, ignore, ignore, ignore, ignore, State).

-spec accept_hb(ballot(), record_id(), grb_time:ts(), t()) -> {ok, t()} | bad_ballot.
-dialyzer({nowarn_function, accept_hb/4}).
accept_hb(Ballot, Id, Ts, State) ->
    accept(Ballot, Id, ignore, ignore, ignore, ok, Ts, State).

-spec decision_hb(ballot(), record_id(), grb_time:ts(), t()) -> {ok, t()} | decision_error().
-dialyzer({nowarn_function, decision_hb/4}).
decision_hb(Ballot, Id, Ts, State) ->
    decision(Ballot, Id, ok, Ts, State).

%%%===================================================================
%%% transaction api
%%%===================================================================

-spec prepare(Id :: record_id(),
              Label :: tx_label(),
              RS :: readset(),
              WS :: writeset(),
              SnapshotVC :: vclock(),
              LastVC :: grb_oplog_vnode:last_vc(),
              Conflicts :: conflict_relations(),
              State :: t()) -> {prepare_result(), t()}.

prepare(Id, Label, RS, WS, SnapshotVC, LastVC, Conflicts, State=#state{ballot=Ballot,
                                                                       entries=EntryMap,
                                                                       pending_reads=PendingReads,
                                                                       writes_cache=PendingWrites}) ->

    case maps:get(Id, EntryMap, empty) of
        #tx_data{state=decided, vote=Decision, clock=CommitVC} ->
            {{already_decided, Decision, CommitVC}, State};

        #tx_data{state=prepared, vote=Decision, clock=PrepareVC} ->
            {{Decision, Ballot, PrepareVC}, State};

        empty ->
            Decision = check_transaction(Id, Label, RS, WS, SnapshotVC,
                                         LastVC, PendingReads, PendingWrites, Conflicts),
            {RedTs, PrepareVC} = assign_timestamp(SnapshotVC),
            Record = make_prepared(Id, Label, RS, WS, RedTs, PrepareVC, Decision, State),
            {{Decision, Ballot, PrepareVC}, State#state{entries=EntryMap#{Id => Record}}}
    end.

%% todo(borja, red): make idempotent, ignore if the transaction has been prepared/decided
%% although the leader should make it impossible for us to receive a prepare for a received
%% transaction (replying with already_prepared), it's better to avoid it.
%% if it's already prepared, don't do anything.
-spec accept(ballot(), record_id(), tx_label(), readset(), writeset(), red_vote(), vclock(), t()) -> {ok, t()} | bad_ballot.
accept(InBallot, _, _, _, _, _, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept(Ballot, Id, Label, RS, WS, Vote, PrepareVC, S=#state{ballot=Ballot, entries=EntryMap}) ->
    RedTs = get_timestamp(Id, PrepareVC),
    Record = make_prepared(Id, Label, RS, WS, RedTs, PrepareVC, Vote, S),
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


-spec make_prepared(record_id(), tx_label(), readset(), writeset(), grb_time:ts(), vclock(), red_vote(), t()) -> tx_data().
make_prepared(Id, Label, RS, WS, RedTs, VC, Decision, #state{index=Idx,
                                                             pending_reads=Reads,
                                                             writes_cache=Writes}) ->

    true = ets:insert(Idx, #index_entry{key={RedTs, Id}, state=prepared, vote=Decision}),
    case Decision of
        ok ->
            make_commit_record(Id, Label, RS, WS, RedTs, VC, Decision, Reads, Writes);
        _ ->
            %% don't store read / written keys, waste of space
            #tx_data{label=Label, writeset=WS, red_ts=RedTs, clock=VC, state=prepared, vote=Decision}
    end.

-spec make_commit_record(record_id(), tx_label(), readset(), writeset(), grb_time:ts(), _, red_vote(), cache_id(), cache_id()) -> tx_data().
make_commit_record({?heartbeat, _}, _, _, _, RedTs, _, Decision, _, _) ->
    #tx_data{label=undefined, read_keys=undefined, write_keys=undefined, writeset=undefined,
             red_ts=RedTs, clock=RedTs, state=prepared, vote=Decision};

make_commit_record(TxId, Label, RS, WS, RedTs, VC, Decision, Reads, Writes) ->
    %% We only add the read / written keys if the transaction has a commit vote,
    %% since we're only concerned about conflicts with committed votes. If we vote
    %% abort, we know we will abort in the end, so we don't need to be conservative.
    true = ets:insert(Reads, [{{K, Label}, TxId} || K <- RS]),
    WriteKeys = maps:keys(WS),
    true = ets:insert(Writes, [ {{K, Label, TxId}, prepared, VC} || K <- WriteKeys]),
    #tx_data{label=Label, read_keys=RS, write_keys=WriteKeys, writeset=WS,
             red_ts=RedTs, clock=VC, state=prepared, vote=Decision}.

-spec deliver_is_valid_ballot(ballot(), t()) -> boolean().
deliver_is_valid_ballot(InBallot, #state{ballot=Ballot}) ->
    InBallot =< Ballot.

-spec decision_pre(ballot(), record_id(), t()) -> {ok, tx_data()}
                                                | already_decided
                                                | decision_error().

decision_pre(InBallot, _, #state{ballot=Ballot}) when InBallot > Ballot ->
    bad_ballot;

decision_pre(_, Id, #state{entries=EntryMap}) ->
    case maps:get(Id, EntryMap, not_prepared) of
        not_prepared ->
            not_prepared;

        #tx_data{state=decided} ->
            ?LOG_WARNING("duplicate DECIDE(~p)", [Id]),
            already_decided;

        Data=#tx_data{state=prepared} ->
            {ok, Data}
    end.

-spec decision(ballot(), record_id(), red_vote(), vclock(), t()) -> {ok, t()} | decision_error().
decision(Ballot, Id, Vote, CommitVC, S=#state{entries=EntryMap, index=Idx,
                                              pending_reads=PrepReads, writes_cache=WriteCache}) ->

    RedTs = get_timestamp(Id, CommitVC),
    case decision_pre(Ballot, Id, S) of
        already_decided ->
            {ok, S};

        {ok, Data=#tx_data{red_ts=OldTs, label=Label, vote=OldVote, read_keys=ReadKeys, write_keys=WriteKeys}} ->
            true = ets:delete(Idx, {OldTs, Id}),
            %% if we voted commit during prepare, we need to clean up / move pending data
            %% if we voted abort, we didn't store anything
            case OldVote of
                ok -> move_pending_data(Id, Label, CommitVC, Vote, ReadKeys, WriteKeys, PrepReads, WriteCache);
                _ -> ok
            end,
            true = ets:insert(Idx, #index_entry{key={RedTs, Id}, state=decided, vote=Vote}),
            NewData = Data#tx_data{red_ts=RedTs, clock=CommitVC, state=decided, vote=Vote},
            {ok, S#state{entries=EntryMap#{Id => NewData}}};

        Other -> Other
    end.

-spec move_pending_data(Id :: record_id(),
                        Label :: tx_label(),
                        CommitVC :: vclock(),
                        Vote :: red_vote(),
                        RKeys :: readset() | undefined,
                        WKeys :: [key()] | undefined,
                        PendingReads :: cache_id(),
                        WriteCache :: cache_id()) -> ok.

%% for a heartbeat, we never stored anything, so there's no need to move any data
move_pending_data({?heartbeat, _}, _, _, _, _, _, _, _) -> ok;
move_pending_data(Id, Label, CommitVC, Vote, RKeys, WKeys, PendingReads, WriteCache) ->
    %% Remove reads, since we only care about them while the transaction is prepared
    %% If the vote is abort, we still might have stored these reads if we voted commit
    lists:foreach(fun(ReadKey) ->
        ets:delete_object(PendingReads, {{ReadKey, Label}, Id})
    end, RKeys),

    case Vote of
        {abort, _} ->
            %% if the vote is abort, don't bother to move, simply delete
            lists:foreach(fun(K) -> ets:delete(WriteCache, {K, Label, Id}) end, WKeys);
        ok ->
            %% if the vote is commit, we should mark all the entries as decided,
            %% and add the commit vector
            lists:foreach(fun(K) ->
                true = ets:update_element(WriteCache, {K, Label, Id}, [{2, decided}, {3, CommitVC}])
            end, WKeys)
    end.

-spec check_transaction(TxId :: record_id(),
                        Label :: tx_label(),
                        RS :: readset(),
                        WS :: writeset(),
                        SnapshotVC :: vclock(),
                        LastVC :: grb_oplog_vnode:last_vc(),
                        PrepReads :: cache_id(),
                        Writes :: cache_id(),
                        Conflicts :: conflict_relations()) -> red_vote().

check_transaction(_, Label, RS, WS, SnapshotVC, LastVC, PrepReads, Writes, Conflicts)
    when is_map(Conflicts) andalso is_map_key(Label, Conflicts) ->

        ConflictLabel = maps:get(Label, Conflicts),
        case check_prepared(ConflictLabel, RS, WS, PrepReads, Writes) of
            {abort, _}=Abort ->
                Abort;
            ok ->
                check_committed(ConflictLabel, RS, SnapshotVC, Writes, LastVC)
        end;

check_transaction(_, _, _, _, _, _, _, _, _) ->
    ok.

-spec check_prepared(ConflictLabel :: tx_label(),
                     RS :: readset(),
                     WS :: writeset(),
                     PendingReads :: cache_id(),
                     PendingWrites :: cache_id()) -> red_vote().

check_prepared(ConflictLabel, RS, WS, PendingReads, PendingWrites) ->
    RWConflict = lists:any(fun(Key) ->
        0 =/= ets:select_count(PendingWrites, [{ {{Key, ConflictLabel, '_'}, prepared, '_'}, [], [true] }])
    end, RS),
    case RWConflict of
        true -> ?abort_conflict;
        false ->
            WRConflict = lists:any(fun(Key) ->
                true =:= ets:member(PendingReads, {Key, ConflictLabel})
            end, maps:keys(WS)),
            case WRConflict of
                true -> ?abort_conflict;
                false -> ok
            end
    end.

-spec check_committed(tx_label(), readset(), vclock(), cache_id(), grb_oplog_vnode:last_vc()) -> red_vote().
check_committed(ConflictLabel, RS, SnapshotVC, PendingWrites, LastVC) ->
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    try
        lists:foreach(fun(Key) ->
            case stale_decided(Key, ConflictLabel, SnapshotVC, AllReplicas, PendingWrites) of
                true ->
                    throw(?abort_stale_dec);
                false ->
                    case stale_committed(Key, ConflictLabel, SnapshotVC, AllReplicas, LastVC) of
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

stale_decided(Key, ConflictLabel, PrepareVC, AtReplicas, PendingWrites) ->
    Match = ets:select(PendingWrites, [{ {{Key, ConflictLabel, '_'}, decided, '$1'}, [], ['$1'] }]),
    case Match of
        [] ->
            false;
        Vectors ->
            lists:any(fun(CommitVC) ->
                not (grb_vclock:leq_at_keys(AtReplicas, CommitVC, PrepareVC))
            end, Vectors)
    end.

-spec stale_committed(key(), tx_label(), vclock(), [replica_id()], grb_oplog_vnode:last_vc()) -> boolean().
stale_committed(Key, ConflictLabel, PrepareVC, AtReplicas, LastVC) ->
    case ets:lookup(LastVC, {Key, ConflictLabel}) of
        [{_, LastCommitVC}] ->
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
    with_states([grb_paxos_state:new(),
                 grb_paxos_state:new()], fun([L0, F0]) ->

        Id = {?heartbeat, 0},
        {{ok, Ballot, Ts}, L1} = grb_paxos_state:prepare_hb(Id, L0),
        {ok, F1} = grb_paxos_state:accept_hb(Ballot, Id, Ts, F0),

        {ok, L2} = grb_paxos_state:decision_hb(Ballot, Id, Ts, L1),
        {ok, F2} = grb_paxos_state:decision_hb(Ballot, Id, Ts, F1),

        {Ts, _} = grb_paxos_state:get_next_ready(0, L2),
        ?assertMatch({Ts, [Id]}, grb_paxos_state:get_next_ready(0, F2)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, L2)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, F2))
    end).

%% todo(borja, test): figure out if we can setup / teardown this
grb_paxos_state_transaction_test() ->
    ok = persistent_term:put({grb_dc_manager, my_replica}, ignore),
    LastVC = ets:new(dummy, [set]),
    with_states([grb_paxos_state:new(),
                 grb_paxos_state:new()], fun([L0, F0]) ->

        TxId = tx_1, Label = <<>>,
        RS = [], WS = #{},
        PVC = #{}, Conflicts = #{},

        {Res={ok, Ballot, CVC}, L1} = grb_paxos_state:prepare(TxId, Label, RS, WS, PVC, LastVC, Conflicts, L0),

        %% preparing again should result in the same result
        {OtherRes, L2} = grb_paxos_state:prepare(TxId, Label, RS, WS, PVC, LastVC, Conflicts, L1),
        ?assertEqual(Res, OtherRes),
        ?assertEqual(L1, L2),

        {ok, F1} = grb_paxos_state:accept(Ballot, TxId, Label, RS, WS, ok, CVC, F0),

        {ok, L3} = grb_paxos_state:decision(Ballot, TxId, ok, CVC, L2),
        {ok, F2} = grb_paxos_state:decision(Ballot, TxId, ok, CVC, F1),

        %% preparing after decision should return early
        {PrepRes, L4} = grb_paxos_state:prepare(TxId, Label, RS, WS, PVC, LastVC, Conflicts, L3),
        ?assertEqual({already_decided, ok, CVC} , PrepRes),
        ?assertEqual(L3 , L4),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC),
        Ready={CommitTime, [{TxId, Label, WS, Clock}]} = grb_paxos_state:get_next_ready(0, L4),
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
    with_states(grb_paxos_state:new(), fun(F0) ->
        [V0, V1, V2] = [ VC(1), VC(5), VC(7)],
        {ok, F1} = grb_paxos_state:accept(0, tx_0, <<>>, [], #{}, ok, V0, F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_1, <<>>, [], #{}, ok, V1, F1),
        {ok, F3} = grb_paxos_state:accept(0, tx_2, <<>>, [], #{}, ok, V2, F2),

        {ok, F4} = grb_paxos_state:decision(0, tx_1, ok, V1, F3),
        {ok, F5} = grb_paxos_state:decision(0, tx_2, ok, V2, F4),

        %% false, still have to decide tx_0, and it was prepared with a lower time
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F5)),
        %% now decide tx_0, marking tx_1 ready for delivery
        {ok, F6} = grb_paxos_state:decision(0, tx_0, ok, V0, F5),

        ?assertEqual({T(V0), [{tx_0, <<>>, #{}, V0}]}, grb_paxos_state:get_next_ready(0, F6)),
        ?assertEqual({T(V1), [{tx_1, <<>>, #{}, V1}]}, grb_paxos_state:get_next_ready(T(V0), F6)),
        ?assertEqual({T(V2), [{tx_2, <<>>, #{}, V2}]}, grb_paxos_state:get_next_ready(T(V1), F6))
    end),

    %% simple example of a tx being blocked by a heartbeat
    with_states(grb_paxos_state:new(), fun(F0) ->
        [T0, V1] = [ 1, VC(5)],

        {ok, F1} = grb_paxos_state:accept_hb(0, {?heartbeat, 0}, T0, F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_1, <<>>, [], #{}, ok, V1, F1),

        {ok, F3} = grb_paxos_state:decision(0, tx_1, ok, V1, F2),

        %% false, still have to decide the heartbeat
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F3)),

        %% now decide the heartbeat, marking tx_1 ready for delivery
        {ok, F4} = grb_paxos_state:decision_hb(0, {?heartbeat, 0}, T0, F3),

        ?assertEqual({T0, [{?heartbeat, 0}]}, grb_paxos_state:get_next_ready(0, F4)),
        ?assertEqual({T(V1), [{tx_1, <<>>, #{}, V1}]}, grb_paxos_state:get_next_ready(T0, F4))
    end),

    %% more complex with transactions moving places
    with_states(grb_paxos_state:new(), fun(F0) ->
        [V0, V1, V2, V3, V4] = [ VC(1), VC(5), VC(7), VC(10), VC(15)],

        {ok, F1} = grb_paxos_state:accept(0, tx_0, <<>>, [], #{}, {abort, ignore}, V0, F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_1, <<>>, [], #{}, ok, V1, F1),
        {ok, F3} = grb_paxos_state:accept(0, tx_2, <<>>, [], #{}, ok, V2, F2),
        {ok, F4} = grb_paxos_state:accept(0, tx_3, <<>>, [], #{}, ok, V3, F3),

        {ok, F5} = grb_paxos_state:decision(0, tx_1, ok, V1, F4),

        %% we can skip tx_0, since it was aborted, no need to wait for decision (because decision will be abort)
        ?assertEqual({T(V1), [{tx_1, <<>>, #{}, V1}]}, grb_paxos_state:get_next_ready(0, F5)),

        %% now we decide tx_2 with a higher time, placing it the last on the queue
        {ok, F6} = grb_paxos_state:decision(0, tx_2, ok, V4, F5),

        %% and decide tx_3, making it ready for delivery
        {ok, F7} = grb_paxos_state:decision(0, tx_3, ok, V3, F6),

        ?assertEqual({T(V3), [{tx_3, <<>>, #{}, V3}]}, grb_paxos_state:get_next_ready(T(V1), F7)),
        ?assertEqual({T(V4), [{tx_2, <<>>, #{}, V4}]}, grb_paxos_state:get_next_ready(T(V3), F7))
    end).

grb_paxos_state_hb_clash_test() ->
    with_states(grb_paxos_state:new(), fun(L0) ->
        {{ok, B, Ts0}, L1} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L0),
        {ok, L2} = grb_paxos_state:decision_hb(B, {?heartbeat, 0}, Ts0, L1),

        ?assertEqual({Ts0, [{?heartbeat, 0}]}, grb_paxos_state:get_next_ready(0, L2)),
        {PrepRes, L3} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L2),
        ?assertMatch({already_decided, ok, Ts0}, PrepRes),

        ?assertEqual({Ts0, [{?heartbeat, 0}]}, grb_paxos_state:get_next_ready(0, L3)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts0, L3))
    end).

grb_paxos_state_tx_clash_test() ->
    VC = fun(I) -> #{?RED_REPLICA => I} end,

    with_states(grb_paxos_state:new(), fun(F0) ->
        {ok, F1} = grb_paxos_state:accept(0, tx_1, <<>>, [], #{a => 0}, ok, VC(0), F0),
        {ok, F2} = grb_paxos_state:accept(0, tx_2, <<>>, [], #{a => 1}, ok, VC(5), F1),

        {ok, F3} = grb_paxos_state:decision(0, tx_1, ok, VC(5), F2),

        %% If we allow to deliver tx_1, we would overtake tx_2 and prevent it from ever being delivered
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F3) ),

        {ok, F4} = grb_paxos_state:decision(0, tx_2, ok, VC(5), F3),

        %% Now we can deliver, since tx_2 moved out of the prepare queue
        %% Order is not guaranteed, so sort both.
        {CommitTime, Ready} = grb_paxos_state:get_next_ready(0, F4),
        ?assertEqual(5, CommitTime),
        ?assertEqual(lists:sort([ {tx_1, <<>>, #{a => 0}, VC(5)},
                                  {tx_2, <<>>, #{a => 1}, VC(5)} ]), lists:sort(Ready) )
    end).

grb_paxos_state_check_prepared_sym_test() ->
    PendingReads = ets:new(dummy_reads, [bag]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),
    Label = <<>>,

    ?assertEqual(ok, check_prepared(Label, [], #{}, PendingReads, PendingWrites)),

    true = ets:insert(PendingReads, [
        {{a, Label}, ignore},
        {{b, Label}, ignore},
        {{c, Label}, ignore}
    ]),
    ?assertMatch(?abort_conflict, check_prepared(Label, [], #{a => <<>>}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(Label, [], #{d => <<"hey">>}, PendingReads, PendingWrites)),

    %% The first aborts over a read-write conflict, while the second one doesn't, because `b` is decided
    true = ets:insert(PendingWrites, [{{a, Label, tx_1}, prepared, 0}, {{b, Label, tx_2}, decided, 0}]),
    ?assertMatch(?abort_conflict, check_prepared(Label, [a], #{}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(Label, [b], #{}, PendingReads, PendingWrites)),

    %% aborts due to write-read conflict
    ?assertMatch(?abort_conflict, check_prepared(Label, [a, b], #{a => <<>>}, PendingReads, PendingWrites)),

    true = ets:delete(PendingReads),
    true = ets:delete(PendingWrites).

grb_paxos_state_check_prepared_asym_test() ->
    PendingReads = ets:new(dummy_reads, [bag]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),
    Label1 = <<"a">>,
    Label2 = <<"b">>,

    ?assertEqual(ok, check_prepared(Label1, [], #{}, PendingReads, PendingWrites)),

    true = ets:insert(PendingReads, [
        {{a, Label1}, ignore},
        {{b, Label2}, ignore}
    ]),

    %% Conflicts on write-read, but only if written by a conflicting transaction
    ?assertMatch(?abort_conflict, check_prepared(Label1, [], #{a => <<>>}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(Label2, [], #{a => <<>>}, PendingReads, PendingWrites)),

    %% Even if there's a conflicting transaction, it only triggers on the same keys
    ?assertMatch(ok, check_prepared(Label1, [], #{d => <<"hey">>}, PendingReads, PendingWrites)),

    %% The first aborts over a read-write conflict, while the second one doesn't, because `b` is decided
    true = ets:insert(PendingWrites, [
        {{a, Label1, tx_1}, prepared, 0},
        {{b, Label2, tx_2}, decided, 0}
    ]),
    ?assertMatch(?abort_conflict, check_prepared(Label1, [a], #{}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(Label2, [b], #{}, PendingReads, PendingWrites)),

     %% aborts due to write-read conflict on a conflicting transaction
    ?assertMatch(?abort_conflict, check_prepared(Label1, [a, b], #{a => <<>>}, PendingReads, PendingWrites)),
    ?assertMatch(ok, check_prepared(Label2, [a, b], #{a => <<>>}, PendingReads, PendingWrites)),

    true = ets:delete(PendingReads),
    true = ets:delete(PendingWrites).

grb_paxos_state_check_committed_sym_test() ->
    LastVC = ets:new(dummy_vc, [set]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),
    ok = persistent_term:put({grb_dc_manager, my_replica}, undefined),
    Label = <<>>,

    ?assertMatch(ok, check_committed(Label, [], #{}, PendingWrites, LastVC)),

    true = ets:insert(PendingWrites, [{{a, Label, tx_1}, prepared, #{?RED_REPLICA => 10}},
                                      {{b, Label, tx_2}, decided, #{?RED_REPLICA => 0}},
                                      {{c, Label, tx_3}, decided, #{?RED_REPLICA => 10}}]),

    %% this won't clash with a, even if it has a higher Red time, because it's in prepared, so we don't care
    ?assertMatch(ok, check_committed(Label, [a], #{}, PendingWrites, LastVC)),

    %% this commits too, since we're not stale
    ?assertMatch(ok, check_committed(Label, [b], #{?RED_REPLICA => 5}, PendingWrites, LastVC)),

    %% this will abort, since tx_3 will be delivered with a higher red timestamp
    ?assertMatch(?abort_stale_dec, check_committed(Label, [c], #{?RED_REPLICA => 8}, PendingWrites, LastVC)),

    %% none of these are affected by the above, since they don't share any keys
    true = ets:insert(LastVC, {{0, Label}, #{?RED_REPLICA => 10}}),
    ?assertMatch(?abort_stale_comm, check_committed(Label, [0], #{?RED_REPLICA => 9}, PendingWrites, LastVC)),
    ?assertMatch(ok, check_committed(Label, [0], #{?RED_REPLICA => 11}, PendingWrites, LastVC)),

    true = ets:delete(LastVC),
    true = ets:delete(PendingWrites),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_check_committed_asym_test() ->
    LastVC = ets:new(dummy_vc, [set]),
    PendingWrites =  ets:new(dummy_writes, [ordered_set]),
    ok = persistent_term:put({grb_dc_manager, my_replica}, undefined),
    Label1 = <<"a">>,
    Label2 = <<"b">>,

    ?assertMatch(ok, check_committed(Label1, [], #{}, PendingWrites, LastVC)),

    true = ets:insert(PendingWrites, [{{a, Label1, tx_1}, prepared, #{?RED_REPLICA => 10}},
                                      {{b, Label1, tx_2}, decided, #{?RED_REPLICA => 0}},
                                      {{c, Label2, tx_3}, decided, #{?RED_REPLICA => 10}}]),

    %% this won't clash with a, even if it has a higher Red time, because it's in prepared, so we don't care
    ?assertMatch(ok, check_committed(Label1, [a], #{}, PendingWrites, LastVC)),

    %% this commits too, since we're not stale
    ?assertMatch(ok, check_committed(Label1, [b], #{?RED_REPLICA => 5}, PendingWrites, LastVC)),

    %% this will abort, since tx_3 will be delivered with a higher red timestamp
    ?assertMatch(?abort_stale_dec, check_committed(Label2, [c], #{?RED_REPLICA => 8}, PendingWrites, LastVC)),

    %% none of these are affected by the above, since they don't share any keys
    true = ets:insert(LastVC, {{0, Label1}, #{?RED_REPLICA => 10}}),
    ?assertMatch(?abort_stale_comm, check_committed(Label1, [0], #{?RED_REPLICA => 9}, PendingWrites, LastVC)),
    ?assertMatch(ok, check_committed(Label1, [0], #{?RED_REPLICA => 11}, PendingWrites, LastVC)),

    true = ets:delete(LastVC),
    true = ets:delete(PendingWrites),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

grb_paxos_state_prune_decided_before_test() ->
    ok = persistent_term:put({grb_dc_manager, my_replica}, ignore),
    LastVC = ets:new(dummy, [set]),

    with_states(grb_paxos_state:new(), fun(L0) ->

        TxId1 = tx_1, Label1 = <<>>,
        RS1 = [a], WS1 = #{a => <<"hello">>},
        PVC1 = #{?RED_REPLICA => 5}, Conflicts = #{},

        {{ok, Ballot, CVC1}, L1} = grb_paxos_state:prepare(TxId1, Label1, RS1, WS1, PVC1, LastVC, Conflicts, L0),
        {ok, L2} = grb_paxos_state:decision(Ballot, TxId1, ok, CVC1, L1),

        %% preparing after decision should return early
        {PrepRes0, L3} = grb_paxos_state:prepare(TxId1, Label1, RS1, WS1, PVC1, LastVC, Conflicts, L2),
        ?assertEqual({already_decided, ok, CVC1}, PrepRes0),
        ?assertEqual(L2, L3),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC1),
        ?assertEqual({Expected, [{TxId1, Label1, WS1, CVC1}]}, grb_paxos_state:get_next_ready(0, L3)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Expected, L3)),

        %% preparing even after delivery should return early
        {PrepRes1, L4} = grb_paxos_state:prepare(TxId1, Label1, RS1, WS1, PVC1, LastVC, Conflicts, L3),
        ?assertEqual({already_decided, ok, CVC1}, PrepRes1),
        ?assertEqual(L3, L4),

        %% let's do a heartbeat now
        {{ok, Ballot, Ts}, L5} = grb_paxos_state:prepare_hb({?heartbeat, 0}, L4),
        {ok, L6} = grb_paxos_state:decision_hb(Ballot, {?heartbeat, 0}, Ts, L5),

        ?assertEqual({Ts, [{?heartbeat, 0}]}, grb_paxos_state:get_next_ready(Expected, L5)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(Ts, L5)),

        %% prune, now we should only have the heartbeat
        L7 = grb_paxos_state:prune_decided_before(Expected + 1, L6),
        ?assertMatch({Ts, [{?heartbeat, 0}]}, grb_paxos_state:get_next_ready(0, L7)),

        %% but if we prune past that, then the heartbeat should be gone
        L8 = grb_paxos_state:prune_decided_before(Ts + 1, L7),
        ?assertMatch(false, grb_paxos_state:get_next_ready(0, L6)),

        %% now we can prepare this transaction again
        {PrepRes2, _} = grb_paxos_state:prepare(TxId1, Label1, RS1, WS1, PVC1, LastVC, Conflicts, L8),
        ?assertMatch({ok, Ballot, #{}}, PrepRes2)
    end),

    true = ets:delete(LastVC),
    true = persistent_term:erase({grb_dc_manager, my_replica}).

-endif.
