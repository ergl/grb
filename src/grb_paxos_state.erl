-module(grb_paxos_state).
-include("grb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(leader, leader).
-define(follower, follower).
-define(heartbeat, heartbeat_entry).
-type status() :: ?leader | ?follower.

-type record_id() :: ?heartbeat | {tx, term()}.
-record(record, {
    id :: record_id(),
    writeset = #{} :: #{},
    red :: grb_time:ts(),
    clock = undefined :: vclock() | undefined,
    state :: prepared | decided,
    vote :: red_vote()
}).

%% fixme(borja, red): red time clash?
%% imagine we have (accepted) transactions X0 and X1 with times T0 and T1.
%% Now we receive a decision for X0 with time T1 (maybe another partition prepared it with T1).
%% Right now, we will overwrite on top of X1 in the index, thereby losing track of it.
%% If we receive a decsion for X1 with time T1 again, we will overwrite X0, etc
%% Possible solution: either make the index time -> list (like we don in other places)
%% or make the key in the index {time, id}, which keeps the same ordering properties
-record(index_entry, {
    red :: grb_time:ts(),
    id :: record_id(),
    state :: prepared | decided,
    vote :: red_vote()
}).

-record(state, {
    status :: status(),
    ballot = 0 :: ballot(),
    entries :: cache_id(),
    index :: cache_id()
}).

-type decision_error() :: bad_ballot | not_prepared | not_ready.
-type t() :: #state{}.

-export_type([t/0, decision_error/0]).

%% constructors
-export([leader/0,
         follower/0,
         delete/1]).

%% ready api
-export([get_next_ready/2]).

%% heartbeat api
-export([prepare_hb/1,
         accept_hb/3,
         decision_hb/2]).

%% transactional api
-export([prepare/5,
         accept/6,
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
           index=ets:new(state_entries_index, [ordered_set, {keypos, #index_entry.red}])}.

-spec delete(t()) -> ok.
delete(#state{entries=Entries, index=Index}) ->
  true = ets:delete(Entries),
  true = ets:delete(Index),
  ok.

%%%===================================================================
%%% ready
%%%===================================================================

-spec get_next_ready(grb_time:ts(), t()) -> false | {heartbeat, grb_time:ts()} | {grb_time:ts(), #{}, vclock()}.
get_next_ready(LastDelivered, #state{entries=Entries, index=Idx}) ->
    Result = ets:select(Idx,
                        [{ #index_entry{red='$1', id='$2', state=decided, vote=ok},
                           [{'>', '$1', {const, LastDelivered}}],
                           [{{'$1', '$2'}}] }],
                        1),
    case Result of
        '$end_of_table' -> false;
        {[{CommitTime, Id}] , _} ->
            case prep_committed_between(LastDelivered, CommitTime, Idx) of
                true -> false;
                false ->
                    case Id of
                        ?heartbeat ->
                            {heartbeat, CommitTime};
                        _ ->
                            [[WS, CommitVC]] =
                                ets:match(Entries, #record{id=Id, writeset='$1', clock='$2', _='_'}),

                            {CommitTime, WS, CommitVC}
                    end
            end
    end.

-spec prep_committed_between(grb_time:ts(), grb_time:ts(), cache_id()) -> boolean().
prep_committed_between(From, To, Table) ->
    0 =/= ets:select_count(Table,
                          [{ #index_entry{red='$1', state=prepared, vote=ok, _='_'},
                             [{'andalso', {'>', '$1', {const, From}},
                              {'<', '$1', {const, To}}}],
                             [true]  }]).

%%%===================================================================
%%% heartbeat api
%%%===================================================================

%% fixme(borja, red): If we don't remove heartbeats, we might clash when we find two heartbeats in the queue
%% check if this is a problem
-spec prepare_hb(t()) -> {ballot(), grb_time:ts()}.
prepare_hb(#state{status=?leader, ballot=Ballot, entries=Entries, index=Idx}) ->
    Ts = grb_time:timestamp(),
    true = ets:insert(Entries, #record{id=?heartbeat, red=Ts, vote=ok, state=prepared}),
    true = ets:insert(Idx, #index_entry{red=Ts, id=?heartbeat, state=prepared, vote=ok}),
    {Ballot, Ts}.

-spec accept_hb(ballot(), grb_time:ts(), vclock()) -> ok | bad_ballot.
accept_hb(InBallot, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept_hb(Ballot, Ts, #state{status=?follower, ballot=Ballot, entries=Entries, index=Idx}) ->
    true = ets:insert(Entries, #record{id=?heartbeat, red=Ts, vote=ok, state=prepared}),
    true = ets:insert(Idx, #index_entry{red=Ts, id=?heartbeat, state=prepared, vote=ok}),
    ok.

-spec decision_hb_pre(ballot(), grb_time:ts(), t()) -> ok | bad_ballot | not_prepared | not_ready.
decision_hb_pre(InBallot, _, #state{ballot=Ballot}) when InBallot > Ballot -> bad_ballot;

decision_hb_pre(_, _, #state{status=?follower, entries=Entries}) ->
    try
        prepared = ets:lookup_element(Entries, ?heartbeat, #record.state),
        ok
    catch _:_ ->
        no_prepared
    end;

decision_hb_pre(_, ClockTime, #state{status=?leader, entries=Entries}) ->
    try
        [[prepared, RedTs]] = ets:match(Entries,
                                        #record{id=?heartbeat, state='$1', red='$2', _='_'}),

        case ClockTime >= RedTs of
            true -> ok;
            false -> not_ready
        end
    catch _:_ ->
        no_prepared
    end.

-spec decision_hb(ballot(), t()) -> ok | decision_error().
decision_hb(Ballot, S=#state{entries=Entries, index=Idx}) ->
    case decision_hb_pre(Ballot, grb_time:timestamp(), S) of
        ok ->
            RedTs = ets:lookup_element(Entries, ?heartbeat, #record.red),
            true = ets:update_element(Entries, ?heartbeat, {#record.state, decided}),
            true = ets:update_element(Idx, RedTs, {#index_entry.state, decided}),
            ok;
        Other ->
            Other
    end.

%%%===================================================================
%%% transaction api
%%%===================================================================

-spec prepare(term(), #{}, #{}, vclock(), t()) -> {red_vote(), ballot(), vclock()}
                                                | {already_decided, red_vote(), vclock()}.

prepare(TxId, RS, WS, SnapshotVC, #state{status=?leader, ballot=Ballot,
                                         entries=Entries, index=Idx}) ->
    Id = {tx, TxId},
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
            RedTs = grb_time:timestamp(),
            {Decision, VC0} = check_transaction(RS, WS, SnapshotVC),
            PrepareVC = grb_vclock:set_time(?RED_REPLICA, RedTs, VC0),

            true = ets:insert(Entries, #record{id=Id, writeset=WS, red=RedTs, clock=PrepareVC,
                                               state=prepared, vote=Decision}),

            true = ets:insert(Idx, #index_entry{red=RedTs, id=Id, state=prepared, vote=Decision}),

            {Decision, Ballot, PrepareVC}
    end.

-spec accept(ballot(), term(), #{}, red_vote(), vclock(), t()) -> ok | bad_ballot.
accept(InBallot, _, _, _, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept(Ballot, TxId, WS, Vote, PrepareVC, #state{entries=Entries, index=Idx, ballot=Ballot}) ->
    Id = {tx, TxId},
    RedTs = grb_vclock:get_time(?RED_REPLICA, PrepareVC),
    true = ets:insert(Entries, #record{id=Id, writeset=WS, red=RedTs, clock=PrepareVC,
                                       state=prepared, vote=Vote}),

    true = ets:insert(Idx, #index_entry{red=RedTs, id=Id, state=prepared, vote=Vote}),

    ok.

-spec decision_pre(ballot(), record_id(), grb_time:ts(), grb_time:ts(), t()) -> ok | decision_error().
decision_pre(InBallot, _, _, _, #state{ballot=Ballot}) when InBallot > Ballot ->
    bad_ballot;

decision_pre(_, Id, _, _, #state{status=?follower, entries=Entries}) ->
    try
        prepared = ets:lookup_element(Entries, Id, #record.state),
        ok
    catch _:_ ->
        no_prepared
    end;

decision_pre(_, Id, CommitRed, ClockTime, #state{status=?leader, entries=Entries}) ->
    try
        prepared = ets:lookup_element(Entries, Id, #record.state),
        case ClockTime >= CommitRed of
            true -> ok;
            false -> not_ready
        end
    catch _:_ ->
        no_prepared
    end.

-spec decision(ballot(), term(), red_vote(), vclock(), t()) -> ok | decision_error().
decision(Ballot, TxId, Vote, CommitVC, S=#state{entries=Entries, index=Idx}) ->
    Id = {tx, TxId},
    RedTs = grb_vclock:get_time(?RED_REPLICA, CommitVC),
    case decision_pre(Ballot, Id, RedTs, grb_time:timestamp(), S) of
        ok ->
            OldTs = ets:lookup_element(Entries, Id, #record.red),
            true = ets:delete(Idx, OldTs),

            true = ets:update_element(Entries, Id, [{#record.red, RedTs},
                                                    {#record.clock, CommitVC},
                                                    {#record.state, decided},
                                                    {#record.vote, Vote}]),

            true = ets:insert(Idx, #index_entry{red=RedTs, id=Id, state=decided, vote=Vote}),
            ok;

        Other -> Other
    end.


%% todo(borja, red)
-spec check_transaction(#{}, #{}, vclock()) -> {red_vote(), vclock()}.
check_transaction(_, _, VC) -> {ok, VC}.

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

        {Ballot, Ts} = grb_paxos_state:prepare_hb(L),
        ok = grb_paxos_state:accept_hb(Ballot, Ts, F),

        ok = grb_paxos_state:decision_hb(Ballot, L),
        ok = grb_paxos_state:decision_hb(Ballot, F),

        {heartbeat, LR} = grb_paxos_state:get_next_ready(0, L),
        ?assertMatch({heartbeat, LR}, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(LR, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(LR, F))
    end).

grb_paxos_state_transaction_test() ->
    with_states([grb_paxos_state:leader(),
                 grb_paxos_state:follower()], fun([L, F]) ->

        TxId = tx_1, RS = #{}, WS = #{}, PVC = #{},

        Res={ok, Ballot, CVC} = grb_paxos_state:prepare(TxId, RS, WS, PVC, L),
        %% preparing again should result in the same result
        ?assertEqual(Res, grb_paxos_state:prepare(TxId, RS, WS, PVC, L)),

        ok = grb_paxos_state:accept(Ballot, TxId, WS, ok, CVC, F),

        ok = grb_paxos_state:decision(Ballot, TxId, ok, CVC, L),
        ok = grb_paxos_state:decision(Ballot, TxId, ok, CVC, F),

        %% preparing after decision should return early
        ?assertEqual({already_decided, ok, CVC} , grb_paxos_state:prepare(TxId, RS, WS, PVC, L)),

        Expected = grb_vclock:get_time(?RED_REPLICA, CVC),
        Ready={CommitTime, WS, Clock} = grb_paxos_state:get_next_ready(0, L),
        ?assertEqual(Ready, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual(Expected, CommitTime),
        ?assertEqual(CVC, Clock),

        ?assertEqual(false, grb_paxos_state:get_next_ready(CommitTime, L)),
        ?assertEqual(false, grb_paxos_state:get_next_ready(CommitTime, F))
    end).

grb_paxos_state_get_next_ready_test() ->
    VC = fun(I) -> #{?RED_REPLICA => I} end,
    T = fun(C) -> grb_vclock:get_time(?RED_REPLICA, C) end,

    %% simple example of a tx being blocked by a previous one
    with_states(grb_paxos_state:follower(), fun(F) ->
        [V0, V1, V2] = [ VC(1), VC(5), VC(7)],
        ok = grb_paxos_state:accept(0, tx_0, #{}, ok, V0, F),
        ok = grb_paxos_state:accept(0, tx_1, #{}, ok, V1, F),
        ok = grb_paxos_state:accept(0, tx_2, #{}, ok, V2, F),

        ok = grb_paxos_state:decision(0, tx_1, ok, V1, F),
        ok = grb_paxos_state:decision(0, tx_2, ok, V2, F),

        %% false, still have to decide tx_0, and it was prepared with a lower time
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F)),
        %% now decide tx_0, marking tx_1 ready for delivery
        ok = grb_paxos_state:decision(0, tx_0, ok, V0, F),

        ?assertEqual({T(V0), #{}, V0}, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual({T(V1), #{}, V1}, grb_paxos_state:get_next_ready(T(V0), F)),
        ?assertEqual({T(V2), #{}, V2}, grb_paxos_state:get_next_ready(T(V1), F))
    end),

    %% simple example of a tx being blocked by a heartbeat
    with_states(grb_paxos_state:follower(), fun(F) ->
        [T0, V1] = [ 1, VC(5)],

        ok = grb_paxos_state:accept_hb(0, T0, F),
        ok = grb_paxos_state:accept(0, tx_1, #{}, ok, V1, F),

        ok = grb_paxos_state:decision(0, tx_1, ok, V1, F),

        %% false, still have to decide the heartbeat
        ?assertEqual(false, grb_paxos_state:get_next_ready(0, F)),
        %% now decide the heartbeat, marking tx_1 ready for delivery
        ok = grb_paxos_state:decision_hb(0, F),

        ?assertEqual({heartbeat, T0}, grb_paxos_state:get_next_ready(0, F)),
        ?assertEqual({T(V1), #{}, V1}, grb_paxos_state:get_next_ready(T0, F))
    end),

    %% more complex with transactions moving places
    with_states(grb_paxos_state:follower(), fun(F) ->
        [V0, V1, V2, V3, V4] = [ VC(1), VC(5), VC(7), VC(10), VC(15)],

        ok = grb_paxos_state:accept(0, tx_0, #{}, {abort, ignore}, V0, F),
        ok = grb_paxos_state:accept(0, tx_1, #{}, ok, V1, F),
        ok = grb_paxos_state:accept(0, tx_2, #{}, ok, V2, F),
        ok = grb_paxos_state:accept(0, tx_3, #{}, ok, V3, F),

        ok = grb_paxos_state:decision(0, tx_1, ok, V1, F),

        %% we can skip tx_0, since it was aborted, no need to wait for decision (because decision will be abort)
        ?assertEqual({T(V1), #{}, V1}, grb_paxos_state:get_next_ready(0, F)),

        %% now we decide tx_2 with a higher time, placing it the last on the queue
        ok = grb_paxos_state:decision(0, tx_2, ok, V4, F),

        %% and decide tx_3, making it ready for delivery
        ok = grb_paxos_state:decision(0, tx_3, ok, V3, F),

        ?assertEqual({T(V3), #{}, V3}, grb_paxos_state:get_next_ready(T(V1), F)),
        ?assertEqual({T(V4), #{}, V4}, grb_paxos_state:get_next_ready(T(V3), F))
    end).

grb_paxos_state_hb_clash_test() ->
    with_states(grb_paxos_state:leader(), fun(L) ->
        {B, Ts0} = grb_paxos_state:prepare_hb(L),
        grb_paxos_state:decision_hb(B, L),

        ?assertEqual({heartbeat, Ts0}, grb_paxos_state:get_next_ready(0, L)),

        timer:sleep(500),

        {_, Ts1} = grb_paxos_state:prepare_hb(L),
        grb_paxos_state:decision_hb(B, L),

        ?assertEqual({heartbeat, Ts0}, grb_paxos_state:get_next_ready(0, L)),
        ?assertEqual({heartbeat, Ts1}, grb_paxos_state:get_next_ready(Ts0, L))
    end).

-endif.
