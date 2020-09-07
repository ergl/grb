-module(grb_paxos_state).
-include("grb.hrl").

-define(leader, leader).
-define(follower, follower).
-define(heartbeat, heartbeat_entry).
-type status() :: ?leader | ?follower.

%% The queues contain either a heartbeat (with only its red time),
%% or a transaction, which contains the TxId, Vote and PrepareVC/CommitVC
%% todo(borja, red): Store writeset
-type entry() :: {term(), grb_time:ts()} | {term(), red_vote(), vclock()}.
-type prepared() :: cache(term(), entry()).
-type decided() :: cache(term(), entry()).

%% Reverse index over prepared / decided, with the red entry as key
-type queue_index() :: cache(grb_time:ts(), term()).

-record(state, {
    status :: status(),
    ballot = 0 :: ballot(),
    prepared :: prepared(),
    decided :: decided(),
    prepared_index :: queue_index(),
    decided_index :: queue_index()
}).

-type t() :: #state{}.

-export_type([t/0, entry/0]).

%% constructors
-export([leader/0,
         follower/0,
         delete/1]).

%% ready api
-export([get_next_ready/2]).

%% heartbeat api
-export([prepare_hb/1,
         accept_hb/3,
         decision_hb_pre/2,
         decision_hb/1]).

%% transactional api
-export([prepare/5,
         accept/5,
         decision_pre/5,
         decision/4]).

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
           prepared=ets:new(prepared_red, [set]),
           decided=ets:new(decided_red, [set]),
           prepared_index=ets:new(prepared_red_index, [ordered_set]),
           decided_index=ets:new(prepared_red_index, [ordered_set])}.

-spec delete(t()) -> ok.
delete(#state{prepared=P, prepared_index=PIdx, decided=D, decided_index=DIdx}) ->
  true = ets:delete(P),
  true = ets:delete(PIdx),
  true = ets:delete(D),
  true = ets:delete(DIdx),
  ok.

%%%===================================================================
%%% ready
%%%===================================================================

%% todo(borja, red): handle aborted transactions
-spec get_next_ready(grb_time:ts(), t()) -> false | {heartbeat, grb_time:ts()} | {grb_time:ts(), entry()}.
get_next_ready(LastDelivered, #state{prepared_index=PIdx, decided=Dec, decided_index=DIdx}) ->
    NextCommit = ets:next(DIdx, LastDelivered),
    case NextCommit of
        '$end_of_table' -> false;
        CommitTime ->
            case prepared_between(LastDelivered, CommitTime, PIdx) of
                true -> false;
                false ->
                    [{CommitTime, TxId}] = ets:take(DIdx, CommitTime),
                    case TxId of
                        ?heartbeat ->
                            true = ets:delete(Dec, ?heartbeat),
                            {heartbeat, CommitTime};
                        _ ->
                            {CommitTime, ets:take(Dec, TxId)}
                    end
            end
    end.

-spec prepared_between(grb_time:ts(), grb_time:ts(), cache_id()) -> boolean().
prepared_between(From, To, Table) ->
    0 =/= ets:select_count(Table, [{ {'$1', '_'},
                                     [{'andalso', {'>', '$1', {const, From}},
                                                  {'<', '$1', {const, To}}}],
                                     [true]  }]).

%%%===================================================================
%%% heartbeat api
%%%===================================================================

-spec prepare_hb(t()) -> {ballot(), grb_time:ts()}.
prepare_hb(#state{status=?leader, ballot=Ballot, prepared=Prep, prepared_index=Idx}) ->
    Ts = grb_time:timestamp(),
    true = ets:insert(Prep, {?heartbeat, Ts}),
    true = ets:insert(Idx, {Ts, ?heartbeat}),
    {Ballot, Ts}.

-spec accept_hb(ballot(), grb_time:ts(), vclock()) -> ok | bad_ballot.
accept_hb(InBallot, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept_hb(Ballot, Ts, #state{status=?follower, prepared=Prep, ballot=Ballot, prepared_index=Idx}) ->
    true = ets:insert(Prep, {?heartbeat, Ts}),
    true = ets:insert(Idx, {Ts, ?heartbeat}),
    ok.

-spec decision_hb_pre(ballot(), t()) -> ok | bad_ballot | not_prepared | not_ready.
decision_hb_pre(InBallot, #state{ballot=Ballot}) when InBallot > Ballot -> bad_ballot;

decision_hb_pre(_, #state{status=?follower, prepared=Prep}) ->
    case ets:member(Prep, ?heartbeat) of
        false -> not_prepared;
        true -> ok
    end;

decision_hb_pre(_, #state{status=?leader, prepared=Prep}) ->
    case ets:lookup(Prep, ?heartbeat) of
        [{?heartbeat, Ts}] ->
            case grb_time:timestamp() >= Ts of
                true -> ok;
                false -> not_ready
            end;
        [] -> not_prepared
    end.

-spec decision_hb(t()) -> ok.
decision_hb(#state{prepared=Prep, prepared_index=PIdx, decided=Dec, decided_index=DIdx}) ->
    [{?heartbeat, Ts}] = ets:take(Prep, ?heartbeat),
    true = ets:delete(PIdx, Ts),
    true = ets:insert(Dec, {?heartbeat, Ts}),
    true = ets:insert(DIdx, {Ts, ?heartbeat}),
    ok.

%%%===================================================================
%%% transaction api
%%%===================================================================

-spec prepare(term(), #{}, #{}, vclock(), t()) -> {red_vote(), ballot(), vclock()} | {already_decided, red_vote(), vclock()}.
prepare(TxId, RS, WS, SnapshotVC, #state{status=?leader, ballot=Ballot,
                                         prepared=Prep, prepared_index=Idx, decided=Dec}) ->
    case ets:lookup(Dec, TxId) of
        [{TxId, CommitVote, CommitVC}] ->
            {already_decided, CommitVote, CommitVC};
        [] ->
            case ets:lookup(Prep, TxId) of
                [{TxId, PrepareVote, PrepareVC}] ->
                    {PrepareVote, Ballot, PrepareVC};
                [] ->
                    RedTs = grb_time:timestamp(),
                    {Vote, VC0} = check_transaction(RS, WS, SnapshotVC),
                    VC = grb_vclock:set_time(?RED_REPLICA, RedTs, VC0),
                    true = ets:insert(Prep, {TxId, Vote, VC}),
                    true = ets:insert(Idx, {RedTs, TxId}),
                    {Vote, Ballot, VC}
            end
    end.

-spec accept(ballot(), term(), red_vote(), vclock(), t()) -> ok | bad_ballot.
accept(InBallot, _, _, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept(Ballot, TxId, Vote, PrepareVC, #state{prepared=Prep, prepared_index=Idx, ballot=Ballot}) ->
    true = ets:insert(Prep, {TxId, Vote, PrepareVC}),
    true = ets:insert(Idx, {grb_vclock:get_time(?RED_REPLICA, PrepareVC), TxId}),
    ok.

-spec decision_pre(ballot(), term(), grb_time:ts(), grb_time:ts(), t()) -> ok | bad_ballot | not_prepared | not_ready.
decision_pre(InBallot, _, _, _, #state{ballot=Ballot}) when InBallot > Ballot ->
    bad_ballot;

decision_pre(_, TxId, _, _, #state{status=?follower, prepared=Prep}) ->
    case ets:member(Prep, TxId) of
        false -> not_prepared;
        true -> ok
    end;

decision_pre(_, TxId, CommitRed, ClockTime, #state{status=?leader, prepared=Prep}) ->
    case ets:member(Prep, TxId) of
        false -> not_prepared;
        true ->
            case ClockTime >= CommitRed of
                false -> not_ready;
                true -> ok
            end
    end.

-spec decision(term(), red_vote(), vclock(), t()) -> ok.
decision(TxId, Vote, CommitVC, #state{prepared=Prep, prepared_index=PIdx, decided=Dec, decided_index=DIdx}) ->
    OldVC = ets:lookup_element(Prep, TxId, 3),
    true = ets:delete(Prep, TxId),
    true = ets:delete(PIdx, grb_vclock:get_time(?RED_REPLICA, OldVC)),
    true = ets:insert(Dec, {TxId, Vote, CommitVC}),
    true = ets:insert(DIdx, {grb_vclock:get_time(?RED_REPLICA, CommitVC), TxId}),
    ok.

%% todo(borja, red)
-spec check_transaction(#{}, #{}, vclock()) -> {red_vote(), vclock()}.
check_transaction(_, _, VC) -> {ok, VC}.
