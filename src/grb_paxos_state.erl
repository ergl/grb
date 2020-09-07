-module(grb_paxos_state).
-include("grb.hrl").

-define(leader, leader).
-define(follower, follower).
-define(heartbeat, heartbeat_entry).
-type status() :: ?leader | ?follower.

-type prepared() :: cache_id().
-type decided() :: cache_id().

-record(state, {
    status :: status(),
    ballot = 0 :: ballot(),
    prepared :: prepared(),
    decided :: decided()
}).

-type t() :: #state{}.

-export_type([t/0]).

-export([leader/0,
         follower/0]).

-export([prepare_hb/1,
         accept_hb/3,
         decision_hb_pre/2,
         decision_hb/1]).

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
           decided=ets:new(decided_red, [set])}.

%%%===================================================================
%%% heartbeat api
%%%===================================================================

-spec prepare_hb(t()) -> {ballot(), grb_time:ts()}.
prepare_hb(#state{status=?leader, ballot=Ballot, prepared=Prep}) ->
    Ts = grb_time:timestamp(),
    true = ets:insert(Prep, {?heartbeat, Ts}),
    {Ballot, Ts}.

-spec accept_hb(ballot(), grb_time:ts(), vclock()) -> ok | bad_ballot.
accept_hb(InBallot, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept_hb(Ballot, Ts, #state{status=?follower, prepared=Prep, ballot=Ballot}) ->
    true = ets:insert(Prep, {?heartbeat, Ts}),
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
decision_hb(#state{prepared=Prep, decided=Dec}) ->
    [{?heartbeat, Ts}] = ets:take(Prep, ?heartbeat),
    true = ets:insert(Dec, {?heartbeat, Ts}),
    ok.

%%%===================================================================
%%% transaction api
%%%===================================================================

-spec prepare(term(), #{}, #{}, vclock(), t()) -> {red_vote(), ballot(), vclock()} | {already_decided, red_vote(), vclock()}.
prepare(TxId, RS, WS, SnapshotVC, #state{status=?leader, ballot=Ballot,
                                         prepared=Prep, decided=Dec}) ->
    case ets:lookup(Dec, TxId) of
        [{TxId, CommitVote, CommitVC}] ->
            {already_decided, CommitVote, CommitVC};
        [] ->
            case ets:lookup(Prep, TxId) of
                [{TxId, PrepareVote, PrepareVC}] ->
                    {PrepareVote, Ballot, PrepareVC};
                [] ->
                    {Vote, TxPrepareVC} = check_transaction(RS, WS, SnapshotVC),
                    true = ets:insert(Prep, {TxId, Vote, TxPrepareVC}),
                    {Vote, Ballot}
            end
    end.



-spec accept(ballot(), term(), red_vote(), vclock(), t()) -> ok | bad_ballot.
accept(InBallot, _, _, _, #state{ballot=Ballot}) when InBallot =/= Ballot -> bad_ballot;
accept(Ballot, TxId, Vote, PrepareVC, #state{prepared=Prep, ballot=Ballot}) ->
    true = ets:insert(Prep, {TxId, Vote, PrepareVC}),
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
decision(TxId, Vote, CommitVC, #state{prepared=Prep, decided=Dec}) ->
    true = ets:delete(Prep, TxId),
    true = ets:insert(Dec, {TxId, Vote, CommitVC}),
    ok.

%% todo(borja, red)
-spec check_transaction(#{}, #{}, vclock()) -> {red_vote(), vclock()}.
check_transaction(_, _, VC) -> {ok, VC}.
