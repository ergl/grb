-module(grb_red_timer).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).

%% erpc
-export([start_timer/0]).
-ignore_xref([start_link/0,
              start_timer/0]).

%% Protocol
-export([handle_accept_ack/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(red_hb, red_heartbeat).
-define(no_active_timer, #state{current_hb_id=undefined,
                                current_hb_timestamp=undefined}).

-type heartbeat_id() :: {heartbeat, non_neg_integer()}.
-record(state, {
    replica :: replica_id(),
    partitions :: [partition_id()],
    quorum_size :: non_neg_integer(),
    next_hb_id = {heartbeat, 0} :: heartbeat_id(),

    %% Active timer accumulator
    current_hb_id = undefined :: heartbeat_id() | undefined,
    current_hb_timestamp = undefined :: grb_time:ts() | undefined,
    ballots = #{} :: #{partition_id() => ballot()},
    quorums_to_ack = #{} :: #{partition_id() => pos_integer()},

    interval :: non_neg_integer(),
    timer :: reference() | undefined
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start_timer() -> ok.
start_timer() ->
    gen_server:call(?MODULE, start_timer, infinity).

-spec handle_accept_ack(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
handle_accept_ack(Partition, Ballot, Id, Ts) ->
    gen_server:cast(?MODULE, {accept_ack, Partition, Ballot, Id, Ts}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) -> {ok, undefined}.

handle_call(start_timer, _From, undefined) ->
    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Interval} = application:get_env(grb, red_heartbeat_interval),
    QuorumSize = grb_red_manager:quorum_size(),
    Partitions = grb_dc_utils:my_partitions(),
    State = #state{replica=ReplicaId,
                   partitions=Partitions,
                   quorum_size=QuorumSize,
                   interval=Interval,
                   timer=start_heartbeat_timer(Interval)},

    {reply, ok, State};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({accept_ack, From, InBallot, Id, InTimestamp}, S0=#state{replica=LocalId,
                                                                     partitions=Partitions,
                                                                     current_hb_id=Id,
                                                                     current_hb_timestamp=Timestamp0,
                                                                     ballots=Ballots0,
                                                                     quorums_to_ack=Quorums0}) ->

    %% todo(borja, red): handle bad ballot / timestamp?
    {ok, Ballots} = check_ballot(From, InBallot, Ballots0),
    Timestamp = max_timestamp(InTimestamp, Timestamp0),
    ToAck = maps:get(From, Quorums0),
    ?LOG_DEBUG("received TIMER_ACK(~b, ~p, ~b) from ~p, ~b to go", [InBallot, Id, InTimestamp, From, ToAck - 1]),
    Quorums = case ToAck of
        1 -> maps:remove(From, Quorums0);
        _ -> Quorums0#{From => ToAck - 1}
    end,
    S = case map_size(Quorums) of
        N when N > 0 ->
            S0#state{ballots=Ballots, quorums_to_ack=Quorums, current_hb_timestamp=Timestamp};
        0 ->
            ?LOG_DEBUG("decided heartbeat ~w with timestamp ~b", [Id, Timestamp]),
            lists:foreach(fun(P) ->
                Ballot = maps:get(P, Ballots),
                ok = grb_paxos_vnode:broadcast_hb_decision(P, LocalId, Ballot, Id, Timestamp)
            end, Partitions),
            rearm_heartbeat_timer(S0)
    end,
    {noreply, S};

handle_cast({accept_ack, _, _, _}, S) ->
    %% ignore any ACCEPT_ACK from past heartbeats
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(?red_hb, State=?no_active_timer) ->
    #state{timer=Timer, partitions=Partitions, quorum_size=QSize, next_hb_id=Id} = State,
    erlang:cancel_timer(Timer),
    ?LOG_DEBUG("starting heartbeat timer ~p", [Id]),
    FoldFun = fun(P, Acc) ->
        ok = grb_paxos_vnode:prepare_heartbeat(P, Id),
        Acc#{P => QSize}
    end,
    Quorums = lists:foldl(FoldFun, #{}, Partitions),
    {noreply, State#state{timer=undefined, ballots=#{}, quorums_to_ack=Quorums,
                          current_hb_id=Id, next_hb_id=next_heartbeat_id(Id)}};

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.

-spec start_heartbeat_timer(non_neg_integer()) -> undefined | reference().
start_heartbeat_timer(0) -> undefined;
start_heartbeat_timer(Interval) -> erlang:send_after(Interval, self(), ?red_hb).

-spec next_heartbeat_id(heartbeat_id()) -> heartbeat_id().
next_heartbeat_id({heartbeat, N}) -> {heartbeat, N + 1}.

-spec check_ballot(From :: partition_id(),
                   Ballot :: ballot(),
                   Ballots :: #{partition_id() => ballot()}) -> error
                                                              | {ok, #{partition_id() => ballot()}}.
check_ballot(From, Ballot, Ballots) ->
    case maps:get(From, Ballots, undefined) of
        undefined -> {ok, Ballots#{From => Ballot}};
        Ballot -> {ok, Ballots};
        _ -> error
    end.

-spec max_timestamp(grb_time:ts(), grb_time:ts() | undefined) -> grb_time:ts().
max_timestamp(Ts, undefined) -> Ts;
max_timestamp(Ts, AccTs) -> max(Ts, AccTs).

-spec rearm_heartbeat_timer(#state{}) -> #state{}.
rearm_heartbeat_timer(S=#state{interval=Int}) ->
    S#state{timer=erlang:send_after(Int, self(), ?red_hb),
            current_hb_id=undefined,
            current_hb_timestamp=undefined}.
