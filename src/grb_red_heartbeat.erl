-module(grb_red_heartbeat).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([new/2,
         schedule_heartbeat/2,
         handle_accept_ack/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(red_hb, red_heartbeat).

-record(active_hb, {
    timestamp = undefined :: grb_time:ts() | undefined,
    ballot = undefined :: ballot() | undefined,
    to_ack :: pos_integer()
}).
-type active_heartbeats() :: #{red_heartbeat_id() := #active_hb{}}.

-record(state, {
    replica :: replica_id(),
    partition :: partition_id(),
    quorum_size :: non_neg_integer(),
    next_hb_id = {?red_heartbeat_marker, 0} :: red_heartbeat_id(),

    %% Active heartbeats accumulator
    active_heartbeats = #{} :: active_heartbeats()
}).

-spec new(replica_id(), partition_id()) -> {ok, pid()} | ignore | {error, term()}.
new(ReplicaId, Partition) ->
    Res = gen_server:start(?MODULE, [ReplicaId, Partition], []),
    case Res of
        {ok, Pid} ->
            ok = persistent_term:put({?MODULE, Partition}, Pid);
        _ ->
            ok
    end,
    Res.

-spec schedule_heartbeat(pid(), non_neg_integer()) -> reference().
schedule_heartbeat(Pid, TimeoutMs) ->
    erlang:send_after(TimeoutMs, Pid, ?red_hb).

-spec handle_accept_ack(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
handle_accept_ack(Partition, Ballot, Id, Ts) ->
    gen_server:cast(persistent_term:get({?MODULE, Partition}),
                    {accept_ack, Ballot, Id, Ts}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ReplicaId, Partition]) ->
    QuorumSize = grb_red_manager:quorum_size(),
    State = #state{replica=ReplicaId,
                   partition=Partition,
                   quorum_size=QuorumSize},
    {ok, State}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({accept_ack, InBallot, HeartbeatId, InTimestamp}, S0=#state{partition=Partition,
                                                                        active_heartbeats=ActiveHeartbeats}) ->
    S = case maps:get(HeartbeatId, ActiveHeartbeats, undefined) of
        undefined ->
            %% ignore ACCEPT_ACK from past heartbeats
            S0;
        HeartBeatState ->
            ?LOG_DEBUG("received TIMER_ACK(~b, ~p, ~b) from ~p", [InBallot, HeartbeatId, InTimestamp, Partition]),
            S0#state{active_heartbeats=handle_ack(Partition,
                                                  HeartbeatId,
                                                  InBallot,
                                                  InTimestamp,
                                                  HeartBeatState,
                                                  ActiveHeartbeats)}
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(?red_hb, State=#state{partition=Partition,
                                  quorum_size=QuorumSize,
                                  next_hb_id=HeartbeatId,
                                  active_heartbeats=Heartbeats}) ->

    ok = grb_paxos_vnode:prepare_heartbeat(Partition, HeartbeatId),
    {noreply, State#state{next_hb_id=next_heartbeat_id(HeartbeatId),
                          active_heartbeats=Heartbeats#{HeartbeatId => #active_hb{to_ack=QuorumSize}}}};

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.

-spec next_heartbeat_id(red_heartbeat_id()) -> red_heartbeat_id().
next_heartbeat_id({?red_heartbeat_marker, N}) -> {?red_heartbeat_marker, N + 1}.

-spec handle_ack(Partition :: partition_id(),
                 HeartbeatId :: red_heartbeat_id(),
                 InBallot :: ballot(),
                 InTimestamp :: grb_time:ts(),
                 HeartbeatState :: #active_hb{},
                 ActiveHeartbeats :: active_heartbeats()) -> active_heartbeats().

handle_ack(Partition, HeartbeatId, InBallot, InTimestamp, HeartbeatState, ActiveHeartbeats) ->
    #active_hb{timestamp=Timestamp0, ballot=Ballot0, to_ack=ToAck0} = HeartbeatState,
    %% todo(borja, red): handle bad ballot / timestamp?
    {ok, Ballot} = check_ballot(InBallot, Ballot0),
    {ok, Timestamp} = check_timestamp(InTimestamp, Timestamp0),
    case ToAck0 of
        N when N > 1 ->
            ActiveHeartbeats#{HeartbeatId =>
                HeartbeatState#active_hb{ballot=Ballot, timestamp=Timestamp, to_ack=ToAck0 - 1}};
        1 ->
            ?LOG_DEBUG("decided heartbeat ~w with timestamp ~b", [HeartbeatId, Timestamp]),
            %% We're always colocated in the same index node as the leader.
            ok = grb_paxos_vnode:decide_heartbeat(Partition, Ballot, HeartbeatId, Timestamp),
            maps:remove(HeartbeatId, ActiveHeartbeats)
    end.

-spec check_ballot(InBallot :: ballot(),
                   Ballot :: ballot() | undefined) -> {ok, ballot()} | error.
check_ballot(InBallot, undefined) -> {ok, InBallot};
check_ballot(InBallot, Ballot) when InBallot =:= Ballot -> {ok, Ballot};
check_ballot(_, _) -> error.

-spec check_timestamp(grb_time:ts(), grb_time:ts() | undefined) -> {ok, grb_time:ts()} | error.
check_timestamp(Ts, undefined) -> {ok, Ts};
check_timestamp(Ts, Ts) -> {ok, Ts};
check_timestamp(_, _) -> error.
