-module(grb_red_timer).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% fixme(borja, red): Look what's happening with red timer, if in doubt, go through the normal protocol
%% use a unique tx key each time, like {heartbeat, ts()} or a special key that the protocol won't check

%% Start/Util
-export([start/2]).

%% Protocol
-export([handle_accept_ack/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(red_hb, red_heartbeat).
-define(no_active_timer, #state{heartbeat_id=undefined,
                                current_ballot=undefined,
                                heartbeat_time=undefined}).

-record(state, {
    partition :: partition_id(),
    replica :: replica_id(),
    quorum_size :: non_neg_integer(),
    next_timer_id = {heartbeat, 0} :: {heartbeat, non_neg_integer()},

    %% Active timer accumulator
    quorum_ack :: pos_integer(),
    current_ballot= undefined :: ballot() | undefined,
    heartbeat_id = undefined :: {heartbeat, non_neg_integer()} | undefined,
    heartbeat_time = undefined :: grb_time:ts() | undefined,

    interval :: non_neg_integer(),
    timer :: reference() | undefined
}).

-spec start(replica_id(), partition_id()) -> {ok, pid()}.
start(ReplicaId, Partition) ->
    gen_server:start({local, generate_name(Partition)}, ?MODULE, [ReplicaId, Partition], []).

-spec generate_name(partition_id()) -> atom().
generate_name(Partition) ->
    BinPart = integer_to_binary(Partition),
    Name = << <<"grb_red_timer_">>/binary, BinPart/binary>>,
    grb_dc_utils:safe_bin_to_atom(Name).

-spec handle_accept_ack(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
handle_accept_ack(Partition, Ballot, Id, Ts) ->
    gen_server:cast(generate_name(Partition), {accept_ack, Ballot, Id, Ts}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ReplicaId, Partition]) ->
    {ok, Interval} = application:get_env(grb, red_heartbeat_interval),
    QuorumSize = length(grb_dc_manager:all_replicas()),
    State = #state{partition=Partition,
                   replica=ReplicaId,
                   quorum_size=QuorumSize,
                   quorum_ack=QuorumSize,
                   interval=Interval,
                   timer=erlang:send_after(Interval, self(), ?red_hb)},
    {ok, State}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({accept_ack, InBallot, Id, InTimestamp}, S0=#state{replica=LocalId,
                                                               partition=Partition,
                                                               quorum_ack=ToAck,
                                                               heartbeat_id=Id,
                                                               current_ballot=Ballot0,
                                                               heartbeat_time=Timestamp0}) ->
    %% todo(borja, red): handle bad ballot / timestamp?
    {ok, Ballot} = check_ballot(InBallot, Ballot0),
    {ok, Timestamp} = check_timestamp(InTimestamp, Timestamp0),
    S = case ToAck of
        N when N > 1 ->
            S0#state{current_ballot=Ballot, heartbeat_time=Timestamp, quorum_ack=ToAck - 1};
        1 ->
            ok = grb_paxos_vnode:broadcast_hb_decision(Partition, LocalId, Ballot, Id, Timestamp),
            rearm_heartbeat_timer(S0)
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(?red_hb, State=?no_active_timer) ->
    #state{timer=Timer, partition=Partition, quorum_size=QSize, next_timer_id=Id} = State,
    erlang:cancel_timer(Timer),
    ok = grb_paxos_vnode:prepare_heartbeat(Partition, Id),
    {noreply, State#state{timer=undefined, quorum_ack=QSize,
                          heartbeat_id=Id, next_timer_id=next_heartbeat_id(Id)}};

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.

-spec next_heartbeat_id({heartbeat, non_neg_integer()}) -> {heartbeat, non_neg_integer()}.
next_heartbeat_id({heartbeat, N}) -> {heartbeat, N + 1}.

-spec check_ballot(ballot(), ballot() | undefined) -> {ok, ballot()} | error.
check_ballot(Ballot, undefined) -> {ok, Ballot};
check_ballot(InBallot, Ballot) when InBallot =:= Ballot -> {ok, Ballot};
check_ballot(_, _) -> error.

-spec check_timestamp(grb_time:ts(), grb_time:ts() | undefined) -> {ok, grb_time:ts()} | error.
check_timestamp(Ts, undefined) -> {ok, Ts};
check_timestamp(Ts, Ts) -> {ok, Ts};
check_timestamp(_, _) -> error.

-spec rearm_heartbeat_timer(#state{}) -> #state{}.
rearm_heartbeat_timer(S=#state{interval=Int}) ->
    S#state{timer=erlang:send_after(Int, self(), ?red_hb),
            heartbeat_id=undefined,
            current_ballot=undefined,
            heartbeat_time=undefined}.
