-module(grb_red_timer).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% fixme(borja, red): Look what's happening with red timer, if in doubt, go through the normal protocol
%% use a unique tx key each time, like {heartbeat, ts()} or a special key that the protocol won't check

%% Start/Util
-export([start/2]).

%% Protocol
-export([handle_accept_ack/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(red_hb, red_heartbeat).

-record(state, {
    partition :: partition_id(),
    replica :: replica_id(),
    quorum_size :: non_neg_integer(),

    quorum_ack :: pos_integer(),
    acc_ballot = undefined :: ballot() | undefined,

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

-spec handle_accept_ack(partition_id(), ballot()) -> ok.
handle_accept_ack(Partition, Ballot) ->
    gen_server:cast(generate_name(Partition), {accept_ack, Ballot}).

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

handle_cast({accept_ack, InBallot}, S0=#state{acc_ballot=Ballot0, quorum_ack=ToAck}) ->
    %% todo(borja, red): handle bad ballot?
    {ok, Ballot} = check_ballot(InBallot, Ballot0),
    S = case ToAck of
        N when N > 1 ->
            S0#state{acc_ballot=Ballot, quorum_ack=ToAck - 1};
        1 ->
            #state{partition=Partition, replica=LocalId, interval=Int} = S0,
            ok = grb_paxos_vnode:broadcast_hb_decision(Partition, LocalId, Ballot),
            S0#state{acc_ballot=undefined, timer=erlang:send_after(Int, self(), ?red_hb)}
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(?red_hb, S=#state{partition=Partition, acc_ballot=undefined, quorum_size=QSize, timer=Timer}) ->
    erlang:cancel_timer(Timer),
    ok = grb_paxos_vnode:prepare_heartbeat(Partition),
    {noreply, S#state{timer=undefined, quorum_ack=QSize}};

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.

-spec check_ballot(ballot(), ballot() | undefined) -> {ok, ballot()} | error.
check_ballot(Ballot, undefined) -> {ok, Ballot};
check_ballot(InBallot, Ballot) when InBallot =:= Ballot -> {ok, Ballot};
check_ballot(_, _) -> error.
