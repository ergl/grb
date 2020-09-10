-module(grb_red_timer).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

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
    acc = undefined :: {ballot(), non_neg_integer()} | undefined,

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
    State = #state{partition=Partition,
                   replica=ReplicaId,
                   quorum_size=length(grb_dc_manager:all_replicas()),
                   interval=Interval,
                   timer=erlang:send_after(Interval, self(), ?red_hb)},
    {ok, State}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({accept_ack, Ballot}, State=#state{acc=undefined}) ->
    {noreply, update_ack_state(Ballot, 1, State)};

handle_cast({accept_ack, Ballot}, State=#state{acc={Ballot, N}}) ->
    {noreply, update_ack_state(Ballot, N + 1, State)};

handle_cast({accept_ack, BadBallot}, S0) ->
    %% drop any other ACCEPT_ACK messages, we need only a quorum
    ?LOG_WARNING("Heartbeat ACCEPT_ACK with bad ballot ~b at ~p", [BadBallot, S0#state.partition]),
    {noreply, S0};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(?red_hb, S=#state{partition=Partition, acc=undefined, timer=Timer}) ->

    erlang:cancel_timer(Timer),
    ok = grb_paxos_vnode:prepare_heartbeat(Partition),
    {noreply, S#state{timer=undefined}};

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.

-spec update_ack_state(ballot(), non_neg_integer(), #state{}) -> #state{}.
update_ack_state(Ballot, Acked, S0=#state{quorum_size=QSize}) ->
    case Acked >=  QSize of
        false ->
            %% still haven't received quorum, wait
            S0#state{acc={Ballot, Acked}};

        true ->
            %% quorum has been reached, broadcast decision and restart with another heartbeat
            #state{partition=P, replica=LocalId, interval=Interval} = S0,
            ok = grb_paxos_vnode:broadcast_hb_decision(P, LocalId, Ballot),
            S0#state{acc=undefined, timer=erlang:send_after(Interval, self(), ?red_hb)}
    end.
