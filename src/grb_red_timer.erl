-module(grb_red_timer).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Start/Util
-export([start/2,
         generate_name/1]).

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
    acked = 0 :: non_neg_integer(),
    current_ballot = undefined :: ballot() | undefined,

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
                   quorum_size=grb_red_manager:quorum_size(),
                   interval=Interval,
                   timer=erlang:send_after(Interval, self(), ?red_hb)},
    {ok, State}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({accept_ack, Ballot}, S0=#state{current_ballot=Ballot, acked=N, quorum_size=QSize}) ->
    Acked = (N + 1),
    S = case Acked >= QSize of
        false ->
            %% still haven't received quorum, wait
            S0#state{acked=Acked};
        true ->
            %% quorum has been reached, broadcast decision and restart with another heartbeat
            #state{partition=P, replica=LocalId, interval=Interval} = S0,
            ok = grb_paxos_vnode:broadcast_hb_decision(P, LocalId, Ballot),
            S0#state{acked=0, current_ballot=undefined,
                     timer=erlang:send_after(Interval, self(), ?red_hb)}
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(?red_hb, S=#state{partition=Partition,
                              current_ballot=undefined,
                              timer=Timer}) ->

    erlang:cancel_timer(Timer),
    {ok, LeaderBallot} = grb_paxos_vnode:prepare_heartbeat(Partition),
    {noreply, S#state{timer=undefined, acked=1, current_ballot=LeaderBallot}};

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.
