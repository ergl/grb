-module(grb_dc_connection_receiver).

-behaviour(gen_server).
-behavior(ranch_protocol).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% Module API
-export([start_server/0]).

%% ranch_protocol callback
-export([start_link/4]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(SERVICE_NAME, grb_inter_dc).
-define(SERVICE_POOL, (1 * erlang:system_info(schedulers_online))).

-record(state, {
    socket :: inet:socket(),
    transport :: module()
}).

start_server() ->
    {ok, Port} = application:get_env(grb, inter_dc_port),
    {ok, _} = ranch:start_listener(?SERVICE_NAME,
                                   ranch_tcp,
                                   [{port, Port},
                                    {num_acceptors, ?SERVICE_POOL},
                                    {max_connections, infinity}],
                                   ?MODULE,
                                   []),
    ActualPort = ranch:get_port(?SERVICE_NAME),
    ?LOG_INFO("~p server started on port ~p", [?MODULE, ActualPort]),
    ok.

%% Ranch workaround for gen_server
%% Socket is deprecated, will be removed
start_link(Ref, _Sock, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, ?INTER_DC_SOCK_OPTS),
    State = #state{socket=Socket, transport=Transport},
    gen_server:enter_loop(?MODULE, [], State).

handle_call(E, From, S) ->
    ?LOG_WARNING("server got unexpected call with msg ~w from ~w", [E, From]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("server got unexpected cast with msg ~w", [E]),
    {noreply, S}.

terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, P:?PARTITION_BITS/big-unsigned-integer, Msg/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    #inter_dc_message{source_id=SourceReplica, payload=Request} = binary_to_term(Msg),
    ?LOG_DEBUG("Received msg from ~p to ~p: ~p", [SourceReplica, P, Request]),
    ok = handle_request(P, SourceReplica, Request),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp, Socket, Data}, State = #state{transport=Transport}) ->
    ?LOG_WARNING("received unknown data ~p", [Data]),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    ?LOG_INFO("replication server received tcp_closed"),
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    ?LOG_INFO("replication server received tcp_error ~p", [Reason]),
    {stop, Reason, S};

handle_info(timeout, State) ->
    ?LOG_INFO("replication server received timeout"),
    {stop, normal, State};

handle_info(E, S) ->
    ?LOG_WARNING("replication server received unexpected info with msg ~w", [E]),
    {noreply, S}.

-spec handle_request(partition_id(), replica_id(), replica_message()) -> ok.
handle_request(Partition, SourceReplica, #blue_heartbeat{timestamp=Ts}) ->
    grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, Ts);

handle_request(Partition, SourceReplica, #replicate_tx{tx_id=TxId, writeset=WS, commit_vc=VC}) ->
    grb_main_vnode:handle_replicate(Partition, SourceReplica, TxId, WS, VC);

handle_request(Partition, SourceReplica, #update_clocks{known_vc=KnownVC, stable_vc=StableVC}) ->
    grb_propagation_vnode:handle_clock_update(Partition, SourceReplica, KnownVC, StableVC).
