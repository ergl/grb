-module(grb_dc_connection_receiver).

-behaviour(gen_server).
-behavior(ranch_protocol).
-include("grb.hrl").
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

terminate(_, #state{socket=undefined, transport=undefined}) ->  ok;
terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.

handle_info({tcp, Socket, Data}, State = #state{socket=Socket,
                                                transport=Transport}) ->
    ?LOG_DEBUG("~p received ~p", [?MODULE, Data]),
    Transport:send(Socket, Data),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    ?LOG_INFO("server got tcp_error"),
    {stop, Reason, S};

handle_info(timeout, State) ->
    ?LOG_INFO("server got timeout"),
    {stop, normal, State};

handle_info(E, S) ->
    ?LOG_WARNING("server ~p got unexpected info with msg ~w", [?MODULE, E]),
    {noreply, S}.
