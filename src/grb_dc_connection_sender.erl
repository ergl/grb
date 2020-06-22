
-module(grb_dc_connection_sender).

-behaviour(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/3]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(state, {
    connected_dc :: replica_id(),
    socket :: inet:socket()
}).

-spec start_link(replica_id(), inet:ip_address(), inet:port_number()) -> {ok, pid()}.
start_link(RemoteID, IP, Port) ->
    Ret = gen_server:start_link(?MODULE, [RemoteID, IP, Port], []),
    case Ret of
        {ok, Pid} ->
            ok = grb_dc_connection_manager:add_replica_connection(RemoteID, Pid),
            {ok, Pid};
        {error, {already_started, ChildPid}} ->
            {ok, ChildPid};
        Err ->
            Err
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([RemoteID, IP, Port]) ->
    case gen_tcp:connect(IP, Port, ?INTER_DC_SOCK_OPTS) of
        {error, Reason} ->
            ?LOG_ERROR("~p ~p failed to start connection with ~p: ~p", [?MODULE, self(), RemoteID, Reason]),
            {stop, Reason};
        {ok, Socket} ->
            {ok, {LocalIP, LocalPort}} = inet:sockname(Socket),
            ?LOG_INFO("~p ~p started connection with ~p on ~p:~p", [?MODULE, self(), RemoteID, LocalIP, LocalPort]),
            {ok, #state{connected_dc=RemoteID,
                        socket=Socket}}
    end.

handle_call(E, _From, S) ->
    ?LOG_WARNING("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket}) ->
    ?LOG_INFO("replication client received ~p", [Data]),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, State) ->
    ?LOG_INFO("replication client received tcp_closed"),
    {stop, normal, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    ?LOG_INFO("replication client received tcp_error ~p", [Reason]),
    {stop, Reason, State};

handle_info(timeout, State) ->
    ?LOG_INFO("replication client received timeout"),
    {stop, normal, State};

%% fixme(borja): remove
handle_info({test, Data}, State=#state{socket=S}) ->
    ?LOG_INFO("Sending test data ~p", [Data]),
    ok = gen_tcp:send(S, Data),
    {noreply, State};

handle_info(E, S) ->
    ?LOG_WARNING("replication client received unexpected info with msg ~w", [E]),
    {noreply, S}.

terminate(_Reason, #state{socket=Socket}) ->
    ok = gen_tcp:close(Socket),
    ok.
