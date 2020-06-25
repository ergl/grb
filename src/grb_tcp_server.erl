
-module(grb_tcp_server).

-behaviour(gen_server).
-behavior(ranch_protocol).
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

-define(TCP_NAME, tcp_server).
-define(TCP_PORT, 7878).
-define(TCP_ACC_POOL, (1 * erlang:system_info(schedulers_online))).
-record(state, {
    socket :: inet:socket(),
    transport :: module(),
    %% The lenght (in bits) of the message identifier
    %% Identifiers are supposed to be opaque, and are ignored by the server,
    %% and simply forwarded back to the client
    id_len :: non_neg_integer()
}).

start_server() ->
    DefaultPort = application:get_env(grb, tcp_port, ?TCP_PORT),
    {ok, _}  = ranch:start_listener(?TCP_NAME,
                                    ranch_tcp,
                                    [{port, DefaultPort},
                                     {num_acceptors, ?TCP_ACC_POOL},
                                     {max_connections, infinity}],
                                    grb_tcp_server,
                                    []),
    Port = ranch:get_port(?TCP_NAME),
    ?LOG_INFO("~p server started on port ~p", [?MODULE, Port]),
    ok.

%% Ranch workaround for gen_server
%% Socket is deprecated, will be removed
start_link(Ref, _Sock, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, 4}]),
    IdLen = application:get_env(grb, tcp_id_len_bits, 16),
    State = #state{socket=Socket, transport=Transport, id_len=IdLen},
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

handle_info({tcp, Socket, Data}, State = #state{socket=Socket,
                                                id_len=IdLen,
                                                transport=Transport}) ->
    case Data of
        <<MessageId:IdLen, Request/binary>> ->
            {Module, Type, Msg} = pvc_proto:decode_client_req(Request),
            ?LOG_DEBUG("request id=~b, type=~p, msg=~w", [MessageId, Type, Msg]),
            Promise = grb_promise:new(self(), {MessageId, Module, Type}),
            ok = grb_tcp_handler:process(Promise, Type, Msg);
        _ ->
            ?LOG_WARNING("received unknown data ~p", [Data])
    end,
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

handle_info({'$grb_promise_resolve', Result, {Id, Mod, Type}}, S=#state{socket=Socket,
                                                                        id_len=IdLen,
                                                                        transport=Transport}) ->
    ?LOG_DEBUG("response id=~b, msg=~w", [Id, Result]),
    Reply = pvc_proto:encode_serv_reply(Mod, Type, Result),
    Transport:send(Socket, <<Id:IdLen, Reply/binary>>),
    {noreply, S};

handle_info(E, S) ->
    ?LOG_WARNING("server got unexpected info with msg ~w", [E]),
    {noreply, S}.
