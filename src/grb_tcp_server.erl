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

-type proto_context() :: {integer(), atom(), atom()}.
-type state() :: #state{}.

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, 4}, {nodelay, true}]),
    IdLen = application:get_env(grb, tcp_id_len_bits, 16),
    State = #state{socket=Socket, transport=Transport, id_len=IdLen},
    gen_server:enter_loop(?MODULE, [], State).

handle_call(E, From, S) ->
    ?LOG_WARNING("~p server got unexpected call with msg ~w from ~w", [?MODULE, E, From]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p server got unexpected cast with msg ~w", [?MODULE, E]),
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
            ok = handle_request(Type, Msg, {MessageId, Module, Type}, State);
        _ ->
            ?LOG_WARNING("~p received unknown data ~p", [?MODULE, Data])
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

handle_info({'$grb_promise_resolve', Result, Context}, State) ->
    ok = reply_to_client(Result, Context, State),
    {noreply, State};

handle_info(E, S) ->
    ?LOG_WARNING("server got unexpected info with msg ~w", [E]),
    {noreply, S}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec reply_to_client(term(), proto_context(), state()) -> ok.
reply_to_client(Result, {Id, Mod, Type}, #state{socket=Socket, id_len=IdLen, transport=Transport}) ->
    ?LOG_DEBUG("response id=~b, msg=~w", [Id, Result]),
    Reply = pvc_proto:encode_serv_reply(Mod, Type, Result),
    Transport:send(Socket, <<Id:IdLen, Reply/binary>>),
    ok.

-spec handle_request(atom(), #{atom() => term()}, proto_context(), state()) -> ok.
handle_request('Load', #{bin_size := Size}, Context, State) ->
    reply_to_client(grb:load(Size), Context, State);

handle_request('UniformBarrier', #{client_vc := CVC, partition := Partition}, Context, _State) ->
    grb:uniform_barrier(grb_promise:new(self(), Context), Partition, CVC);

handle_request('ConnectRequest', _, Context, State) ->
    reply_to_client(grb:connect(), Context, State);

handle_request('StartReq', #{client_vc := CVC, partition := Partition}, Context, State) ->
    reply_to_client(grb:start_transaction(Partition, CVC), Context, State);

handle_request('GetKeyVersion', Args, Context, State) ->
    #{partition := Partition, key := Key, snapshot_vc := SnapshotVC} = Args,
    case grb:try_key_vsn(Partition, Key, SnapshotVC) of
        not_ready ->
            grb:async_key_vsn(grb_promise:new(self(), Context), Partition, Key, SnapshotVC);
        Return ->
            reply_to_client(Return, Context, State)
    end;

handle_request('GetKeyVersionAgain', Args, Contents, State) ->
    #{partition := Partition, key := Key, snapshot_vc := SnapshotVC} = Args,
    reply_to_client(grb:key_vsn_bypass(Partition, Key, SnapshotVC), Contents, State);

handle_request('PrepareBlueNode', Args, Context, State) ->
    #{transaction_id := TxId, snapshot_vc := VC, prepares := Prepares} = Args,
    Votes = [ {ok, P, grb:prepare_blue(P, TxId, WS, VC)} || #{partition := P, writeset := WS} <- Prepares],
    reply_to_client(Votes, Context, State);

handle_request('DecideBlueNode', Args, _Context, _State) ->
    #{transaction_id := TxId, partitions := Ps, commit_vc := CVC} = Args,
    _ = [grb:decide_blue(P, TxId, CVC) || P <- Ps],
    ok;

handle_request('CommitRed', Args, Context, _State) ->
    #{transaction_id := TxId, snapshot_vc := VC, prepares := Prepares, partition := TargetP} = Args,
    grb:commit_red(grb_promise:new(self(), Context), TargetP, TxId, VC, Prepares).
