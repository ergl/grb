-module(grb_tcp_server).
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
handle_request('StartReq', #{client_vc := CVC, partition := Partition}, Context, State) ->
    reply_to_client(grb:start_transaction(Partition, CVC), Context, State);

handle_request('OpRequest', Args = #{operation := Operation}, Context, State) ->
     #{
        partition := Partition,
        transaction_id := TxId,
        key := Key,
        read_again := ReadAgain,
        snapshot_vc := SnapshotVC
    } = Args,
    ok = grb:update(Partition, TxId, Key, Operation),
    try_partition_wait(Partition, TxId, ReadAgain, SnapshotVC, Context, State);

handle_request('OpRequest', Args = #{read_operation := ReadOperation}, Context, State) ->
     #{
        partition := Partition,
        transaction_id := TxId,
        key := Key,
        read_again := ReadAgain,
        snapshot_vc := SnapshotVC
    } = Args,
    try_read_operation(Partition, TxId, Key, ReadOperation, ReadAgain, SnapshotVC, Context, State);

handle_request('OpRequest', Args, Context, State) ->
    #{
        partition := Partition,
        transaction_id := TxId,
        key := Key,
        type := Type,
        read_again := ReadAgain,
        snapshot_vc := SnapshotVC
    } = Args,
    try_read(Partition, TxId, Key, Type, ReadAgain, SnapshotVC, Context, State);

handle_request('OpRequestPartition', Args = #{reads := Reads}, Context, _State) ->
    #{
        partition := Partition,
        transaction_id := TxId,
        snapshot_vc := SVC,
        read_again := ReadAgain
    } = Args,
    try_multikey_read(grb_promise:new(self(), Context), Partition, TxId, SVC, ReadAgain, Reads);

handle_request('OpRequestPartition', _Args = #{read_ops := _ReadOps}, Context, State) ->
    %% todo(borja, crdt): Implement OpRequestPartition
    reply_to_client(#{}, Context, State);

handle_request('OpRequestPartition', Args = #{operations := Ops}, Context, _State) ->
    #{
        partition := Partition,
        transaction_id := TxId,
        snapshot_vc := SVC,
        read_again := ReadAgain
    } = Args,
    try_multikey_update(grb_promise:new(self(), Context), Partition, TxId, SVC, ReadAgain, Ops);

handle_request('OpSend', Args, Context, State) ->
    #{partition := Partition, transaction_id := TxId, key := Key, operation := Op} = Args,
    reply_to_client(grb:update(Partition, TxId, Key, Op), Context, State);

handle_request('PrepareBlueNode', Args, Context, State) ->
    #{transaction_id := TxId, snapshot_vc := VC, partitions := Partitions} = Args,
    Votes = [ {ok, P, grb:prepare_blue(P, TxId, VC)} || P <- Partitions],
    reply_to_client(Votes, Context, State);

handle_request('DecideBlueNode', Args, _Context, _State) ->
    #{transaction_id := TxId, partitions := Ps, commit_vc := CVC} = Args,
    _ = [grb:decide_blue(P, TxId, CVC) || P <- Ps],
    ok;

handle_request('CommitRed', Args, Context, _State) ->
    #{
        partition := TargetP,
        transaction_id := TxId,
        snapshot_vc := VC,
        transaction_label := Label,
        prepares := Prepares
    } = Args,
    grb:commit_red(grb_promise:new(self(), Context), TargetP, TxId, Label, VC, Prepares);

handle_request('UniformBarrier', #{client_vc := CVC, partition := Partition}, Context, _State) ->
    grb:uniform_barrier(grb_promise:new(self(), Context), Partition, CVC);

handle_request('ConnectRequest', _, Context, State) ->
    reply_to_client(grb:connect(), Context, State);

handle_request('PutConflictRelations', #{payload := Conflicts}, Context, State) ->
    reply_to_client(grb:put_conflicts(Conflicts), Context, State);

handle_request('PutDirect', #{partition := Partition, payload := WS}, Context, State) ->
    reply_to_client(grb:put_direct(Partition, WS), Context, State);

handle_request('Preload', #{payload := Properties}, Context, _State) ->
    grb_load_utils:preload_micro(grb_promise:new(self(), Context), Properties);

handle_request('RubisPreload', #{payload := Properties}, Context, _State) ->
    grb_load_utils:preload_rubis(grb_promise:new(self(), Context), Properties).

-spec try_read(partition_id(), term(), key(), crdt(), boolean(), vclock(), proto_context(), state()) -> ok.
try_read(Partition, TxId, Key, Type, true, SnapshotVC, Context, State) ->
    reply_to_client(grb:key_snapshot_bypass(Partition, TxId, Key, Type, SnapshotVC), Context, State);

try_read(Partition, TxId, Key, Type, false, SnapshotVC, Context, State) ->
    case grb:partition_ready(Partition, SnapshotVC) of
        true ->
            reply_to_client(grb:key_snapshot_bypass(Partition, TxId, Key, Type, SnapshotVC), Context, State);
        false ->
            grb:key_snapshot(grb_promise:new(self(), Context), Partition, TxId, Key, Type, SnapshotVC)
    end.

-spec try_partition_wait(partition_id(), term(), boolean(), vclock(), proto_context(), state()) -> ok.
try_partition_wait(_Partition, _TxId, true, _SnapshotVC, Context, State) ->
    reply_to_client({ok, <<>>}, Context, State);

try_partition_wait(Partition, TxId, false, SnapshotVC, Context, State) ->
    case grb:partition_ready(Partition, SnapshotVC) of
        true ->
            reply_to_client({ok, <<>>}, Context, State);
        false ->
            grb:partition_wait(grb_promise:new(self(), Context), Partition, TxId, SnapshotVC)
    end.

-spec try_read_operation(partition_id(), term(), key(), operation(), boolean(), vclock(), proto_context(), state()) -> ok.
try_read_operation(Partition, TxId, Key, ReadOp, true, SnapshotVC, Context, State) ->
    reply_to_client(
        grb:read_operation_bypass(Partition, TxId, Key, grb_crdt:op_type(ReadOp), ReadOp, SnapshotVC),
        Context,
        State
    );

try_read_operation(Partition, TxId, Key, ReadOp, false, SnapshotVC, Context, State) ->
    Type = grb_crdt:op_type(ReadOp),
    case grb:partition_ready(Partition, SnapshotVC) of
        true ->
            reply_to_client(
                grb:read_operation_bypass(Partition, TxId, Key, Type, ReadOp, SnapshotVC),
                Context,
                State
            );
        false ->
            grb:read_operation(grb_promise:new(self(), Context), Partition, TxId, Key, Type, ReadOp, SnapshotVC)
    end.

-spec try_multikey_read(grb_promise:t(), partition_id(), term(), vclock(), boolean(), [{key(), crdt()}]) -> ok.
try_multikey_read(Promise, Partition, TxId, SVC, true, KeyTypes) ->
    grb:multikey_snapshot_bypass(Promise, Partition, TxId, SVC, KeyTypes);

try_multikey_read(Promise, Partition, TxId, SVC, false, KeyTypes) ->
    case grb:partition_ready(Partition, SVC) of
        true ->
            grb:multikey_snapshot_bypass(Promise, Partition, TxId, SVC, KeyTypes);
        false ->
            grb:multikey_snapshot(Promise, Partition, TxId, SVC, KeyTypes)
    end.

-spec try_multikey_update(grb_promise:t(), partition_id(), term(), vclock(), boolean(), [{key(), operation()}]) -> ok.
try_multikey_update(Promise, Partition, TxId, SVC, true, KeyTypes) ->
    grb:multikey_update_bypass(Promise, Partition, TxId, SVC, KeyTypes);

try_multikey_update(Promise, Partition, TxId, SVC, false, KeyTypes) ->
    case grb:partition_ready(Partition, SVC) of
        true ->
            grb:multikey_update_bypass(Promise, Partition, TxId, SVC, KeyTypes);
        false ->
            grb:multikey_update(Promise, Partition, TxId, SVC, KeyTypes)
    end.
