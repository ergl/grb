%% -------------------------------------------------------------------
%% This module allows multiple readers on the ETS tables of a grb_main_vnode
%% -------------------------------------------------------------------
-module(grb_partition_replica).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/2]).

%% supervision tree
-export([start_link/2]).

%% protocol api
-export([async_key_vsn/4,
         decide_blue/3]).

%% replica management API
-export([start_replicas/2,
         stop_replicas/2,
         replica_ready/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {
    %% Name of this read replica
    self :: atom(),
    %% Partition that this server is replicating
    partition :: partition_id(),
    replica_id :: replica_id(),

    known_barrier_wait_ms :: non_neg_integer(),

    %% Read replica of the opLog ETS table
    oplog_replica :: atom()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Replica management API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start a replica responsible for serving reads to this partion
%%
%%      To allow concurrency, multiple replicas are started. The `Id`
%%      parameter helps to distinguish them.
%%
%%      Since they replicate ETS tables stored in vnodes, they have
%%      to be started in the same physical node.
%%
%%
%%      This function is called from the supervisor dynamically
%%
-spec start_link(Partition :: partition_id(),
                 Id :: non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.

start_link(Partition, Id) ->
    Name = {local, generate_replica_name(Partition, Id)},
    gen_server:start_link(Name, ?MODULE, [Partition, Id], []).

%% @doc Start `Count` read replicas for the given partition
-spec start_replicas(partition_id(), non_neg_integer()) -> ok.
start_replicas(Partition, Count) ->
    start_replicas_internal(Partition, Count).

%% @doc Stop `Count` read replicas for the given partition
-spec stop_replicas(partition_id(), non_neg_integer()) -> ok.
stop_replicas(Partition, Count) ->
    stop_replicas_internal(Partition, Count).

%% @doc Check if all the read replicas at this node and partitions are ready
-spec replica_ready(partition_id(), non_neg_integer()) -> boolean().
replica_ready(_Partition, 0) ->
    true;

replica_ready(Partition, N) ->
    try
        case gen_server:call(generate_replica_name(Partition, N), ready) of
            ready ->
                replica_ready(Partition, N - 1);
            _ ->
                false
        end
    catch _:_ -> false end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec async_key_vsn(grb_promise:t(), partition_id(), key(), vclock()) -> ok.
async_key_vsn(Promise, Partition, Key, VC) ->
    Target = random_replica(Partition),
    gen_server:cast(Target, {key_vsn, Promise, Key, VC}).

-spec decide_blue(partition_id(), _, vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    Target = random_replica(Partition),
    gen_server:cast(Target, {decide_blue, TxId, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    Self = generate_replica_name(Partition, Id),
    OpLog = grb_main_vnode:op_log_table(Partition),
    {ok, WaitMs} = application:get_env(grb, partition_ready_wait_ms),
    {ok, #state{self=Self,
                partition=Partition,
                replica_id=grb_dc_manager:replica_id(),
                known_barrier_wait_ms=WaitMs,
                oplog_replica = OpLog}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast({key_vsn, Promise, Key, VC}, State) ->
    ok = key_vsn_wait(Promise, Key, VC, State),
    {noreply, State};

handle_cast({decide_blue, TxId, VC}, State=#state{replica_id=ReplicaId, known_barrier_wait_ms=WaitMs}) ->
    ok = decide_blue_internal(State#state.partition, WaitMs, ReplicaId, TxId, VC),
    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({retry_key_vsn_wait, Promise, Key, VC}, State) ->
    ok = key_vsn_wait(Promise, Key, VC, State),
    {noreply, State};

handle_info({retry_decide, TxId, VC}, State=#state{replica_id=ReplicaId, known_barrier_wait_ms=WaitMs}) ->
    ok = decide_blue_internal(State#state.partition, WaitMs, ReplicaId, TxId, VC),
    {noreply, State};

handle_info(Info, State) ->
    ?LOG_INFO("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec key_vsn_wait(Promise :: grb_promise:t(),
                   Key :: key(),
                   SnapshotVC :: vclock(),
                   State :: #state{}) -> ok.

key_vsn_wait(Promise, Key, SnapshotVC, #state{partition=Partition,
                                              replica_id=ReplicaId,
                                              oplog_replica=OpLogReplica,
                                              known_barrier_wait_ms=WaitMs}) ->

    case grb_propagation_vnode:partition_ready(Partition, ReplicaId, SnapshotVC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_key_vsn_wait, Promise, Key, SnapshotVC}),
            ok;
        ready ->
            grb_promise:resolve(grb_main_vnode:get_key_version_with_table(OpLogReplica, Key, SnapshotVC),
                                Promise)
    end.

-spec decide_blue_internal(partition_id(), non_neg_integer(), replica_id(), _, vclock()) -> ok.
decide_blue_internal(Partition, WaitMs, ReplicaId, TxId, VC) ->
    case grb_main_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_decide, TxId, VC}),
            ok;
        ready ->
            grb_main_vnode:decide_blue(Partition, TxId, VC)
    end.

-spec generate_replica_name(partition_id(), non_neg_integer()) -> atom().
generate_replica_name(Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    binary_to_atom(<<BinPart/binary, "_", BinId/binary>>, latin1).

-spec random_replica(partition_id()) -> atom().
random_replica(Partition) ->
    generate_replica_name(Partition, rand:uniform(?READ_CONCURRENCY)).

-spec start_replicas_internal(partition_id(), non_neg_integer()) -> ok.
start_replicas_internal(_Partition, 0) ->
    ok;

start_replicas_internal(Partition, N) ->
    case grb_partition_replica_sup:start_replica(Partition, N) of
        {ok, _} ->
            start_replicas_internal(Partition, N - 1);
        {error, {already_started, _}} ->
            start_replicas_internal(Partition, N - 1);
        _Other ->
            ?LOG_ERROR("Unable to start pvc read replica for ~p, will skip", [Partition]),
            try
                ok = gen_server:call(generate_replica_name(Partition, N), shutdown)
            catch _:_ ->
                ok
            end,
            start_replicas_internal(Partition, N - 1)
    end.

-spec stop_replicas_internal(partition_id(), non_neg_integer()) -> ok.
stop_replicas_internal(_Partition, 0) ->
    ok;

stop_replicas_internal(Partition, N) ->
    try
        ok = gen_server:call(generate_replica_name(Partition, N), shutdown)
    catch _:_ ->
        ok
    end,
    stop_replicas_internal(Partition, N - 1).
