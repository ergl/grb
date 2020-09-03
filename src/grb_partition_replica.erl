%% -------------------------------------------------------------------
%% This module allows multiple readers on the ETS tables of a grb_main_vnode
%% -------------------------------------------------------------------
-module(grb_partition_replica).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/4]).

%% supervision tree
-export([start_link/4]).

%% protocol api
-export([async_op/5,
         decide_blue/3]).

%% replica management API
-export([start_replicas/4,
         stop_replicas/2,
         update_default/4,
         replica_ready/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-ignore_xref([start_link/2]).

-record(state, {
    %% Name of this read replica
    self :: atom(),
    %% Partition that this server is replicating
    partition :: partition_id(),
    replica_id :: replica_id(),

    known_barrier_wait_ms :: non_neg_integer(),

    %% Read replica of the opLog ETS table
    oplog_replica :: atom(),
    default_bottom_value :: term(),
    default_bottom_red :: grb_time:ts()
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
                 Id :: non_neg_integer(),
                 Val :: term(),
                 RedTs :: grb_time:ts()) -> {ok, pid()} | ignore | {error, term()}.

start_link(Partition, Id, Val, RedTs) ->
    Name = {local, generate_replica_name(Partition, Id)},
    gen_server:start_link(Name, ?MODULE, [Partition, Id, Val, RedTs], []).

%% @doc Start `Count` read replicas for the given partition
-spec start_replicas(partition_id(), non_neg_integer(), term(), grb_time:ts()) -> ok.
start_replicas(Partition, Count, Val, RedTs) ->
    start_replicas_internal(Partition, Count, Val, RedTs).

%% @doc Stop `Count` read replicas for the given partition
-spec stop_replicas(partition_id(), non_neg_integer()) -> ok.
stop_replicas(Partition, Count) ->
    stop_replicas_internal(Partition, Count).

%% @doc Update the default values at `Count` read replicas
-spec update_default(partition_id(), non_neg_integer(), term(), grb_time:ts()) -> ok.
update_default(_Partition, 0, _, _) ->
    ok;

update_default(Partition, N, Val, RedTs) ->
    ok = gen_server:call(generate_replica_name(Partition, N), {update_default, Val, RedTs}),
    update_default(Partition, N - 1, Val, RedTs).

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

-spec async_op(grb_promise:t(), partition_id(), key(), vclock(), val()) -> ok.
async_op(Promise, Partition, Key, VC, Val) ->
    Target = random_replica(Partition),
    gen_server:cast(Target, {perform_op, Promise, Key, VC, Val}).

-spec decide_blue(partition_id(), _, vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    Target = random_replica(Partition),
    gen_server:cast(Target, {decide_blue, TxId, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id, Val, RedTs]) ->
    Self = generate_replica_name(Partition, Id),
    OpLog = grb_dc_utils:cache_name(Partition, ?OP_LOG_TABLE),
    {ok, OpWait} = application:get_env(grb, op_prepare_wait_ms),
    {ok, #state{self=Self,
                partition=Partition,
                replica_id=grb_dc_manager:replica_id(),
                known_barrier_wait_ms=OpWait,
                oplog_replica = OpLog,
                default_bottom_value=Val,
                default_bottom_red=RedTs}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call({update_default, Val, RedTs}, _From, S) ->
    {reply, ok, S#state{default_bottom_value=Val, default_bottom_red=RedTs}};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast({perform_op, Promise, Key, VC, Val}, State) ->
    ok = perform_op_internal(Promise, Key, VC, Val, State),
    {noreply, State};

handle_cast({decide_blue, TxId, VC}, State=#state{replica_id=ReplicaId, known_barrier_wait_ms=WaitMs}) ->
    ok = decide_blue_internal(State#state.partition, WaitMs, ReplicaId, TxId, VC),
    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({retry_op_wait, Promise, Key, VC, Val}, State) ->
    ok = perform_op_wait(Promise, Key, VC, Val, State),
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

-spec perform_op_internal(Promise :: grb_promise:t(),
                          Key :: key(),
                          SnapshotVC :: vclock(),
                          Val :: val(),
                          State :: #state{}) -> ok.

-ifdef(BASIC_REPLICATION).

perform_op_internal(Promise, Key, SnapshotVC, Val, State=#state{partition=Partition}) ->
    _ = grb_propagation_vnode:merge_remote_stable_vc(Partition, SnapshotVC),
    perform_op_wait(Promise, Key, SnapshotVC, Val, State).

-else.

perform_op_internal(Promise, Key, SnapshotVC, Val, State=#state{partition=Partition}) ->
    _ = grb_propagation_vnode:merge_remote_uniform_vc(Partition, SnapshotVC),
    perform_op_wait(Promise, Key, SnapshotVC, Val, State).

-endif.

-spec perform_op_wait(Promise :: grb_promise:t(),
                      Key :: key(),
                      SnapshotVC :: vclock(),
                      Val :: val(),
                      State :: #state{}) -> ok.

perform_op_wait(Promise, Key, SnapshotVC, Val, S=#state{partition=Partition,
                                                        replica_id=ReplicaId,
                                                        known_barrier_wait_ms=WaitMs}) ->

    case check_known_vc(Partition, ReplicaId, SnapshotVC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_op_wait, Promise, Key, SnapshotVC, Val}),
            ok;
        ready ->
            perform_op_continue(Promise, Key, SnapshotVC, Val, S)
    end.

-spec check_known_vc(partition_id(), replica_id(), vclock()) -> ready | not_ready.
check_known_vc(Partition, ReplicaId, VC) ->
    ClientTime = grb_vclock:get_time(ReplicaId, VC),
    LocalTime = grb_propagation_vnode:known_time(Partition, ReplicaId),
    case LocalTime >= ClientTime of
        true ->
            ready;
        false ->
            %% todo(borja, stat): log miss
            not_ready
    end.

-spec perform_op_continue(grb_promise:t(), key(), vclock(), val(), #state{}) -> ok.
perform_op_continue(Promise, Key, VC, Val, State=#state{default_bottom_value=BottomVal,
                                                        default_bottom_red=BottomRedTs}) ->
    BaseVal = case Val of <<>> -> BottomVal; _ -> Val end,
    case ets:lookup(State#state.oplog_replica, Key) of
        [] ->
            %% todo(borja, warn): Check soundness
            grb_promise:resolve({ok, BaseVal, BottomRedTs}, Promise);
        [{Key, Log}] ->
            %% todo(borja, warn): Totally order log operations
            %% should introduce lamport clock to updates to totally order them
            %% Right now, return the first (highest in the snapshot)
            %% todo(borja, red): Update redTS with dependence vectors
            case grb_version_log:get_first_lower(VC, Log) of
                undefined -> grb_promise:resolve({ok, BaseVal, BottomRedTs}, Promise);
                {_, LastVal, _LastVC} ->
                    ReturnVal = case Val of <<>> -> LastVal; _ -> Val end,
                    grb_promise:resolve({ok, ReturnVal, BottomRedTs}, Promise)
            end
    end.

-spec decide_blue_internal(partition_id(), non_neg_integer(), replica_id(), _, vclock()) -> ok.
decide_blue_internal(Partition, WaitMs, ReplicaId, TxId, VC) ->
    case check_current_clock(ReplicaId, VC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_decide, TxId, VC}),
            ok;
        ready ->
            grb_main_vnode:decide_blue(Partition, TxId, VC)
    end.

-spec check_current_clock(replica_id(), vclock()) -> ready | not_ready.
check_current_clock(ReplicaId, VC) ->
    SelfBlue = grb_vclock:get_time(ReplicaId, VC),
    CurrentTS = grb_time:timestamp(),
    case CurrentTS >= SelfBlue of
        true ->
            ready;
        false ->
            %% todo(borja, stat): log miss
            not_ready
    end.

-spec generate_replica_name(partition_id(), non_neg_integer()) -> atom().
generate_replica_name(Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    binary_to_atom(<<BinPart/binary, "_", BinId/binary>>, latin1).

-spec random_replica(partition_id()) -> atom().
random_replica(Partition) ->
    generate_replica_name(Partition, rand:uniform(?READ_CONCURRENCY)).

-spec start_replicas_internal(partition_id(), non_neg_integer(), term(), grb_time:ts()) -> ok.
start_replicas_internal(_Partition, 0, _, _) ->
    ok;

start_replicas_internal(Partition, N, Val, Clock) ->
    case grb_partition_replica_sup:start_replica(Partition, N, Val, Clock) of
        {ok, _} ->
            start_replicas_internal(Partition, N - 1, Val, Clock);
        {error, {already_started, _}} ->
            start_replicas_internal(Partition, N - 1, Val, Clock);
        _Other ->
            ?LOG_ERROR("Unable to start pvc read replica for ~p, will skip", [Partition]),
            try
                ok = gen_server:call(generate_replica_name(Partition, N), shutdown)
            catch _:_ ->
                ok
            end,
            start_replicas_internal(Partition, N - 1, Val, Clock)
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
