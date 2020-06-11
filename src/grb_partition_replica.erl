%% -------------------------------------------------------------------
%% This module allows multiple readers on the ETS tables of a grb_vnode
%% -------------------------------------------------------------------
-module(grb_partition_replica).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% supervision tree
-export([start_link/2]).

%% protocol api
-export([async_op/5,
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

-ignore_xref([start_link/2]).

%% Time (in ms) a partition should wait between retries at checking
%% a partition's most knownVC during reads.
-define(OP_WAIT_MS, 1000).

-record(state, {
    %% Name of this read replica
    self :: atom(),
    %% Partition that this server is replicating
    partition :: partition_id(),

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
%%      (see pvc_read_replica_sup:start_replica/2)
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
    case gen_server:call(generate_replica_name(Partition, N), ready) of
        ready ->
            replica_ready(Partition, N - 1);
        _ ->
            false
    end.

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

init([Partition, Id]) ->
    Self = generate_replica_name(Partition, Id),
    OpLog = grb_vnode:cache_name(Partition, ?OP_LOG_TABLE),
    {ok, #state{self = Self,
                partition = Partition,
                oplog_replica = OpLog}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast({perform_op, Promise, Key, VC, Val}, State) ->
    ok = perform_op_internal(Promise, Key, VC, Val, State),
    {noreply, State};

handle_cast({decide_blue, TxId, VC}, State) ->
    ok = decide_blue_internal(State#state.partition, TxId, VC),
    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({retry_op, Promise, Key, VC, Val}, State) ->
    ok = perform_op_internal(Promise, Key, VC, Val, State),
    {noreply, State};

handle_info({retry_decide, TxId, VC}, State) ->
    ok = decide_blue_internal(State#state.partition, TxId, VC),
    {noreply, State};

handle_info(Info, State) ->
    ?LOG_INFO("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% todo(borja): Update uniformVC here once we add replication
%% update it before we check knownVC
-spec perform_op_internal(grb_promise:t(), key(), vclock(), val(), #state{}) -> ok.
perform_op_internal(Promise, Key, VC, Val, State) ->
    case check_known_vc(VC) of
        {not_ready, WaitTime} ->
            erlang:send_after(WaitTime, self(), {retry_op, Promise, Key, VC, Val}),
            ok;
        ready ->
            perform_op_internal_continue(Promise, Key, VC, Val, State)
    end.

-spec check_known_vc(vclock()) -> ready | {not_ready, non_neg_integer()}.
check_known_vc(VC) ->
    KnownVC = grb_replica_state:known_vc(),
    CurrentReplica = grb_dc_utils:replica_id(),
    SelfBlue = grb_vclock:get_time(CurrentReplica, VC),
    SelfRed = grb_vclock:get_time(red, VC),
    BlueTime = grb_vclock:get_time(CurrentReplica, KnownVC),
    RedTime = grb_vclock:get_time(red, KnownVC),
    BlueCheck = BlueTime >= SelfBlue,
    RedCheck = RedTime >= SelfRed,
    case (BlueCheck andalso RedCheck) of
        true ->
            ready;
        false ->
            %% todo(borja): stat here
            {not_ready, ?OP_WAIT_MS}
    end.

%% todo(borja): Actually read from correct snapshot
-spec perform_op_internal_continue(grb_promise:t(), key(), vclock(), val(), #state{}) -> ok.
perform_op_internal_continue(Promise, Key, _VC, Val, State) ->
    case ets:lookup(State#state.oplog_replica, Key) of
        [] ->
            grb_promise:resolve({ok, Val, 0}, Promise);
        [{Key, Log}] ->
            [{FirstVC, FirstVal} | _] = Log,
            ResTs = grb_vclock:get_time(red, FirstVC),
            grb_promise:resolve({ok, FirstVal, ResTs}, Promise)
    end.

-spec decide_blue_internal(partition_id(), _, vclock()) -> ok.
decide_blue_internal(Partition, TxId, VC) ->
    case check_current_clock(VC) of
        {not_ready, WaitTime} ->
            erlang:send_after(WaitTime, self(), {retry_decide, TxId, VC}),
            ok;
        ready ->
            riak_core_vnode_master:command({Partition, node()},
                {decide_blue, TxId, VC},
                grb_vnode_master)
    end.

-spec check_current_clock(vclock()) -> ready | {not_rady, non_neg_integer()}.
check_current_clock(VC) ->
    CurrentReplica = grb_dc_utils:replica_id(),
    SelfBlue = grb_vclock:get_time(CurrentReplica, VC),
    CurrentTS = grb_time:timestamp(),
    case CurrentTS >= SelfBlue of
        true ->
            ready;
        false ->
            {not_ready, ?OP_WAIT_MS}
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
            ?LOG_DEBUG("Unable to start pvc read replica for ~p, will retry", [Partition]),
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
