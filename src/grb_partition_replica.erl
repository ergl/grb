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
         stop_replicas/1,
         replica_ready/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(NUM_REPLICAS_KEY, num_replicas).

-record(state, {
    %% Partition that this server is replicating
    partition :: partition_id(),
    replica_id :: replica_id(),

    known_barrier_wait_ms :: non_neg_integer(),
    pending_decides = #{} :: #{term() => vclock()},

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
    gen_server:start_link(?MODULE, [Partition, Id], []).

%% @doc Start `Count` read replicas for the given partition
-spec start_replicas(partition_id(), non_neg_integer()) -> ok.
start_replicas(Partition, Count) ->
    ok = persist_num_replicas(Partition, Count),
    start_replicas_internal(Partition, Count).

%% @doc Check if all the read replicas at this node and partitions are ready
-spec replica_ready(partition_id(), non_neg_integer()) -> boolean().
replica_ready(Partition, N) ->
    replica_ready_internal(Partition, N).

%% @doc Stop read replicas for the given partition
-spec stop_replicas(partition_id()) -> ok.
stop_replicas(Partition) ->
    stop_replicas_internal(Partition, num_replicas(Partition)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec async_key_vsn(grb_promise:t(), partition_id(), key(), vclock()) -> ok.
async_key_vsn(Promise, Partition, Key, VC) ->
    gen_server:cast(random_replica(Partition), {key_vsn, Promise, Key, VC}).

-spec decide_blue(partition_id(), _, vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    gen_server:cast(random_replica(Partition), {decide_blue, TxId, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    ok = persist_replica_pid(Partition, Id, self()),
    OpLog = grb_main_vnode:op_log_table(Partition),
    {ok, WaitMs} = application:get_env(grb, partition_ready_wait_ms),
    {ok, #state{partition=Partition,
                replica_id=grb_dc_manager:replica_id(),
                known_barrier_wait_ms=WaitMs,
                oplog_replica = OpLog}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [Request]),
    {noreply, State}.

handle_cast({key_vsn, Promise, Key, VC}, State) ->
    ok = key_vsn_wait(Promise, Key, VC, State),
    {noreply, State};

handle_cast({decide_blue, TxId, VC}, S0=#state{partition=Partition,
                                               replica_id=ReplicaId,
                                               pending_decides=Pending,
                                               known_barrier_wait_ms=WaitMs}) ->

    S = case grb_main_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId}),
            S0#state{pending_decides=Pending#{TxId => VC}};
        ready ->
            grb_main_vnode:decide_blue(Partition, TxId, VC),
            S0
    end,
    {noreply, S};

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [Request]),
    {noreply, State}.

handle_info({retry_key_vsn_wait, Promise, Key, VC}, State) ->
    ok = key_vsn_wait(Promise, Key, VC, State),
    {noreply, State};

handle_info({retry_decide, TxId}, S0=#state{partition=Partition,
                                            replica_id=ReplicaId,
                                            pending_decides=Pending,
                                            known_barrier_wait_ms=WaitMs}) ->
    #{TxId := VC} = Pending,
    S = case grb_main_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId}),
            S0;
        ready ->
            grb_main_vnode:decide_blue(Partition, TxId, VC),
            S0#state{pending_decides=maps:remove(TxId, Pending)}
    end,
    {noreply, S};

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled msg ~p", [Info]),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Start / Ready / Stop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
                ok = gen_server:call(replica_pid(Partition, N), shutdown)
            catch _:_ ->
                ok
            end,
            start_replicas_internal(Partition, N - 1)
    end.

-spec replica_ready_internal(partition_id(), non_neg_integer()) -> boolean().
replica_ready_internal(_Partition, 0) ->
    true;
replica_ready_internal(Partition, N) ->
    try
        case gen_server:call(replica_pid(Partition, N), ready) of
            ready ->
                replica_ready_internal(Partition, N - 1);
            _ ->
                false
        end
    catch _:_ ->
        false
    end.

-spec stop_replicas_internal(partition_id(), non_neg_integer()) -> ok.
stop_replicas_internal(_Partition, 0) ->
    ok;

stop_replicas_internal(Partition, N) ->
    try
        ok = gen_server:call(replica_pid(Partition, N), shutdown)
    catch _:_ ->
        ok
    end,
    stop_replicas_internal(Partition, N - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Naming
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec replica_pid(partition_id(), non_neg_integer()) -> pid().
replica_pid(Partition, Id) ->
    persistent_term:get({?MODULE, Partition, Id}).

-spec persist_replica_pid(partition_id(), non_neg_integer(), pid()) -> ok.
persist_replica_pid(Partition, Id, Pid) ->
    persistent_term:put({?MODULE, Partition, Id}, Pid).

-spec random_replica(partition_id()) -> pid().
random_replica(Partition) ->
    replica_pid(Partition, rand:uniform(num_replicas(Partition))).

-spec num_replicas(partition_id()) -> non_neg_integer().
num_replicas(Partition) ->
    persistent_term:get({?MODULE, Partition, ?NUM_REPLICAS_KEY}, ?READ_CONCURRENCY).

-spec persist_num_replicas(partition_id(), non_neg_integer()) -> ok.
persist_num_replicas(Partition, N) ->
    persistent_term:put({?MODULE, Partition, ?NUM_REPLICAS_KEY}, N).
