-module(grb_propagation_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([get_state/1,
         get_uniform_barrier/1,
         get_commit_log/1,
         get_commit_log/2]).
-endif.

%% Management API
-export([start_propagate_timer_all/0,
         stop_propagate_timer_all/0]).

%% Common public API
-export([partition_ready/2,
         partition_ready/3,
         known_vc/1,
         known_time/2,
         stable_vc/1,
         stable_red/1,
         update_stable_vc_sync/2,
         append_blue_commit/3,
         append_remote_blue_commit/5,
         append_remote_blue_commit_no_hb/4,
         handle_blue_heartbeat/3,
         handle_red_heartbeat/2,
         handle_self_blue_heartbeat/2]).

%% I hope you know what you're doing
-export([clock_table/1,
         handle_blue_heartbeat_unsafe/3]).

-ifdef(STABLE_SNAPSHOT).
%% Basic Replication API
-export([merge_remote_stable_vc/2,
         merge_into_stable_vc/2]).
-endif.

%% For CURE-FT
-export([handle_clock_update/3,
         handle_clock_heartbeat_update/3]).

-ifndef(STABLE_SNAPSHOT).
-ignore_xref([handle_clock_update/3,
              handle_clock_heartbeat_update/3]).
-endif.

%% Uniform Replication API
-export([uniform_vc/1,
         merge_remote_uniform_vc/2,
         merge_into_uniform_vc/2,
         handle_clock_update/4,
         handle_clock_heartbeat_update/4,
         register_uniform_barrier/3,
         register_red_uniform_barrier/4]).

%% riak_core_vnode callbacks
-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_overload_command/3,
         handle_overload_info/2,
         handle_coverage/4,
         handle_exit/3,
         handle_info/2]).

%% Called by vnode proxy
-ignore_xref([start_vnode/1,
              handle_info/2,
              stable_vc/1]).

-define(master, grb_propagation_vnode_master).

-define(prune_req, prune_event).
-define(replication_req, replication_event).
-define(uniform_req, uniform_replication_event).
-define(clock_send_req, clock_send_event).

-define(known_key(Replica), {known_vc, Replica}).
-define(stable_red_key, stable_vc_red).
-define(stable_key, stable_vc).
-define(uniform_key, uniform_vc).

-define(PARTITION_CLOCK_TABLE, partition_clock_table).
-define(CLOG_TABLE(Replica, Partition), {?MODULE, commit_log, Replica, Partition}).

-type stable_matrix() :: #{replica_id() => vclock()}.
-type global_known_matrix() :: #{{replica_id(), replica_id()} => grb_time:ts()}.
-type remote_commit_logs() :: #{replica_id() => grb_remote_commit_log:t()}.
-type uniform_barriers() :: orddict:orddict(grb_time:ts(), [grb_promise:t()]).
-type clock_cache() :: cache(atom, vclock()) | cache({known_vc, replica_id() | id}, grb_time:ts()).

-record(state, {
    partition :: partition_id(),
    local_replica :: replica_id() | undefined,

    self_log :: grb_blue_commit_log:t() | undefined,
    remote_logs = #{} :: remote_commit_logs(),

    %% only used in basic replication mode
    basic_last_sent = 0 :: grb_time:ts(),
    global_known_matrix = #{} :: global_known_matrix(),

    %% send our transactions / heartbeats to remote replicas
    replication_interval :: non_neg_integer(),
    replication_timer = undefined :: reference() | undefined,

    %% relay transactions from other replicas
    uniform_interval :: non_neg_integer(),
    uniform_timer = undefined :: reference() | undefined,

    %% send knownVC / stableVC to remote replicas
    replicate_clocks_interval :: non_neg_integer(),
    replicate_clocks_timer = undefined :: reference() | undefined,

    %% prune committedBlue
    prune_interval :: non_neg_integer(),
    prune_timer = undefined :: reference() | undefined,

    %% All groups with f+1 replicas that include ourselves
    fault_tolerant_groups = [] :: [[replica_id()]],
    stable_matrix = #{} :: stable_matrix(),

    clock_cache :: clock_cache(),

    %% List of pending uniform barriers by clients, recompute on uniformVC update
    pending_barriers = [] :: uniform_barriers()
}).

-ifdef(NO_FWD_REPLICATION).
-define(timers_unset, #state{replication_timer=undefined}).
-else.
-ifdef(UNIFORM_SNAPSHOT).
-define(timers_unset, #state{replication_timer=undefined, uniform_timer=undefined,
                             replicate_clocks_timer=undefined, prune_timer=undefined}).
-else.
-define(timers_unset, #state{replication_timer=undefined, uniform_timer=undefined, prune_timer=undefined}).
-endif.
-endif.

-type state() :: #state{}.

-ifdef(TEST).
get_state(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()}, get_state, ?master, infinity).

-spec get_uniform_barrier(partition_id()) -> uniform_barriers().
get_uniform_barrier(Partition) ->
    State = riak_core_vnode_master:sync_command({Partition, node()}, get_state, ?master, infinity),
    State#state.pending_barriers.

-spec get_commit_log(partition_id()) -> grb_blue_commit_log:t().
get_commit_log(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()}, get_clog, ?master, infinity).

-endif.

-spec get_commit_log(replica_id(), partition_id()) -> grb_remote_commit_log:t().
get_commit_log(Replica, Partition) ->
    persistent_term:get(?CLOG_TABLE(Replica, Partition)).

%%%===================================================================
%%% common public api
%%%===================================================================

-spec start_propagate_timer_all() -> ok.
start_propagate_timer_all() ->
    [try
        riak_core_vnode_master:command(N, start_propagate_timer, ?master)
     catch
         _:_ -> ok
     end  || N <- grb_dc_utils:get_index_nodes() ],
    ok.

-spec stop_propagate_timer_all() -> ok.
stop_propagate_timer_all() ->
    [try
        riak_core_vnode_master:command(N, stop_propagate_timer, ?master)
     catch
         _:_ -> ok
     end  || N <- grb_dc_utils:get_index_nodes() ],
    ok.

-spec partition_ready(partition_id(), vclock()) -> ready | not_ready.
partition_ready(Partition, SnapshotVC) ->
    partition_ready(Partition, grb_dc_manager:replica_id(), SnapshotVC).

-spec partition_ready(partition_id(), replica_id(), vclock()) -> ready | not_ready.
-ifdef(BLUE_KNOWN_VC).
partition_ready(Partition, ReplicaId, SnapshotVC) ->
    SnapshotTime = grb_vclock:get_time(ReplicaId, SnapshotVC),
    PartitionTime = known_time(Partition, ReplicaId),
    case PartitionTime >= SnapshotTime of
        true ->
            ready;
        false ->
            ok = grb_measurements:log_counter({?MODULE, ?FUNCTION_NAME}),
            not_ready
    end.
-else.
partition_ready(Partition, ReplicaId, SnapshotVC) ->
    SnapshotTime = grb_vclock:get_time(ReplicaId, SnapshotVC),
    PartitionTime = known_time(Partition, ReplicaId),
    case PartitionTime >= SnapshotTime of
        false ->
            ok = grb_measurements:log_counter({?MODULE, ?FUNCTION_NAME}),
            not_ready;
        true ->
            SnapshotRed = grb_vclock:get_time(?RED_REPLICA, SnapshotVC),
            PartitionRed = known_time(Partition, ?RED_REPLICA),
            case PartitionRed >= SnapshotRed of
                false ->
                    ok = grb_measurements:log_counter({?MODULE, ?FUNCTION_NAME}),
                    not_ready;
                true ->
                    ready
            end
    end.
-endif.

-spec known_vc(partition_id()) -> vclock().
-ifdef(BLUE_KNOWN_VC).
known_vc(Partition) ->
    known_vc_internal(clock_table(Partition)).
-else.
known_vc(Partition) ->
    Table = clock_table(Partition),
    grb_vclock:set_time(?RED_REPLICA,
                        known_time_internal(?RED_REPLICA, Table),
                        known_vc_internal(Table)).
-endif.

-spec known_vc_internal(clock_cache()) -> vclock().
known_vc_internal(ClockTable) ->
    lists:foldl(fun(Replica, Acc) ->
        Ts = known_time_internal(Replica, ClockTable),
        grb_vclock:set_time(Replica, Ts, Acc)
    end, grb_vclock:new(), grb_dc_manager:all_replicas()).

-spec known_time(partition_id(), (replica_id() | ?RED_REPLICA)) -> grb_time:ts().
known_time(Partition, ReplicaId) ->
    known_time_internal(ReplicaId, clock_table(Partition)).

-spec known_time_internal((replica_id() | ?RED_REPLICA), clock_cache()) -> grb_time:ts().
known_time_internal(ReplicaId, ClockTable) ->
    try
        ets:lookup_element(ClockTable, ?known_key(ReplicaId), 2)
    catch _:_ ->
        0
    end.

-spec stable_vc(partition_id()) -> vclock().
stable_vc(Partition) ->
    ets:lookup_element(clock_table(Partition), ?stable_key, 2).

-spec stable_red(partition_id()) -> grb_time:ts().
stable_red(Partition) ->
    ets:lookup_element(clock_table(Partition), ?stable_red_key, 2).

-spec update_stable_vc_sync(partition_id(), vclock()) -> ok.
update_stable_vc_sync(Partition, SVC) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {recompute_stable_vc, SVC},
                                        ?master,
                                        infinity).

-spec append_blue_commit(partition_id(), writeset(), vclock()) -> ok.
append_blue_commit(Partition, WS, CommitVC) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {append_blue, WS, CommitVC},
                                        ?master,
                                        infinity).

-spec append_remote_blue_commit(replica_id(), partition_id(), grb_time:ts(), #{}, vclock()) -> ok.
append_remote_blue_commit(ReplicaId, Partition, CommitTime, WS, CommitVC) ->
    ok = handle_blue_heartbeat(Partition, ReplicaId, CommitTime),
    ok = grb_remote_commit_log:insert(CommitTime, WS, CommitVC, get_commit_log(ReplicaId, Partition)).

-spec append_remote_blue_commit_no_hb(replica_id(), partition_id(), writeset(), vclock()) -> ok.
append_remote_blue_commit_no_hb(ReplicaId, Partition, WS, CommitVC) ->
    ok = grb_remote_commit_log:insert(grb_vclock:get_time(ReplicaId, CommitVC),
                                      WS,
                                      CommitVC,
                                      get_commit_log(ReplicaId, Partition)).

%%%===================================================================
%%% basic replication api
%%%===================================================================

-ifdef(STABLE_SNAPSHOT).
%% @doc Update the stableVC at all replicas but the current one, return result
-spec merge_remote_stable_vc(partition_id(), vclock()) -> vclock().
merge_remote_stable_vc(Partition, VC) ->
    S0 = stable_vc(Partition),
    S1 = grb_vclock:max_at_keys(grb_dc_manager:remote_replicas(), S0, VC),
    riak_core_vnode_master:command({Partition, node()},
                                   {cure_update_svc, S1},
                                   ?master),
    S1.

-spec merge_into_stable_vc(partition_id(), vclock()) -> ok.
merge_into_stable_vc(Partition, VC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {cure_update_svc_no_return, VC},
                                   ?master).

-endif.

%%%===================================================================
%%% uniform replication api
%%%===================================================================

-spec uniform_vc(partition_id()) -> vclock().
uniform_vc(Partition) ->
    ets:lookup_element(clock_table(Partition), ?uniform_key, 2).

-spec update_uniform_vc(partition_id(), vclock()) -> ok.
update_uniform_vc(Partition, SVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {update_uniform_vc, SVC},
                                   ?master).

%% @doc Update the uniformVC at all replicas but the current one, return result
-spec merge_remote_uniform_vc(partition_id(), vclock()) -> vclock().
merge_remote_uniform_vc(Partition, VC) ->
    S0 = uniform_vc(Partition),
    S1 = grb_vclock:max_at_keys(grb_dc_manager:remote_replicas(), S0, VC),
    update_uniform_vc(Partition, S1),
    S1.

%% @doc Update the uniformVC at all replicas but the current one, doesn't return anything
-spec merge_into_uniform_vc(partition_id(), vclock()) -> ok.
merge_into_uniform_vc(Partition, VC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {merge_uniform_vc, VC},
                                   ?master).

-spec handle_blue_heartbeat(partition_id(), replica_id(), grb_time:ts()) -> ok.
handle_blue_heartbeat(Partition, ReplicaId, Ts) ->
    update_known_vc(ReplicaId, Ts, clock_table(Partition)).

-spec handle_red_heartbeat(partition_id(), grb_time:ts()) -> ok.
handle_red_heartbeat(Partition, Ts) ->
    update_known_vc(?RED_REPLICA, Ts, clock_table(Partition)).

%% @doc Like handle_blue_heartbeat/3, but for our own replica, and sync
-spec handle_self_blue_heartbeat(partition_id(), grb_time:ts()) -> ok.
handle_self_blue_heartbeat(Partition, Ts) ->
    SelfReplica = grb_dc_manager:replica_id(),
    %% ok to insert directly, we are always called from the same place
    handle_blue_heartbeat_unsafe(SelfReplica, Ts, clock_table(Partition)).

-spec handle_blue_heartbeat_unsafe(replica_id(), grb_time:ts(), cache_id()) -> ok.
handle_blue_heartbeat_unsafe(ReplicaId, Time, Table) ->
    true = ets:insert(Table, {?known_key(ReplicaId), Time}),
    ok.

-spec handle_clock_update(partition_id(), replica_id(), vclock()) -> ok.
handle_clock_update(Partition, FromReplicaId, KnownVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {remote_clock_update, FromReplicaId, KnownVC},
                                   ?master).

%% @doc Same as handle_clock_update/3, but treat knownVC as a blue heartbeat
-spec handle_clock_heartbeat_update(partition_id(), replica_id(), vclock()) -> ok.
handle_clock_heartbeat_update(Partition, FromReplicaId, KnownVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {remote_clock_heartbeat_update, FromReplicaId, KnownVC},
                                   ?master).

-spec handle_clock_update(partition_id(), replica_id(), vclock(), vclock()) -> ok.
handle_clock_update(Partition, FromReplicaId, KnownVC, StableVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {remote_clock_update, FromReplicaId, KnownVC, StableVC},
                                   ?master).

%% @doc Same as handle_clock_update/4, but treat knownVC as a blue heartbeat
-spec handle_clock_heartbeat_update(partition_id(), replica_id(), vclock(), vclock()) -> ok.
handle_clock_heartbeat_update(Partition, FromReplicaId, KnownVC, StableVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {remote_clock_heartbeat_update, FromReplicaId, KnownVC, StableVC},
                                   ?master).


-spec register_uniform_barrier(grb_promise:t(), partition_id(), grb_time:ts()) -> ok.
register_uniform_barrier(Promise, Partition, Timestamp) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {uniform_barrier, Promise, Timestamp},
                                   ?master).

-spec register_red_uniform_barrier(partition_id(), grb_time:ts(), red_coordinator(), term()) -> ok.
register_red_uniform_barrier(Partition, Timestamp, Pid, TxId) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {red_uniform_barrier, Pid, TxId, Timestamp},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, ReplInt} = application:get_env(grb, basic_replication_interval),
    {ok, PruneInterval} = application:get_env(grb, prune_committed_blue_interval),
    {ok, UniformInterval} = application:get_env(grb, uniform_replication_interval),
    {ok, SendClockInterval} = application:get_env(grb, remote_clock_broadcast_interval),

    ClockTable = ets:new(?PARTITION_CLOCK_TABLE, [ordered_set, public, {read_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?PARTITION_CLOCK_TABLE}, ClockTable),

    true = ets:insert(ClockTable, [{?uniform_key, grb_vclock:new()},
                                   {?stable_key, grb_vclock:new()},
                                   {?stable_red_key, 0},
                                   {?known_key(?RED_REPLICA), 0}]),

    {ok, #state{partition=Partition,
                local_replica=undefined, % ok to do this, we'll overwrite it after join
                self_log=undefined, % ok to do this, we'll overwrite it after join
                prune_interval=PruneInterval,
                replication_interval=ReplInt,
                uniform_interval=UniformInterval,
                replicate_clocks_interval=SendClockInterval,
                clock_cache=ClockTable}}.

terminate(_Reason, #state{clock_cache=ClockCache, remote_logs=RemoteLogs}) ->
    try
        ets:delete(ClockCache),
        [ grb_remote_commit_log:delete(Log) || Log <- maps:values(RemoteLogs)]
    catch _:_ ->
        ok
    end,
    ok.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(get_state, _Sender, State) ->
    {reply, State, State};

handle_command(get_clog, _Sender, State=#state{self_log=Log}) ->
    {reply, Log, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.clock_cache]),
    {reply, Ready, State};

handle_command(learn_dc_id, _Sender, S=#state{clock_cache=ClockTable}) ->
    %% called after joining ring, this is now the correct id
    ReplicaId = grb_dc_manager:replica_id(),
    true = ets:insert(ClockTable, {?known_key(ReplicaId), 0}),

    %% Fill stableVC
    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    true = ets:update_element(ClockTable, ?stable_key, {2, grb_vclock:set_time(ReplicaId, 0, OldSVC)}),

    %% Fill uniformVC
    OldUVC = ets:lookup_element(ClockTable, ?uniform_key, 2),
    true = ets:update_element(ClockTable, ?uniform_key, {2, grb_vclock:set_time(ReplicaId, 0, OldUVC)}),

    {reply, ok, S#state{local_replica=ReplicaId,
                        self_log=grb_blue_commit_log:new(ReplicaId)}};

handle_command({learn_dc_groups, MyGroups}, _From, S) ->
    %% called after connecting other replicas
    %% populate log, avoid allocating on the replication path
    %% Once we know the identifiers of all remote replicas, we can allocate long-lived resources,
    %% so we don't have to do some operations as we go
    {reply, ok, S#state{fault_tolerant_groups=MyGroups}};

handle_command(populate_logs, _From, State) ->
    {reply, ok, populate_logs_internal(State)};

handle_command(start_propagate_timer, _From, State) ->
    {noreply, start_propagation_timers(State)};

handle_command(stop_propagate_timer, _From, State) ->
    {noreply, stop_propagation_timers(State)};

handle_command({cure_update_svc, StableVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, StableVC),
    true = ets:update_element(ClockTable, ?stable_key, {2, NewSVC}),
    {noreply, S};

handle_command({cure_update_svc_no_return, SnapshotVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    NewSVC = grb_vclock:max_at_keys(grb_dc_manager:remote_replicas(), OldSVC, SnapshotVC),
    true = ets:update_element(ClockTable, ?stable_key, {2, NewSVC}),
    {noreply, S};

handle_command({recompute_stable_vc, SVC}, _Sender, State=#state{clock_cache=ClockTable}) ->
    NewStableVC = recompute_stable_vc(SVC, ClockTable),
    {reply, ok, recompute_local_uniform_vc(NewStableVC, State)};

handle_command({update_uniform_vc, SVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, ?uniform_key, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, SVC),
    true = ets:update_element(ClockTable, ?uniform_key, {2, NewSVC}),
    {noreply, S};

handle_command({merge_uniform_vc, SnapshotVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, ?uniform_key, 2),
    NewSVC = grb_vclock:max_at_keys(grb_dc_manager:remote_replicas(), OldSVC, SnapshotVC),
    true = ets:update_element(ClockTable, ?uniform_key, {2, NewSVC}),
    {noreply, S};

handle_command({remote_clock_update, FromReplicaId, KnownVC, StableVC}, _Sender, S) ->
    {noreply, update_clocks(FromReplicaId, KnownVC, StableVC, S)};

handle_command({remote_clock_heartbeat_update, FromReplicaId, KnownVC, StableVC}, _Sender, S=#state{clock_cache=ClockCache}) ->
    Timestamp = grb_vclock:get_time(FromReplicaId, KnownVC),
    ok = update_known_vc(FromReplicaId, Timestamp, ClockCache),
    {noreply, update_clocks(FromReplicaId, KnownVC, StableVC, S)};

handle_command({append_blue, WS, CommitVC}, _Sender, S=#state{self_log=Log})->
    {reply, ok, S#state{self_log=grb_blue_commit_log:insert(WS, CommitVC, Log)}};

handle_command({uniform_barrier, Promise, Timestamp}, _Sender, S=#state{pending_barriers=Barriers}) ->
    {noreply, S#state{pending_barriers=insert_uniform_barrier(Promise, Timestamp, Barriers)}};

handle_command({red_uniform_barrier, Pid, TxId, Timestamp}, _Sender, S=#state{pending_barriers=Barriers}) ->
    {noreply, S#state{pending_barriers=insert_red_uniform_barrier(Pid, TxId, Timestamp, Barriers)}};

handle_command({remote_clock_update, FromReplicaId, KnownVC}, _Sender, S) ->
    {noreply, update_clocks(FromReplicaId, KnownVC, S)};

handle_command({remote_clock_heartbeat_update, FromReplicaId, KnownVC}, _Sender, S=#state{clock_cache=ClockCache}) ->
    Timestamp = grb_vclock:get_time(FromReplicaId, KnownVC),
    ok = update_known_vc(FromReplicaId, Timestamp, ClockCache),
    {noreply, update_clocks(FromReplicaId, KnownVC, S)};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

handle_info(?replication_req, State=#state{replication_timer=Timer,
                                           replication_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    NewState = replicate_internal(State),
    {ok, NewState#state{replication_timer=erlang:send_after(Interval, self(), ?replication_req)}};

handle_info(?uniform_req, State=#state{partition=P,
                                       uniform_timer=Timer,
                                       uniform_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    ?LOG_DEBUG("starting uniform replication at ~p", [P]),
    GlobalMatrix = uniform_replicate_internal(State),
    {ok, State#state{global_known_matrix=GlobalMatrix,
                     uniform_timer=erlang:send_after(Interval, self(), ?uniform_req)}};

handle_info(?prune_req, S0=#state{prune_timer=Timer,
                                  prune_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    State = prune_commit_logs(S0),
    {ok, State#state{prune_timer=erlang:send_after(Interval, self(), ?prune_req)}};

handle_info(?clock_send_req, State=#state{partition=Partition,
                                          clock_cache=ClockTable,
                                          replicate_clocks_timer=Timer,
                                          replicate_clocks_interval=Interval}) ->
    erlang:cancel_timer(Timer),

    KnownVC = known_vc_internal(ClockTable),
    StableVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    lists:foreach(fun(Target) ->
        ok = grb_dc_connection_manager:send_clocks(Target, Partition, KnownVC, StableVC)
    end, grb_dc_connection_manager:connected_replicas()),

    {ok, State#state{replicate_clocks_timer=erlang:send_after(Interval, self(), ?clock_send_req)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

%%%===================================================================
%%% internal functions
%%%===================================================================

-spec populate_logs_internal(state()) -> state().
populate_logs_internal(S=#state{remote_logs=Logs0,
                                partition=Partition,
                                clock_cache=ClockTable,
                                local_replica=LocalReplica,
                                stable_matrix=StableMatrix0}) ->

    RemoteReplicas = grb_dc_manager:remote_replicas(),
    true = ets:insert(ClockTable, [{?known_key(R), 0} || R <- RemoteReplicas]),

    FillClock = fun(Replica, Acc) -> grb_vclock:set_time(Replica, 0, Acc) end,

    %% Fill stableVC
    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    true = ets:update_element(ClockTable, ?stable_key,
                              {2, lists:foldl(FillClock, OldSVC, RemoteReplicas)}),

    %% Fill uniformVC
    OldUVC = ets:lookup_element(ClockTable, ?uniform_key, 2),
    true = ets:update_element(ClockTable, ?uniform_key,
                              {2, lists:foldl(FillClock, OldUVC, RemoteReplicas)}),

    Logs = lists:foldl(fun(Replica, LogAcc) ->
        L = grb_remote_commit_log:new(),
        ok = persistent_term:put(?CLOG_TABLE(Replica, Partition), L),
        LogAcc#{Replica => L}
    end, Logs0, RemoteReplicas),

    StableMatrix = lists:foldl(fun(Replica, MatrixAcc) ->
        MatrixAcc#{Replica => grb_vclock:new()}
    end, StableMatrix0, [LocalReplica | RemoteReplicas]),

    S#state{remote_logs=Logs, stable_matrix=StableMatrix}.

-spec start_propagation_timers(state()) -> state().
start_propagation_timers(State=?timers_unset) -> start_propagation_timers_internal(State);
start_propagation_timers(State) -> State.

-spec stop_propagation_timers(state()) -> state().
stop_propagation_timers(State=?timers_unset) -> State;
stop_propagation_timers(State) -> stop_propagation_timers_internal(State).

-spec start_propagation_timers_internal(state()) -> state().
-spec stop_propagation_timers_internal(state()) -> state().

-ifdef(NO_FWD_REPLICATION).

start_propagation_timers_internal(State) ->
    State#state{
        replication_timer=erlang:send_after(State#state.replication_interval, self(), ?replication_req)
    }.


stop_propagation_timers_internal(State) ->
    erlang:cancel_timer(State#state.replication_timer),
    State#state{
        replication_timer=undefined
    }.

-else.

-ifdef(UNIFORM_SNAPSHOT).

start_propagation_timers_internal(State) ->
    State#state{
        prune_timer=erlang:send_after(State#state.prune_interval, self(), ?prune_req),
        uniform_timer=erlang:send_after(State#state.uniform_interval, self(), ?uniform_req),
        replication_timer=erlang:send_after(State#state.replication_interval, self(), ?replication_req),
        replicate_clocks_timer=erlang:send_after(State#state.replicate_clocks_interval, self(), ?clock_send_req)
    }.

stop_propagation_timers_internal(State) ->
    erlang:cancel_timer(State#state.prune_timer),
    erlang:cancel_timer(State#state.uniform_timer),
    erlang:cancel_timer(State#state.replication_timer),
    erlang:cancel_timer(State#state.replicate_clocks_timer),
    State#state{
        prune_timer=undefined,
        uniform_timer=undefined,
        replication_timer=undefined,
        replicate_clocks_timer=undefined
    }.

-else.

start_propagation_timers_internal(State) ->
    State#state{
        prune_timer=erlang:send_after(State#state.prune_interval, self(), ?prune_req),
        uniform_timer=erlang:send_after(State#state.uniform_interval, self(), ?uniform_req),
        replication_timer=erlang:send_after(State#state.replication_interval, self(), ?replication_req)
    }.

stop_propagation_timers_internal(State) ->
    erlang:cancel_timer(State#state.prune_timer),
    erlang:cancel_timer(State#state.uniform_timer),
    erlang:cancel_timer(State#state.replication_timer),
    State#state{
        prune_timer=undefined,
        uniform_timer=undefined,
        replication_timer=undefined
    }.

-endif.
-endif.

-spec prune_commit_logs(state()) -> state().
prune_commit_logs(S=#state{self_log=LocalLog,
                           remote_logs=RemoteLogs,
                           local_replica=LocalId,
                           global_known_matrix=Matrix}) ->

    ?LOG_DEBUG("Running prune on logs"),
    RemoteReplicas = grb_dc_manager:remote_replicas(),
    ok = prune_remote_commit_logs(RemoteReplicas, RemoteLogs, Matrix),
    LocalMinTS = min_global_matrix_ts(RemoteReplicas, LocalId, Matrix),
    S#state{self_log=grb_blue_commit_log:remove_leq(LocalMinTS, LocalLog)}.

-spec prune_remote_commit_logs([replica_id()], remote_commit_logs(), global_known_matrix()) -> ok.
prune_remote_commit_logs(RemoteReplicas, Logs, Matrix) ->
    lists:foreach(fun({Replica, Log}) ->
        MinTs = min_global_matrix_ts(RemoteReplicas, Replica, Matrix),
        ok = grb_remote_commit_log:remove_leq(MinTs, Log)
    end, maps:to_list(Logs)).

-spec recompute_stable_vc(vclock(), clock_cache()) -> vclock().
-ifdef(BLUE_KNOWN_VC).
recompute_stable_vc(NewStableVC, ClockTable) ->
    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max_at_keys(grb_dc_manager:all_replicas(), OldSVC, NewStableVC),
    true = ets:update_element(ClockTable, ?stable_key, {2, NewSVC}),
    NewSVC.
-else.
recompute_stable_vc(NewStableVC, ClockTable) ->
    %% Update stableVC
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    NewSVC = grb_vclock:max_at_keys(grb_dc_manager:all_replicas(), OldSVC, NewStableVC),
    true = ets:update_element(ClockTable, ?stable_key, {2, NewSVC}),

    %% Update stableVC[red]
    NewRedTs = grb_vclock:get_time(?RED_REPLICA, NewStableVC),
    ets:select_replace(ClockTable,
                       [{ {?stable_red_key, '$1'}, [{'<', '$1', NewRedTs}], [{const, {?stable_red_key, NewRedTs}}] }]),
    NewSVC.
-endif.

-spec recompute_local_uniform_vc(vclock(), state()) -> state().
-ifdef(STABLE_SNAPSHOT).

recompute_local_uniform_vc(_, State) -> State.

-else.

recompute_local_uniform_vc(StableVC, S=#state{local_replica=LocalId,
                                        clock_cache=ClockTable,
                                        stable_matrix=StableMatrix0,
                                        fault_tolerant_groups=Groups,
                                        pending_barriers=PendingBarriers0}) ->

    StableMatrix = StableMatrix0#{LocalId => StableVC},
    UniformVC = update_uniform_vc(StableMatrix, ClockTable, Groups),
    PendingBarriers = lift_pending_uniform_barriers(LocalId, UniformVC, PendingBarriers0),
    S#state{stable_matrix=StableMatrix, pending_barriers=PendingBarriers}.

-endif.

-spec insert_uniform_barrier(grb_promise:t(), grb_time:ts(), uniform_barriers()) -> uniform_barriers().
insert_uniform_barrier(Promise, Timestamp, Barriers) ->
    case orddict:is_key(Timestamp, Barriers) of
        true -> orddict:append(Timestamp, Promise, Barriers);
        false -> orddict:store(Timestamp, [Promise], Barriers)
    end.

-spec insert_red_uniform_barrier(red_coordinator(), term(), grb_time:ts(), uniform_barriers()) -> uniform_barriers().
insert_red_uniform_barrier(Pid, TxId, Timestamp, Barriers) ->
    case orddict:is_key(Timestamp, Barriers) of
        true -> orddict:append(Timestamp, {red, Pid, TxId}, Barriers);
        false -> orddict:store(Timestamp, [{red, Pid, TxId}], Barriers)
    end.

-spec lift_pending_uniform_barriers(replica_id(), vclock(), uniform_barriers()) -> uniform_barriers().
lift_pending_uniform_barriers(_, _, []) -> [];
lift_pending_uniform_barriers(ReplicaId, UniformVC, PendingBarriers) ->
    Timestamp = grb_vclock:get_time(ReplicaId, UniformVC),
    lift_pending_uniform_barriers(Timestamp, PendingBarriers).

-spec lift_pending_uniform_barriers(grb_time:ts(), uniform_barriers()) -> uniform_barriers().
lift_pending_uniform_barriers(_, []) -> [];

lift_pending_uniform_barriers(Cutoff, [{Ts, DataList} | Rest]) when Ts =< Cutoff ->
    lists:foreach(fun
        ({red, Pid, TxId}) -> grb_red_coordinator:commit_send(Pid, TxId);
        (Promise) -> grb_promise:resolve(ok, Promise)
    end, DataList),
    lift_pending_uniform_barriers(Cutoff, Rest);

lift_pending_uniform_barriers(Cutoff, [{Ts, _} | _]=Remaining) when Ts > Cutoff ->
    Remaining.

%% For FT-CURE only
-spec update_clocks(replica_id(), vclock(), state()) -> state().
update_clocks(FromReplicaId, KnownVC, S=#state{global_known_matrix=KnownMatrix0}) ->
    KnownMatrix = update_known_matrix(FromReplicaId, KnownVC, KnownMatrix0),
    S#state{global_known_matrix=KnownMatrix}.

-spec update_clocks(replica_id(), vclock(), vclock(), state()) -> state().
update_clocks(FromReplicaId, KnownVC, StableVC, S=#state{local_replica=LocalId,
                                                         clock_cache=ClockCache,
                                                         stable_matrix=StableMatrix0,
                                                         fault_tolerant_groups=Groups,
                                                         global_known_matrix=KnownMatrix0,
                                                         pending_barriers=PendingBarriers0}) ->

    KnownMatrix = update_known_matrix(FromReplicaId, KnownVC, KnownMatrix0),
    StableMatrix = StableMatrix0#{FromReplicaId => StableVC},
    UniformVC = update_uniform_vc(StableMatrix, ClockCache, Groups),
    PendingBarriers = lift_pending_uniform_barriers(LocalId, UniformVC, PendingBarriers0),
    S#state{global_known_matrix=KnownMatrix,
            stable_matrix=StableMatrix,
            pending_barriers=PendingBarriers}.

-spec replicate_internal(state()) -> state().
-ifdef(NO_FWD_REPLICATION).

replicate_internal(S=#state{self_log=LocalLog0,
                            partition=Partition,
                            local_replica=LocalId,
                            clock_cache=ClockTable,
                            basic_last_sent=LastSent}) ->

    LocalTime = known_time_internal(LocalId, ClockTable),
    {ToSend, LocalLog} = grb_blue_commit_log:remove_bigger(LastSent, LocalLog0),
    lists:foreach(fun(Target) ->
        case ToSend of
            [] ->
                HBRes = grb_dc_connection_manager:send_heartbeat(Target, Partition, LocalTime),
                ?LOG_DEBUG("send basic heartbeat to ~p: ~p~n", [Target, HBRes]),
                ok;
            Transactions ->
                lists:foreach(fun({WS, VC}) ->
                    TxRes = grb_dc_connection_manager:send_tx(Target, Partition, WS, VC),
                    ?LOG_DEBUG("send transaction to ~p: ~p~n", [Target, TxRes]),
                    ok
                end, Transactions)
        end
    end, grb_dc_connection_manager:connected_replicas()),

    S#state{basic_last_sent=LocalTime, self_log=LocalLog}.

-else.
-ifdef(UNIFORM_SNAPSHOT).
%% Difference here: don't send the clocks during heartbeats or transactions, use a different timer.
%% (see clock_send_req)
replicate_internal(S=#state{self_log=LocalLog,
                            partition=Partition,
                            local_replica=LocalId,
                            clock_cache=ClockTable,
                            global_known_matrix=Matrix0}) ->

    LocalTime = known_time_internal(LocalId, ClockTable),
    Matrix = lists:foldl(fun(Target, AccMatrix) ->
        ThresholdTime = maps:get({Target, LocalId}, AccMatrix, 0),
        ToSend = grb_blue_commit_log:get_bigger(ThresholdTime, LocalLog),
        case ToSend of
            [] ->
                HBRes = grb_dc_connection_manager:send_heartbeat(Target, Partition, LocalTime),
                ?LOG_DEBUG("send basic heartbeat to ~p: ~p~n", [Target, HBRes]),
                ok;
            Transactions ->
                ok = send_transactions(Target, Partition, Transactions)
        end,
        AccMatrix#{{Target, LocalId} => LocalTime}
    end, Matrix0, grb_dc_connection_manager:connected_replicas()),

    S#state{global_known_matrix=Matrix}.

-else.

replicate_internal(S=#state{self_log=LocalLog,
                            partition=Partition,
                            local_replica=LocalId,
                            clock_cache=ClockTable,
                            global_known_matrix=Matrix0}) ->

    KnownVC = known_vc_internal(ClockTable),
    LocalTime = grb_vclock:get_time(LocalId, KnownVC),
    StableVC = ets:lookup_element(ClockTable, ?stable_key, 2),

    Matrix = lists:foldl(fun(Target, AccMatrix) ->
        ThresholdTime = maps:get({Target, LocalId}, AccMatrix, 0),
        ToSend = grb_blue_commit_log:get_bigger(ThresholdTime, LocalLog),
        case ToSend of
            [] ->
                % piggy back clocks on top of the send_heartbeat message, avoid extra message on the wire
                HBRes = grb_dc_connection_manager:send_clocks_heartbeat(Target, Partition, KnownVC, StableVC),
                ?LOG_DEBUG("send clocks/heartbeat to ~p: ~p~n", [Target, HBRes]),
                ok;
            Transactions ->
                %% can't merge with other messages here, send one before
                %% we could piggy-back on top of the first tx, but w/ever
                ClockRes = grb_dc_connection_manager:send_clocks(Target, Partition, KnownVC, StableVC),
                ?LOG_DEBUG("send clocks to ~p: ~p~n", [Target, ClockRes]),
                ok = send_transactions(Target, Partition, Transactions)
        end,
        AccMatrix#{{Target, LocalId} => LocalTime}
    end, Matrix0, grb_dc_connection_manager:connected_replicas()),

    S#state{global_known_matrix=Matrix}.

-endif.

-spec send_transactions(Target :: replica_id(),
                        Partition :: partition_id(),
                        Transactions :: [tx_entry()]) -> ok.

send_transactions(_, _, []) ->
    ok;
send_transactions(Target, Partition, [Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8 | Rest]) ->
    ok = grb_dc_connection_manager:send_tx_array(Target, Partition, Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8),
    send_transactions(Target, Partition, Rest);
send_transactions(Target, Partition, [Tx1, Tx2, Tx3, Tx4 | Rest]) ->
    ok = grb_dc_connection_manager:send_tx_array(Target, Partition, Tx1, Tx2, Tx3, Tx4),
    send_transactions(Target, Partition, Rest);
send_transactions(Target, Partition, Transactions) ->
    lists:foreach(fun({WS, VC}) ->
        ok = grb_dc_connection_manager:send_tx(Target, Partition, WS, VC)
    end, Transactions).

-endif.

-spec uniform_replicate_internal(state()) -> global_known_matrix().
uniform_replicate_internal(#state{remote_logs=Logs,
                                  partition=Partition,
                                  clock_cache=ClockTable,
                                  global_known_matrix=Matrix}) ->

    RemoteReplicas = grb_dc_manager:remote_replicas(),
    ConnectedReplicas = grb_dc_connection_manager:connected_replicas(),
    lists:foldl(fun(TargetReplica, GlobalMatrix) ->
        ureplicate_to(TargetReplica, RemoteReplicas, Partition, Logs, ClockTable, GlobalMatrix)
    end, Matrix, ConnectedReplicas).

%% @doc Replicate other's transactions / heartbeats to the target replica.
%%
%%      This will iterate over all the other connected replicas and fetch the
%%      necessary transactions/replicas to relay to the target replica.
%%
%%      In effect, we re-send transactions from other replicas to the target,
%%      to ensure that even if a replica goes down, if we received an update
%%      from it, other replicas will see it.
-spec ureplicate_to(Target :: replica_id(),
                    RemoteReplicas :: [replica_id()],
                    LocalPartition :: partition_id(),
                    Logs :: remote_commit_logs(),
                    ClockTable :: clock_cache(),
                    Matrix :: global_known_matrix()) -> global_known_matrix().

ureplicate_to(_TargetReplica, [], _Partition, _Logs, _ClockTable, MatrixAcc) ->
    MatrixAcc;

ureplicate_to(TargetReplica, [TargetReplica | Rest], Partition, Logs, ClockTable, MatrixAcc) ->
    %% don't send back transactions to the sender, skip
    ureplicate_to(TargetReplica, Rest, Partition, Logs, ClockTable, MatrixAcc);

ureplicate_to(TargetReplica, [RelayReplica | Rest], Partition, Logs, ClockTable, MatrixAcc) ->
    %% tx <- { <_, _, VC> \in log[relay] | VC[relay] > globalMatrix[target, relay] }
    %% if tx =/= \emptyset
    %%     for all t \in tx (in t.VC[relay] order)
    %%         send REPLICATE(relay, t) to target
    %% else
    %%     send HEARTBEAT(relay, knownVC[relay]) to target
    %% globalMatrix[target, relay] <- knownVC[relay]
    HeartBeatTime = known_time_internal(RelayReplica, ClockTable),
    ThresholdTime = maps:get({TargetReplica, RelayReplica}, MatrixAcc, 0),
    ToSend = grb_remote_commit_log:get_bigger(ThresholdTime, maps:get(RelayReplica, Logs)),
    case ToSend of
        [] ->
            HBRes = grb_dc_connection_manager:forward_heartbeat(TargetReplica, RelayReplica, Partition, HeartBeatTime),
            ?LOG_DEBUG("relay heartbeat to ~p from ~p: ~p~n", [TargetReplica, RelayReplica, HBRes]),
            ok;
        Txs ->
            %% Entries are already ordered to commit time at the replica of the log
            lists:foreach(fun({WS, VC}) ->
                TxRes = grb_dc_connection_manager:forward_tx(TargetReplica, RelayReplica, Partition, WS, VC),
                ?LOG_DEBUG("relay transaction to ~p from ~p: ~p~n", [TargetReplica, RelayReplica, TxRes]),
                ok
            end, Txs)
    end,
    NewMatrix = MatrixAcc#{{TargetReplica, RelayReplica} => HeartBeatTime},
    ureplicate_to(TargetReplica, Rest, Partition, Logs, ClockTable, NewMatrix).

%% @doc Set knownVC[ReplicaId] <-max- Time
-spec update_known_vc((replica_id() | ?RED_REPLICA), grb_time:ts(), clock_cache()) -> ok.
update_known_vc(ReplicaId, Time, ClockTable) ->
    KeyName = ?known_key(ReplicaId),
    %% select_replace over a single key is atomic, so no-one should interleave with us
    ets:select_replace(ClockTable,
                       [{ {KeyName, '$1'}, [{'<', '$1', Time}], [{const, {KeyName, Time}}] }]),
    ok.

-spec update_known_matrix(replica_id(), vclock(), global_known_matrix()) -> global_known_matrix().
update_known_matrix(FromReplicaId, KnownVC, Matrix) ->
    %% globalKnownMatrix[FromReplicaId] <- KnownVC,
    %% transformed into globalKnownMatrix[FromReplicaId][j] <- knownVC[j]
    lists:foldl(fun({AtReplica, Ts}, Acc) ->
        Acc#{{FromReplicaId, AtReplica} => max(Ts, maps:get({FromReplicaId, AtReplica}, Acc, 0))}
    end, Matrix, grb_vclock:to_list(KnownVC)).

-spec update_uniform_vc(StableMatrix :: stable_matrix(),
                        ClockCache :: clock_cache(),
                        Groups :: [[replica_id()]]) -> vclock().

update_uniform_vc(StableMatrix, ClockCache, Groups) ->
    UniformVC0 = ets:lookup_element(ClockCache, ?uniform_key, 2),
    UniformVC = compute_uniform_vc(UniformVC0, StableMatrix, Groups),
    true = ets:update_element(ClockCache, ?uniform_key, {2, UniformVC}),
    UniformVC.

-spec compute_uniform_vc(vclock(), stable_matrix(), [[replica_id()]]) -> vclock().
compute_uniform_vc(UniformVC, StableMatrix, Groups) ->
    Fresh = grb_vclock:new(),
    VisibleBound = lists:foldl(fun(Group, Acc) ->
        [H|T] = Group,
        SVC = maps:get(H, StableMatrix, Fresh),
        GroupMin = lists:foldl(fun(R, AccSVC) ->
            grb_vclock:min(AccSVC, maps:get(R, StableMatrix, Fresh))
        end, SVC, T),
        grb_vclock:max(Acc, GroupMin)
    end, Fresh, Groups),
    grb_vclock:max(VisibleBound, UniformVC).

%% @doc Compute the lower bound of visible transactions from the globalKnownMatrix
%%
%%      min{ globalKnownMatrix[j][i] | j = 1..D, j /= d }
%%      where i = SourceReplica
%%            j = RemoteReplicas
%%
-spec min_global_matrix_ts([replica_id()], replica_id(), global_known_matrix()) -> grb_time:ts().
min_global_matrix_ts(RemoteReplicas, SourceReplica, GlobalMatrix) ->
    min_global_matrix_ts(RemoteReplicas, SourceReplica, GlobalMatrix, undefined).

-spec min_global_matrix_ts([replica_id()], replica_id(), global_known_matrix(), grb_time:ts() | undefined) -> grb_time:ts().
min_global_matrix_ts([], _SourceReplica, _GlobalMatrix, Min) -> Min;
min_global_matrix_ts([RemoteReplica | Rest], SourceReplica, GlobalMatrix, Min) ->
    Ts = maps:get({RemoteReplica, SourceReplica}, GlobalMatrix, 0),
    min_global_matrix_ts(Rest, SourceReplica, GlobalMatrix, min_ts(Ts, Min)).

-spec min_ts(grb_time:ts(), grb_time:ts() | undefined) -> grb_time:ts().
min_ts(Left, undefined) -> Left;
min_ts(Left, Right) -> min(Left, Right).

%%%===================================================================
%%% Util Functions
%%%===================================================================

-spec clock_table(partition_id()) -> cache_id().
clock_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?PARTITION_CLOCK_TABLE}).

-spec is_ready(cache_id()) -> boolean().
is_ready(Table) ->
    undefined =/= ets:info(Table).

%%%===================================================================
%%% stub riak_core callbacks
%%%===================================================================

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handoff_starting(_, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_, State) ->
    {ok, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_data(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

delete(State=#state{clock_cache=ClockCache}) ->
    try ets:delete(ClockCache) catch _:_ -> ok end,
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.

-ifdef(TEST).

grb_propagation_vnode_compute_uniform_vc_test() ->
    Matrix = #{
        dc_id1 => #{dc_id1 => 2, dc_id2 => 2, dc_id3 => 1},
        dc_id2 => #{dc_id1 => 2, dc_id2 => 3, dc_id3 => 1},
        dc_id3 => #{dc_id1 => 1, dc_id2 => 2, dc_id3 => 2}
    },
    FGroups = [[dc_id1, dc_id2], [dc_id1, dc_id3]],
    UniformVC = compute_uniform_vc(grb_vclock:new(), Matrix, FGroups),
    ?assertEqual(#{dc_id1 => 2, dc_id2 => 2, dc_id3 => 1}, UniformVC).

grb_propagation_vnode_min_global_matrix_ts_test() ->
    DC1 = dc_id1, DC2 = dc_id2, DC3 = dc_id3,
    Matrix = #{
        {DC1, DC1} => 4,
        {DC1, DC2} => 5,
        {DC1, DC3} => 10,

        {DC2, DC1} => 4,
        {DC2, DC2} => 7,
        {DC2, DC3} => 12,

        {DC3, DC1} => 3,
        {DC3, DC2} => 5,
        {DC3, DC3} => 12
    },

    AllReplicas = [DC1, DC2, DC3],
    lists:foreach(fun(AtReplica) ->
        Remotes = AllReplicas -- [AtReplica],
        Mins = [ min_global_matrix_ts(Remotes, R, Matrix) || R <- AllReplicas ],
        case AtReplica of
            DC1 ->
                ?assertEqual([3, 5, 12], Mins);
            DC2 ->
                ?assertEqual([3, 5, 10], Mins);
            DC3 ->
                ?assertEqual([4, 5, 10], Mins)
        end
    end, AllReplicas).

grb_propagation_vnode_prune_commit_logs_test() ->
    DC1 = dc_id1, DC2 = dc_id2, DC3 = dc_id3,
    VClock = fun(R, N) -> grb_vclock:set_time(R, N, grb_vclock:new()) end,

    Matrix = #{
        {DC1, DC1} => 4,
        {DC1, DC2} => 5,
        {DC1, DC3} => 10,

        {DC2, DC1} => 4,
        {DC2, DC2} => 7,
        {DC2, DC3} => 12,

        {DC3, DC1} => 3,
        {DC3, DC2} => 5,
        {DC3, DC3} => 12
    },

    Logs = #{
        DC1 => grb_blue_commit_log:from_list(DC1, [{#{}, VClock(DC1, 3)}, {#{}, VClock(DC1, 4)}]),
        DC2 => grb_blue_commit_log:from_list(DC2, [{#{}, VClock(DC2, 2)}, {#{}, VClock(DC2, 5)}]),
        DC3 => grb_blue_commit_log:from_list(DC3, [{#{}, VClock(DC3, 10)}, {#{}, VClock(DC3, 12)}])
    },

    AllReplicas = [DC1, DC2, DC3],
    lists:foreach(fun(AtReplica) ->
        Remotes = AllReplicas -- [AtReplica],
        #{ DC1 := Log1 , DC2 := Log2, DC3 := Log3 } = maps:map(fun(R, CLog) ->
            MinTs = min_global_matrix_ts(Remotes, R, Matrix),
            grb_blue_commit_log:remove_leq(MinTs, CLog)
        end, Logs),
        case AtReplica of
            DC1 ->
                ?assertEqual([{#{}, VClock(DC1, 4)}], grb_blue_commit_log:get_bigger(0, Log1)),
                ?assertEqual([], grb_blue_commit_log:get_bigger(0, Log2)),
                ?assertEqual([], grb_blue_commit_log:get_bigger(0, Log3));
            DC2 ->
                ?assertEqual([{#{}, VClock(DC1, 4)}], grb_blue_commit_log:get_bigger(0, Log1)),
                ?assertEqual([], grb_blue_commit_log:get_bigger(0, Log2)),
                ?assertEqual([{#{}, VClock(DC3, 12)}], grb_blue_commit_log:get_bigger(0, Log3));
            DC3 ->
                ?assertEqual([], grb_blue_commit_log:get_bigger(0, Log1)),
                ?assertEqual([], grb_blue_commit_log:get_bigger(0, Log2)),
                ?assertEqual([{#{}, VClock(DC3, 12)}], grb_blue_commit_log:get_bigger(0, Log3))
        end
    end, AllReplicas).

grb_propagation_vnode_prune_remote_commit_logs_test() ->
    DC1 = dc_id1, DC2 = dc_id2, DC3 = dc_id3,
    VClock = fun(R, N) -> grb_vclock:set_time(R, N, grb_vclock:new()) end,

    Matrix = #{
        {DC1, DC1} => 4,
        {DC1, DC2} => 5,
        {DC1, DC3} => 10,

        {DC2, DC1} => 4,
        {DC2, DC2} => 7,
        {DC2, DC3} => 12,

        {DC3, DC1} => 3,
        {DC3, DC2} => 5,
        {DC3, DC3} => 12
    },

    CreateLogs = fun() -> #{
        DC1 => grb_remote_commit_log:from_list(DC1, [{#{}, VClock(DC1, 3)}, {#{}, VClock(DC1, 4)}]),
        DC2 => grb_remote_commit_log:from_list(DC2, [{#{}, VClock(DC2, 2)}, {#{}, VClock(DC2, 5)}]),
        DC3 => grb_remote_commit_log:from_list(DC3, [{#{}, VClock(DC3, 10)}, {#{}, VClock(DC3, 12)}])
    } end,

    DeleteLogs = fun(Logs) ->
        [ grb_remote_commit_log:delete(Log) || {_, Log} <- maps:to_list(Logs) ],
        ok
    end,

    AllReplicas = [DC1, DC2, DC3],
    lists:foreach(fun(AtReplica) ->
        Remotes = AllReplicas -- [AtReplica],
        Logs = CreateLogs(),

        ok = prune_remote_commit_logs(Remotes, Logs, Matrix),
        #{ DC1 := Log1 , DC2 := Log2, DC3 := Log3 } = Logs,
        case AtReplica of
            DC1 ->
                ?assertEqual([{#{}, VClock(DC1, 4)}], grb_remote_commit_log:get_bigger(0, Log1)),
                ?assertEqual([], grb_remote_commit_log:get_bigger(0, Log2)),
                ?assertEqual([], grb_remote_commit_log:get_bigger(0, Log3));
            DC2 ->
                ?assertEqual([{#{}, VClock(DC1, 4)}], grb_remote_commit_log:get_bigger(0, Log1)),
                ?assertEqual([], grb_remote_commit_log:get_bigger(0, Log2)),
                ?assertEqual([{#{}, VClock(DC3, 12)}], grb_remote_commit_log:get_bigger(0, Log3));
            DC3 ->
                ?assertEqual([], grb_remote_commit_log:get_bigger(0, Log1)),
                ?assertEqual([], grb_remote_commit_log:get_bigger(0, Log2)),
                ?assertEqual([{#{}, VClock(DC3, 12)}], grb_remote_commit_log:get_bigger(0, Log3))
        end,
        ok = DeleteLogs(Logs)
    end, AllReplicas).

-endif.
