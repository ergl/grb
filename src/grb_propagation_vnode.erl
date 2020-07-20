-module(grb_propagation_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API
-export([known_vc/1,
         stable_vc/1,
         update_stable_vc/2,
         uniform_vc/1,
         update_uniform_vc/2,
         handle_blue_heartbeat/3,
         handle_clock_update/4,
         handle_clock_heartbeat_update/4,
         append_blue_commit/6,
         register_uniform_barrier/3]).

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
              handle_info/2]).

-define(master, grb_propagation_vnode_master).

-define(prune_req, prune_event).
-define(replication_req, replication_event).
-define(uniform_req, uniform_replication_event).

-define(known_key, known_vc).
-define(stable_key, stable_vc).
-define(uniform_key, uniform_vc).

-type stable_matrix() :: #{replica_id() => vclock()}.
-type global_known_matrix() :: #{{replica_id(), replica_id()} => grb_time:ts()}.
-type blue_commit_logs() :: #{replica_id() => grb_blue_commit_log:t()}.
-type uniform_barriers() :: orddict:orddict(grb_time:ts(), [grb_promise:t()]).

-record(state, {
    partition :: partition_id(),
    local_replica :: replica_id(),

    logs = #{} :: blue_commit_logs(),
    global_known_matrix = #{} :: global_known_matrix(),

    %% send our transactions / heartbeats to remote replicas
    replication_interval :: non_neg_integer(),
    replication_timer = undefined :: reference() | undefined,

    %% relay transactions from other replicas
    uniform_interval :: non_neg_integer(),
    uniform_timer = undefined :: reference() | undefined,

    %% prune committedBlue
    prune_interval :: non_neg_integer(),
    prune_timer = undefined :: reference() | undefined,

    %% All groups with f+1 replicas that include ourselves
    fault_tolerant_groups = [] :: [[replica_id()]],
    stable_matrix = #{} :: stable_matrix(),

    %% It doesn't make sense to append it if we're not connected to other clusters
    should_append_commit = true :: boolean(),
    clock_cache :: cache(atom(), vclock()),

    %% List of pending uniform barriers by clients, recompute on uniformVC update
    pending_barriers = [] :: uniform_barriers()
}).

-type state() :: #state{}.

%%%===================================================================
%%% public api
%%%===================================================================

-spec uniform_vc(partition_id()) -> vclock().
uniform_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), ?uniform_key, 2).

-spec stable_vc(partition_id()) -> vclock().
stable_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), ?stable_key, 2).

-spec update_stable_vc(partition_id(), vclock()) -> ok.
update_stable_vc(Partition, SVC) ->
    riak_core_vnode_master:command({Partition, node()}, {update_stable_vc, SVC}, ?master).

-spec update_uniform_vc(partition_id(), vclock()) -> ok.
update_uniform_vc(Partition, SVC) ->
    riak_core_vnode_master:command({Partition, node()}, {update_uniform_vc, SVC}, ?master).

-spec known_vc(partition_id()) -> vclock().
known_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), ?known_key, 2).

-spec handle_blue_heartbeat(partition_id(), replica_id(), grb_time:ts()) -> ok.
handle_blue_heartbeat(Partition, ReplicaId, Ts) ->
    riak_core_vnode_master:command({Partition, node()}, {blue_hb, ReplicaId, Ts}, ?master).

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

-spec append_blue_commit(replica_id(), partition_id(), grb_time:ts(), term(), #{}, vclock()) -> ok.
append_blue_commit(ReplicaId, Partition, KnownTime, TxId, WS, CommitVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {append_blue, ReplicaId, KnownTime, TxId, WS, CommitVC},
                                   ?master).

-spec register_uniform_barrier(grb_promise:t(), partition_id(), grb_time:ts()) -> ok.
register_uniform_barrier(Promise, Partition, Timestamp) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {uniform_barrier, Promise, Timestamp},
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

    ClockTable = new_cache(Partition, ?PARTITION_CLOCK_TABLE),
    true = ets:insert(ClockTable, [{?uniform_key, grb_vclock:new()},
                                   {?stable_key, grb_vclock:new()},
                                   {?known_key, grb_vclock:new()}]),

    {ok, #state{partition=Partition,
                local_replica=undefined, % ok to do this, we'll overwrite it after join
                prune_interval=PruneInterval,
                replication_interval=ReplInt,
                uniform_interval=UniformInterval,
                clock_cache=ClockTable}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(enable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=true}};

handle_command(disable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=false}};

handle_command(learn_dc_id, _Sender, S) ->
    %% called after joining ring, this is now the correct id
    {reply, ok, S#state{local_replica=grb_dc_manager:replica_id()}};

handle_command({learn_dc_groups, MyGroups}, _From, S) ->
    %% called after connecting other replicas
    {reply, ok, S#state{fault_tolerant_groups=MyGroups}};

handle_command(populate_logs, _From, S=#state{logs=Logs0}) ->
    %% called after connecting other replicas
    %% populate log, avoid allocating on the replication path
    AllReplicas = grb_dc_manager:all_replicas(),
    Logs = lists:foldl(fun(Replica, LogAcc) ->
        LogAcc#{Replica => grb_blue_commit_log:new(Replica)}
    end, Logs0, AllReplicas),
    {reply, ok, S#state{logs=Logs}};

handle_command(start_propagate_timer, _From, State0) ->
    State = case timers_set(State0) of
        true -> State0;
        false ->
            State0#state{
                prune_timer=erlang:send_after(State0#state.prune_interval, self(), ?prune_req),
                uniform_timer=erlang:send_after(State0#state.uniform_interval, self(), ?uniform_req),
                replication_timer=erlang:send_after(State0#state.replication_interval, self(), ?replication_req)
            }
    end,
    {reply, ok, State};

handle_command(stop_propagate_timer, _From, State0) ->
    State = case timers_set(State0) of
        false -> State0;
        true ->
            erlang:cancel_timer(State0#state.prune_timer),
            erlang:cancel_timer(State0#state.uniform_timer),
            erlang:cancel_timer(State0#state.replication_timer),
            State0#state{
                prune_timer=undefined,
                uniform_timer=undefined,
                replication_timer=undefined
            }
    end,
    {reply, ok, State};

handle_command({update_stable_vc, SVC}, _Sender, S=#state{local_replica=LocalId,
                                                          clock_cache=ClockTable,
                                                          stable_matrix=StableMatrix0,
                                                          fault_tolerant_groups=Groups,
                                                          pending_barriers=PendingBarriers0}) ->

    OldSVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, SVC),
    true = ets:update_element(ClockTable, ?stable_key, {2, NewSVC}),
    {UniformVC, StableMatrix} = update_uniform_vc(LocalId, NewSVC, StableMatrix0, ClockTable, Groups),
    PendingBarriers = lift_pending_uniform_barriers(LocalId, UniformVC, PendingBarriers0),
    {noreply, S#state{stable_matrix=StableMatrix, pending_barriers=PendingBarriers}};

handle_command({update_uniform_vc, SVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, ?uniform_key, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, SVC),
    true = ets:update_element(ClockTable, ?uniform_key, {2, NewSVC}),
    {noreply, S};

handle_command({blue_hb, FromReplica, Ts}, _Sender, S=#state{clock_cache=ClockTable}) ->
    ok = update_known_vc(FromReplica, Ts, ClockTable),
    {noreply, S};

handle_command({remote_clock_update, FromReplicaId, KnownVC, StableVC}, _Sender, S) ->
    {noreply, update_clocks(FromReplicaId, KnownVC, StableVC, S)};

handle_command({remote_clock_heartbeat_update, FromReplicaId, KnownVC, StableVC}, _Sender, S=#state{clock_cache=ClockCache}) ->
    Timestamp = grb_vclock:get_time(FromReplicaId, KnownVC),
    ok = update_known_vc(FromReplicaId, Timestamp, ClockCache),
    {noreply, update_clocks(FromReplicaId, KnownVC, StableVC, S)};

handle_command({append_blue, ReplicaId, KnownTime, _TxId, _WS, _CommitVC}, _Sender, S=#state{clock_cache=ClockTable,
                                                                                             should_append_commit=false}) ->
    ok = update_known_vc(ReplicaId, KnownTime, ClockTable),
    {noreply, S};

handle_command({append_blue, ReplicaId, KnownTime, TxId, WS, CommitVC}, _Sender, S=#state{logs=Logs,
                                                                                          clock_cache=ClockTable,
                                                                                          should_append_commit=true})->
    ReplicaLog = maps:get(ReplicaId, Logs),
    ok = update_known_vc(ReplicaId, KnownTime, ClockTable),
    {noreply, S#state{logs = Logs#{ReplicaId => grb_blue_commit_log:insert(TxId, WS, CommitVC, ReplicaLog)}}};

handle_command({uniform_barrier, Promise, Timestamp}, _Sender, S=#state{pending_barriers=Barriers}) ->
    {noreply, S#state{pending_barriers=insert_uniform_barrier(Promise, Timestamp, Barriers)}};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_info(?replication_req, State=#state{partition=P,
                                           local_replica=LocalId,
                                           clock_cache=ClockTable,
                                           replication_timer=Timer,
                                           replication_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    KnownVC = get_updated_known_vc(LocalId, grb_main_vnode:get_known_time(P), ClockTable),
    StableVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    GlobalMatrix = replicate_internal(KnownVC, StableVC, State),
    {ok, State#state{global_known_matrix=GlobalMatrix,
                     replication_timer=erlang:send_after(Interval, self(), ?replication_req)}};

handle_info(?uniform_req, State=#state{partition=P,
                                       local_replica=LocalId,
                                       clock_cache=ClockTable,
                                       uniform_timer=Timer,
                                       uniform_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    KnownVC = get_updated_known_vc(LocalId, grb_main_vnode:get_known_time(P), ClockTable),
    StableVC = ets:lookup_element(ClockTable, ?stable_key, 2),
    GlobalMatrix = propagate_internal(KnownVC, StableVC, State),
    {ok, State#state{global_known_matrix=GlobalMatrix,
                     replication_timer=erlang:send_after(Interval, self(), ?uniform_req)}};

handle_info(?prune_req, State=#state{logs=Logs0,
                                     global_known_matrix=Matrix,
                                     prune_timer=Timer,
                                     prune_interval=Interval}) ->
    ?LOG_DEBUG("Running prune on logs"),
    erlang:cancel_timer(Timer),
    RemoteReplicas = grb_dc_manager:remote_replicas(),
    Logs = maps:map(fun(Replica, CommitLog) ->
        MinTs = min_global_matrix_ts(RemoteReplicas, Replica, Matrix),
        ?LOG_DEBUG("Min for replica ~p is ~p~n", [Replica, MinTs]),
        grb_blue_commit_log:remove_leq(MinTs, CommitLog)
    end, Logs0),
    {ok, State#state{logs=Logs, prune_timer=erlang:send_after(Interval, self(), ?prune_req)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("unhandled_info ~p", [Msg]),
    {ok, State}.

%%%===================================================================
%%% internal functions
%%%===================================================================

-spec timers_set(state()) -> boolean().
timers_set(#state{replication_timer=undefined, uniform_timer=undefined, prune_timer=undefined}) -> false;
timers_set(_) -> true.

-spec insert_uniform_barrier(grb_promise:t(), grb_time:ts(), uniform_barriers()) -> uniform_barriers().
insert_uniform_barrier(Promise, Timestamp, Barriers) ->
    case orddict:is_key(Timestamp, Barriers) of
        true -> orddict:append(Timestamp, Promise, Barriers);
        false -> orddict:store(Timestamp, [Promise], Barriers)
    end.

-spec lift_pending_uniform_barriers(replica_id(), vclock(), uniform_barriers()) -> uniform_barriers().
lift_pending_uniform_barriers(_, _, []) -> [];
lift_pending_uniform_barriers(ReplicaId, UniformVC, PendingBarriers) ->
    Timestamp = grb_vclock:get_time(ReplicaId, UniformVC),
    lift_pending_uniform_barriers(Timestamp, PendingBarriers).

-spec lift_pending_uniform_barriers(grb_time:ts(), uniform_barriers()) -> uniform_barriers().
lift_pending_uniform_barriers(_, []) -> [];

lift_pending_uniform_barriers(Cutoff, [{Ts, Promises} | Rest]) when Ts =< Cutoff ->
    lists:foreach(fun(P) -> grb_promise:resolve(ok, P) end, Promises),
    lift_pending_uniform_barriers(Cutoff, Rest);

lift_pending_uniform_barriers(Cutoff, [{Ts, _} | _]=Remaining) when Ts > Cutoff ->
    Remaining.

-spec update_clocks(replica_id(), vclock(), vclock(), state()) -> state().
update_clocks(FromReplicaId, KnownVC, StableVC, S=#state{local_replica=LocalId,
                                                         clock_cache=ClockCache,
                                                         stable_matrix=StableMatrix0,
                                                         fault_tolerant_groups=Groups,
                                                         global_known_matrix=KnownMatrix0,
                                                         pending_barriers=PendingBarriers0}) ->

    KnownMatrix = update_known_matrix(FromReplicaId, KnownVC, KnownMatrix0),
    {UniformVC, StableMatrix} = update_uniform_vc(FromReplicaId, StableVC, StableMatrix0, ClockCache, Groups),
    PendingBarriers = lift_pending_uniform_barriers(LocalId, UniformVC, PendingBarriers0),
    S#state{global_known_matrix=KnownMatrix,
            stable_matrix=StableMatrix,
            pending_barriers=PendingBarriers}.

-spec replicate_internal(vclock(), vclock(), #state{}) -> global_known_matrix().
replicate_internal(KnownVC, StableVC, #state{logs=Logs,
                                             partition=Partition,
                                             local_replica=LocalId,
                                             global_known_matrix=Matrix}) ->
    #{LocalId := LocalLog} = Logs,
    ConnectedReplicas = grb_dc_connection_manager:connected_replicas(),
    lists:foldl(fun(TargetReplica, MatrixAcc) ->
        ThresholdTime = maps:get({TargetReplica, LocalId}, MatrixAcc, 0),
        ?LOG_INFO("Treshold time for ~p: ~p~n", [TargetReplica, ThresholdTime]),
        ToSend = grb_blue_commit_log:get_bigger(ThresholdTime, LocalLog),
        case ToSend of
            [] ->
                %% piggy back clocks on top of the send_heartbeat message, avoid extra message on the wire
                HBRes = grb_dc_connection_manager:send_clocks_heartbeat(TargetReplica, LocalId, Partition, KnownVC, StableVC),
                ?LOG_DEBUG("broadcast clocks/heartbeat to ~p: ~p~n", [TargetReplica, HBRes]),
                ok;
            Txs ->
                %% can't merge with other messages here, send one before
                %% we could piggy-back on top of the first tx, but w/ever
                ClockRes = grb_dc_connection_manager:send_clocks(TargetReplica, LocalId, Partition, KnownVC, StableVC),
                ?LOG_DEBUG("broadcast clocks to ~p: ~p~n", [TargetReplica, ClockRes]),
                lists:foreach(fun(Tx) ->
                    TxRes = grb_dc_connection_manager:send_tx(TargetReplica, LocalId, Partition, Tx),
                    ?LOG_DEBUG("broadcast transaction ~p to ~p: ~p~n", [Tx, TargetReplica, TxRes]),
                    ok
                end, Txs)
        end,
        %% todo(borja, speed): since we always send up to the same value, extract from matrix into single counter / vc
        MatrixAcc#{{TargetReplica, LocalId} => grb_vclock:get_time(LocalId, KnownVC)}
    end, Matrix, ConnectedReplicas).

-spec propagate_internal(vclock(), vclock(), #state{}) -> global_known_matrix().
propagate_internal(KnownVC, StableVC, #state{local_replica=LocalId,
                                             partition=Partition,
                                             logs=Logs,
                                             global_known_matrix=Matrix}) ->

    AllReplicas = grb_dc_manager:all_replicas(),
    ConnectedReplicas = grb_dc_connection_manager:connected_replicas(),
    lists:foldl(fun(TargetReplica, GlobalMatrix) ->
        %% piggy-back on this loop to send our clocks
        %% todo(borja, speed): piggy-back on a blue heartbeat inside propagate_to when ReplayReplica = LocalId?
        Res = grb_dc_connection_manager:send_clocks(TargetReplica, LocalId, Partition, KnownVC, StableVC),
        ?LOG_DEBUG("send_clocks to ~p from ~p: ~p~n", [TargetReplica, LocalId, Res]),
        propagate_to(TargetReplica, AllReplicas, Partition, Logs, KnownVC, GlobalMatrix)
    end, Matrix, ConnectedReplicas).

%% @doc Propagate transactions / heartbeats to the target replica.
%%
%%      This will iterate over all the other connected replicas and fetch the
%%      necessary transactions/replicas to relay to the target replica.
%%
%%      In effect, we re-send transactions from other replicas to the target,
%%      to ensure that even if a replica goes down, if we received an update
%%      from it, other replicas will see it.
-spec propagate_to(Target :: replica_id(),
                   AllReplicas :: [replica_id()],
                   LocalPartition :: partition_id(),
                   Logs :: blue_commit_logs(),
                   KnownVC :: vclock(),
                   Matrix :: global_known_matrix()) -> global_known_matrix().

propagate_to(_TargetReplica, [], _Partition, _Logs, _KnownVC, MatrixAcc) ->
    MatrixAcc;
propagate_to(TargetReplica, [TargetReplica | Rest], Partition, Logs, KnownVC, MatrixAcc) ->
    %% skip ourselves
    propagate_to(TargetReplica, Rest, Partition, Logs, KnownVC, MatrixAcc);
propagate_to(TargetReplica, [RelayReplica | Rest], Partition, Logs, KnownVC, MatrixAcc) ->
    %% tx <- { <_, _, VC> \in log[relay] | VC[relay] > globalMatrix[target, relay] }
    %% if tx =/= \emptyset
    %%     for all t \in tx (in t.VC[relay] order)
    %%         send REPLICATE(relay, t) to target
    %% else
    %%     send HEARTBEAT(relay, knownVC[relay]) to target
    %% globalMatrix[target, relay] <- knownVC[relay]
    RelayKnownTime = grb_vclock:get_time(RelayReplica, KnownVC),
    Log = maps:get(RelayReplica, Logs),
    LastSent = maps:get({TargetReplica, RelayReplica}, MatrixAcc, 0),
    ToSend = grb_blue_commit_log:get_bigger(LastSent, Log),
    case ToSend of
        [] ->
            HBRes = grb_dc_connection_manager:send_heartbeat(TargetReplica, RelayReplica, Partition, RelayKnownTime),
            ?LOG_DEBUG("blue_hb to ~p from ~p: ~p~n", [TargetReplica, RelayReplica, HBRes]),
            ok;
        Txs ->
            %% Entries are already ordered to commit time at the replica of the log
            lists:foreach(fun(Tx) ->
                TxRes = grb_dc_connection_manager:send_tx(TargetReplica, RelayReplica, Partition, Tx),
                ?LOG_DEBUG("replicate to ~p from ~p: ~p~n", [TargetReplica, RelayReplica, TxRes]),
                ok
            end, Txs)
    end,
    NewMatrix = MatrixAcc#{{TargetReplica, RelayReplica} => RelayKnownTime},
    propagate_to(TargetReplica, Rest, Partition, Logs, KnownVC, NewMatrix).

%% @doc Set knownVC[ReplicaId] <-max- Time
-spec update_known_vc(replica_id(), grb_time:ts(), cache(atom(), vclock())) -> ok.
update_known_vc(ReplicaId, Time, ClockTable) ->
    Old = ets:lookup_element(ClockTable, ?known_key, 2),
    New = grb_vclock:set_max_time(ReplicaId, Time, Old),
    true = ets:update_element(ClockTable, ?known_key, {2, New}),
    ok.

%% @doc Same as update_known_vc/3, but return resulting knownVC
-spec get_updated_known_vc(replica_id(), grb_time:ts(), cache(atom(), vclock())) -> vclock().
get_updated_known_vc(ReplicaId, Time, ClockTable) ->
    Old = ets:lookup_element(ClockTable, ?known_key, 2),
    New = grb_vclock:set_max_time(ReplicaId, Time, Old),
    true = ets:update_element(ClockTable, ?known_key, {2, New}),
    New.

-spec update_known_matrix(replica_id(), vclock(), global_known_matrix()) -> global_known_matrix().
update_known_matrix(FromReplicaId, KnownVC, Matrix) ->
    %% globalKnownMatrix[FromReplicaId] <- KnownVC,
    %% transformed into globalKnownMatrix[FromReplicaId][j] <- knownVC[j]
    lists:foldl(fun({AtReplica, Ts}, Acc) ->
        Acc#{{FromReplicaId, AtReplica} => max(Ts, maps:get({FromReplicaId, AtReplica}, Acc, 0))}
    end, Matrix, grb_vclock:to_list(KnownVC)).

-spec update_uniform_vc(From :: replica_id(),
                        StableVC :: vclock(),
                        StableMatrix :: stable_matrix(),
                        ClockCache :: cache(atom(), vclock()),
                        Groups :: [[replica_id()]]) -> {vclock(), stable_matrix()}.

update_uniform_vc(FromReplicaId, StableVC, StableMatrix0, ClockCache, Groups) ->
    StableMatrix = StableMatrix0#{FromReplicaId => StableVC},
    UniformVC0 = ets:lookup_element(ClockCache, ?uniform_key, 2),
    UniformVC = compute_uniform_vc(UniformVC0, StableMatrix, Groups),
    true = ets:update_element(ClockCache, ?uniform_key, {2, UniformVC}),
    {UniformVC, StableMatrix}.

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

-spec new_cache(partition_id(), atom()) -> cache_id().
new_cache(Partition, Name) ->
    new_cache(Partition, Name, [set, protected, named_table, {read_concurrency, true}]).

new_cache(Partition, Name, Options) ->
    CacheName = cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            ?LOG_INFO("Unable to create cache ~p at ~p, retrying", [Name, Partition]),
            timer:sleep(100),
            try ets:delete(CacheName) catch _:_ -> ok end,
            new_cache(Partition, Name, Options)
    end.

-spec cache_name(partition_id(), atom()) -> cache_id().
cache_name(Partition, Name) ->
    BinNode = atom_to_binary(node(), latin1),
    BinName = atom_to_binary(Name, latin1),
    BinPart = integer_to_binary(Partition),
    TableName = <<BinName/binary, <<"-">>/binary, BinPart/binary, <<"@">>/binary, BinNode/binary>>,
    safe_bin_to_atom(TableName).

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

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

terminate(_Reason, #state{clock_cache=ClockCache}) ->
    try ets:delete(ClockCache) catch _:_ -> ok end,
    ok.

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
    Matrix = #{
        {dc_id1, dc_id1} => 2,
        {dc_id1, dc_id2} => 2,
        {dc_id1, dc_id3} => 1,

        {dc_id2, dc_id1} => 2,
        {dc_id2, dc_id2} => 3,
        {dc_id2, dc_id3} => 1,

        {dc_id3, dc_id1} => 1,
        {dc_id3, dc_id2} => 2,
        {dc_id3, dc_id3} => 2
    },

    RemoteReplicas = [dc_id2, dc_id3],

    MintAt1 = min_global_matrix_ts(RemoteReplicas, dc_id1, Matrix),
    ?assertEqual(1, MintAt1),

    MintAt2 = min_global_matrix_ts(RemoteReplicas, dc_id2, Matrix),
    ?assertEqual(2, MintAt2),

    MintAt3 = min_global_matrix_ts(RemoteReplicas, dc_id3, Matrix),
    ?assertEqual(1, MintAt3).

-endif.
