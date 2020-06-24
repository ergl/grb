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
         broadcast_clocks/1,
         handle_blue_heartbeat/3,
         append_blue_commit/5,
         propagate_transactions/2]).

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
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-define(master, grb_propagation_vnode_master).

-type local_matrix() :: #{partition_id() => vclock()}.

-record(state, {
    partition :: partition_id(),
    %% todo(borja, uniformity): Change last_sent to globalKnownMatrix
    last_sent = 0 :: grb_time:ts(),
    logs = #{} :: #{replica_id() => grb_blue_commit_log:t()},
    local_known_matrix = #{} :: local_matrix(),
    clock_cache :: cache(atom(), vclock()),

    %% the partitions present at this cluster, kept to
    %% speed up localKnownMatrix computation
    cluster_partitions = [] :: [partition_id()],
    %% How ofter to broadcast our clocks to all partitions
    broadcast_clock_interval :: non_neg_integer(),
    broadcast_clock_timer = undefined :: timer:tref() | undefined
}).

%%%===================================================================
%%% public api
%%%===================================================================

-spec uniform_vc(partition_id()) -> vclock().
uniform_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), uniform_vc, 2).

-spec stable_vc(partition_id()) -> vclock().
stable_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), stable_vc, 2).

-spec update_stable_vc(partition_id(), vclock()) -> ok.
update_stable_vc(Partition, SVC) ->
    riak_core_vnode_master:command({Partition, node()}, {update_stable_vc, SVC}, ?master).

-spec known_vc(partition_id()) -> vclock().
known_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), known_vc, 2).

-spec handle_blue_heartbeat(partition_id(), replica_id(), grb_time:ts()) -> ok.
handle_blue_heartbeat(Partition, ReplicaId, Ts) ->
    riak_core_vnode_master:command({Partition, node()}, {blue_hb, ReplicaId, Ts}, ?master).

%% @doc Broadcast the clocks for the given partition to the rest of partitions in this cluster
-spec broadcast_clocks(partition_id()) -> ok.
broadcast_clocks(Partition) ->
    KnownVC = known_vc(Partition),
    grb_dc_utils:bcast_vnode_async(?master, {clock_event, Partition, KnownVC}).

-spec propagate_transactions(partition_id(), grb_time:ts()) -> ok.
propagate_transactions(Partition, KnownTime) ->
    riak_core_vnode_master:command({Partition, node()}, {propagate_tx, KnownTime}, ?master).

-spec append_blue_commit(replica_id(), partition_id(), term(), #{}, vclock()) -> ok.
append_blue_commit(ReplicaId, Partition, TxId, WS, CommitVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {append_blue, ReplicaId, TxId, WS, CommitVC},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, Interval}  = application:get_env(grb, broadcast_clock_interval),
    ClockTable = new_cache(Partition, ?PARTITION_CLOCK_TABLE),
    true = ets:insert(ClockTable, [{uniform_vc, grb_vclock:new()},
                                   {stable_vc, grb_vclock:new()},
                                   {known_vc, grb_vclock:new()}]),

    {ok, #state{partition=Partition,
                clock_cache=ClockTable,
                broadcast_clock_interval=Interval}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(start_broadcast_timer, _From, S=#state{partition=SelfPartition,
                                                      broadcast_clock_interval=Int,
                                                      broadcast_clock_timer=undefined}) ->

    {ok, TRef} = timer:apply_interval(Int, ?MODULE, broadcast_clocks, [SelfPartition]),
    {reply, ok, S#state{broadcast_clock_timer=TRef, cluster_partitions=grb_dc_utils:all_partitions()}};

handle_command(start_broadcast_timer, _From, S=#state{broadcast_clock_timer=_TRef}) ->
    {reply, ok, S};

handle_command(stop_broadcast_timer, _From, S=#state{broadcast_clock_timer=undefined}) ->
    {reply, ok, S};

handle_command(stop_broadcast_timer, _From, S=#state{broadcast_clock_timer=TRef}) ->
    {ok, cancel} = timer:cancel(TRef),
    {reply, ok, S#state{broadcast_clock_timer=undefined}};

handle_command({update_stable_vc, SVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, stable_vc, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, SVC),
    true = ets:update_element(ClockTable, stable_vc, {2, NewSVC}),
    {noreply, S};

handle_command({clock_event, FromPartition, KnownVC}, _Sender, S=#state{clock_cache=ClockTable,
                                                                        partition=LocalPartition,
                                                                        local_known_matrix=LocalMatrix,
                                                                        cluster_partitions=AllPartitions}) ->
    NewMatrix = LocalMatrix#{FromPartition => KnownVC},
    LocalReplica = grb_dc_utils:replica_id(),
    RemoteReplicas = grb_dc_connection_manager:connected_replicas(),
    SVC = compute_stable_vc(NewMatrix, LocalPartition, AllPartitions, [LocalReplica | RemoteReplicas]),
    true = ets:update_element(ClockTable, stable_vc, {2, SVC}),
    {noreply, S#state{local_known_matrix=NewMatrix}};

handle_command({blue_hb, FromReplica, Ts}, _Sender, S=#state{clock_cache=ClockTable}) ->
    ok = update_known_vc(FromReplica, Ts, ClockTable),
    {noreply, S};

handle_command({append_blue, ReplicaId, TxId, WS, CommitVC}, _Sender, S=#state{logs=Logs}) ->
    ReplicaLog = maps:get(ReplicaId, Logs, grb_blue_commit_log:new(ReplicaId)),
    {noreply, S#state{logs = Logs#{ReplicaId => grb_blue_commit_log:insert(TxId, WS, CommitVC, ReplicaLog)}}};

handle_command({propagate_tx, KnownTime}, _Sender, S=#state{clock_cache=ClockTable}) ->
    NewLogs = propagate_internal(KnownTime, S),
    ok = update_known_vc(KnownTime, ClockTable),
    %% todo(borja, uniformity): last_send should change to globalKnownMatrix
    {noreply, S#state{last_sent=KnownTime, logs=NewLogs}};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

%%%===================================================================
%%% internal functions
%%%===================================================================

-spec compute_stable_vc(local_matrix(), partition_id(), [partition_id()], [replica_id()]) -> vclock().
compute_stable_vc(LocalKnownMatrix, LocalPartition, AllPartitions, AllReplicas) ->
    Fresh = grb_vclock:new(),
    lists:foldl(fun(Partition, Acc) ->
        RVC = maps:get(Partition, LocalKnownMatrix, Fresh),
        grb_vclock:min_at(AllReplicas, RVC, Acc)
    end, maps:get(LocalPartition, LocalKnownMatrix, Fresh), AllPartitions).

%% todo(borja): Relay transactions from other replicas when we add uniformity
-spec propagate_internal(grb_time:ts(), #state{}) -> #{replica_id() => grb_blue_commit_log:t()}.
propagate_internal(LocalKnownTime, #state{partition=P, last_sent=LastSent, logs=Logs}) ->
    LocalId = grb_dc_utils:replica_id(),
    LocalLog = maps:get(LocalId, Logs, grb_blue_commit_log:new(LocalId)),
    {ToSend, NewLog} = grb_blue_commit_log:remove_bigger(LastSent, LocalLog),
    case ToSend of
        [] ->
            grb_dc_connection_manager:broadcast_heartbeat(LocalId, P, LocalKnownTime);
        Txs ->
            %% Entries are already ordered according to local commit time at this replica
            lists:foreach(fun(Tx) ->
                grb_dc_connection_manager:broadcast_tx(LocalId, P, Tx)
            end, Txs)
    end,
    Logs#{LocalId => NewLog}.

-spec update_known_vc(grb_time:ts(), cache(atom(), vclock())) -> ok.
update_known_vc(Time, ClockTable) ->
    update_known_vc(grb_dc_utils:replica_id(), Time, ClockTable).

-spec update_known_vc(replica_id(), grb_time:ts(), cache(atom(), vclock())) -> ok.
update_known_vc(ReplicaId, Time, ClockTable) ->
    Old = ets:lookup_element(ClockTable, known_vc, 2),
    New = grb_vclock:set_max_time(ReplicaId, Time, Old),
    true = ets:update_element(ClockTable, known_vc, {2, New}),
    ok.

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
            ?LOG_INFO("Unsable to create cache ~p at ~p, retrying", [Name, Partition]),
            timer:sleep(100),
            try ets:delete(CacheName) catch _:_ -> ok end,
            new_cache(Partition, Name, Options)
    end.

-spec cache_name(partition_id(), atom()) -> cache_id().
cache_name(Partition, Name) ->
    BinNode = atom_to_binary(node(), latin1),
    BiName = atom_to_binary(Name, latin1),
    BinPart = integer_to_binary(Partition),
    TableName = <<BiName/binary, <<"-">>/binary, BinPart/binary, <<"@">>/binary, BinNode/binary>>,
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

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.

-ifdef(TEST).
grb_propagation_vnode_compute_stable_vc_test() ->
    SelfPartition = p,
    Partitions = [p, q, r, s],
    Replicas = [dc_id1, dc_id2, dc_id3],

    EmptySVC = compute_stable_vc(#{}, SelfPartition, Partitions, Replicas),
    lists:foreach(fun(P) ->
        ?assertEqual(0, grb_vclock:get_time(P, EmptySVC))
    end, Partitions),

    Matrix0 = #{
        p => #{dc_id1 => 0, dc_id2 => 0, dc_id3 => 10},
        q => #{dc_id1 => 5, dc_id2 => 3, dc_id3 => 2},
        r => #{dc_id1 => 3, dc_id2 => 4, dc_id3 => 7},
        s => #{dc_id1 => 0, dc_id2 => 2, dc_id3 => 3}
    },

    ResultSVC = compute_stable_vc(Matrix0, SelfPartition, Partitions, Replicas),
    ?assertEqual(#{dc_id1 => 0, dc_id2 => 0, dc_id3 => 2}, ResultSVC).

-endif.
