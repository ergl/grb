-module(grb_SUITE).
-include("grb.hrl").
-include_lib("common_test/include/ct.hrl").

%% CT exports
-export([all/0,
         groups/0,
         init_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         end_per_suite/1]).

%% Test exports

-export([sanity_check_test/1,
         empty_read_test/1,
         read_your_writes_test/1,
         read_your_writes_red_test/1,
         monotonic_red_commit_timestamp/1,
         propagate_updates_test/1,
         replication_queue_flush_test/1,
         uniform_barrier_flush_test/1,
         transaction_ops_flush_test/1,
         abort_ops_flush_test/1,
         known_replicas_test/1,
         advance_clocks_test/1]).

-define(foreach_node(Map, Fun),
    lists:foreach(fun({Replica, #{nodes := Nodes}}) ->
        ok = lists:foreach(fun(N) -> Fun(Replica, N) end, Nodes)
    end, maps:to_list(Map))).

-define(random_key, crypto:strong_rand_bytes(64)).
-define(random_val, crypto:strong_rand_bytes(8)).
-define(random_key_map(__M), begin
    __L = maps:keys(__M),
    lists:nth(length(__L), __L)
end).

-define(repeat_test_limit, 100).

all() -> [{group, all_tests}].

groups() ->
    [
        {pure_operations,
            [parallel],
            [sanity_check_test, empty_read_test, known_replicas_test, advance_clocks_test]},

        {basic_operations,
            [sequence],
            [{group, pure_operations}, read_your_writes_test]},

        {single_dc,
            [sequence],
            [{group, basic_operations}]},

        {single_dc_red,
            [sequence, {repeat_until_ok, 100}],
            [
                read_your_writes_red_test,
                monotonic_red_commit_timestamp,
                transaction_ops_flush_test,
                abort_ops_flush_test
            ]},

        {multi_dc,
            [sequence],
            [{group, basic_operations}]},

        {replication,
            [sequence, {repeat_until_ok, 100}],
            [propagate_updates_test,
             replication_queue_flush_test,
             uniform_barrier_flush_test,
             transaction_ops_flush_test]},

        {all_tests,
            [sequence],
            [
                {group, single_dc},
                {group, single_dc_red},
                {group, multi_dc},
                {group, replication}
            ]}
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Setup / Teardown
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_per_suite(C) -> C.
end_per_suite(C) -> C.

init_per_group(single_node_dc, C) ->
    grb_utils:init_single_node_dc(?MODULE, C);

init_per_group(single_dc, C) ->
    grb_utils:init_single_dc(?MODULE, [dev1, dev2], C);

init_per_group(single_dc_red, C) ->
    C1 = grb_utils:init_single_dc(?MODULE, [dev1, dev2], C),
    ClusterMap = ?config(cluster_info, C1),
    Replica = random_replica(ClusterMap),
    Tx = tx_1,
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),

    Conflicts = #{<<1>> => <<1>>},
    ok = put_conflicts(Node, Conflicts),

    {ok, Val, CVC} = update_red_transaction(Node, Partition, Tx, <<1>>, Key, grb_crdt:make_op(grb_lww, Val), #{}),
    [ {propagate_info, {Tx, Key, Val, CVC}}, {red_conflicts, {Replica, Conflicts}} | C1 ];

init_per_group(multi_dc, C) ->
    grb_utils:init_multi_dc(?MODULE, [[clusterdev1, clusterdev2], [clusterdev3, clusterdev4]], C);

init_per_group(replication, C0) ->
    C1 = grb_utils:init_multi_dc(?MODULE, [[clusterdev1, clusterdev2], [clusterdev3, clusterdev4]], C0),
    ClusterMap = ?config(cluster_info, C1),
    Replica = random_replica(ClusterMap),
    Tx = tx_1,
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),
    CVC = update_transaction(Replica, Node, Partition, Tx, Key, grb_crdt:make_op(grb_lww, Val), #{}),
    [ {propagate_info, {Tx, Key, Val, CVC}} | C1 ];

init_per_group(_, C) ->
    C.

end_per_group(single_node_dc, C) ->
    [#{main_node := Node}] = maps:values(?config(cluster_info, C)),
    ok = grb_utils:stop_node(Node),
    C;

end_per_group(single_dc, C) ->
    ok = grb_utils:stop_clusters(?config(cluster_info, C)),
    C;

end_per_group(single_dc_red, C) ->
    ok = grb_utils:stop_clusters(?config(cluster_info, C)),
    C;

end_per_group(multi_dc, C) ->
    ok = grb_utils:stop_clusters(?config(cluster_info, C)),
    C;

end_per_group(replication, C) ->
    ok = grb_utils:stop_clusters(?config(cluster_info, C)),
    C;

end_per_group(_, C) -> C.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Every node has their replica id set, and knows about all other replicas
sanity_check_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    AllReplicas = lists:sort(maps:keys(ClusterMap)),
    ?foreach_node(ClusterMap, fun(ReplicaId, Node) ->
        ReplicaId = erpc:call(Node, grb_dc_manager, replica_id, []),
        AllReplicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        RingSize = grb_utils:ring_size(),
        {ok, ReplicaId, RingSize, _} = erpc:call(Node, grb, connect, [])
    end).

empty_read_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),
    {<<>>, _} = read_only_transaction(Node, Partition, tx_1, Key, grb_lww, #{}),
    {0, _} = read_only_operation_transaction(Node, Partition, tx_2, Key, grb_crdt:wrap_op(grb_gset, grb_gset:count_op()), #{}),
    {false, _} = read_only_operation_transaction(Node, Partition, tx_2, Key, grb_crdt:wrap_op(grb_gset, grb_gset:member_op(test)), #{}),
    {[], _} = read_only_operation_transaction(Node, Partition, tx_2, Key, grb_crdt:wrap_op(grb_gset, grb_gset:limit_op(10)), #{}).

read_your_writes_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),
    CVC = update_transaction(Replica, Node, Partition, tx_1, Key, grb_crdt:make_op(grb_lww, Val), #{}),
    {Val, _} = read_only_transaction(Node, Partition, tx_2, Key, grb_lww, CVC).

-ifndef(BLUE_KNOWN_VC).
read_your_writes_red_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {_, Conflicts} = ?config(red_conflicts, C),
    Label = ?random_key_map(Conflicts),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),
    {ok, Val, CVC} = update_red_transaction(Node, Partition, tx_1, Label, Key, grb_crdt:make_op(grb_lww, Val), #{}),
    {Val, _} = read_only_transaction(Node, Partition, tx_2, Key, grb_lww, CVC).
-else.
read_your_writes_red_test(_C) ->
    %% No red transactions
    ok.
-endif.

-ifndef(BLUE_KNOWN_VC).
monotonic_red_commit_timestamp(C) ->
    ClusterMap = ?config(cluster_info, C),
    {_, Conflicts} = ?config(red_conflicts, C),
    Label = ?random_key_map(Conflicts),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),

    TxId = {?FUNCTION_NAME, os:timestamp()},
    %% Commit time is 5 seconds into the future
    FakeTs = (grb_time:timestamp() + (5 * 1000000)),
    {ok, Val, #{ ?RED_REPLICA := CommitTs }} =
        update_red_transaction(
            Node,
            Partition,
            TxId,
            Label,
            Key,
            grb_crdt:make_op(grb_lww, Val),
            #{ ?RED_REPLICA => FakeTs }
        ),
    true = CommitTs > FakeTs,
    ok.
-else.
monotonic_red_commit_timestamp(_C) ->
    %% No red transactions
    ok.
-endif.

-ifndef(BASIC_REPLICATION).
propagate_updates_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {_, Key, Val, CommitVC} = ?config(propagate_info, C),
    foreach_replica(ClusterMap, fun(Replica) ->
        {Partition, Node} = key_location(Key, Replica, ClusterMap),
        ok = uniform_barrier(Replica, Node, Partition, CommitVC),
        {Val, _} = read_only_transaction(Node, Partition, tx_2, Key, grb_lww, CommitVC),
        ok
    end).
-else.
propagate_updates_test(_C) ->
    %% There's no reliable way of marking when an update has been replicated
    %% at another partition, uniform barrier won't help here.
    ok.
-endif.

replication_queue_flush_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {_, Key, _, _} = ?config(propagate_info, C),
    ok = foreach_replica(ClusterMap, fun(RemoteReplica) ->
        {Partition, Node} = key_location(Key, RemoteReplica, ClusterMap),
        CommitLog = erpc:call(Node, grb_propagation_vnode, get_commit_log, [Partition]),
        [] = grb_blue_commit_log:to_list(CommitLog),
        ok
    end).

uniform_barrier_flush_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {_, Key, _, CommitVC} = ?config(propagate_info, C),
    foreach_replica(ClusterMap, fun(Replica) ->
        {Partition, Node} = key_location(Key, Replica, ClusterMap),
        ok = uniform_barrier(Replica, Node, Partition, CommitVC),
        Barriers = erpc:call(Node, grb_propagation_vnode, get_uniform_barrier, [Partition]),
        [] = orddict:to_list(Barriers),
        ok
    end).

transaction_ops_flush_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {TxId, Key, _, _} = ?config(propagate_info, C),
    foreach_replica(ClusterMap, fun(Replica) ->
        {Partition, Node} = key_location(Key, Replica, ClusterMap),
        0 = erpc:call(Node, grb_oplog_vnode, transaction_ops, [Partition, TxId]),
        ok
    end).

-ifndef(BLUE_KNOWN_VC).
abort_ops_flush_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {_, Conflicts} = ?config(red_conflicts, C),
    Label = ?random_key_map(Conflicts),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),

    TxId1 = tx_1,
    Val1 = ?random_val,
    Operation1 = grb_crdt:make_op(grb_lww, Val1),

    TxId2 = tx_2,
    Val2 = ?random_val,
    Operation2 = grb_crdt:make_op(grb_lww, Val2),

    SVC1 = erpc:call(Node, grb, start_transaction, [Partition, #{}]),
    SVC2 = erpc:call(Node, grb, start_transaction, [Partition, #{}]),

    ok = erpc:call(Node, grb, update, [Partition, TxId1, Key, Operation1]),
    {ok, _} = erpc:call(Node, grb, sync_key_vsn, [Partition, TxId1, Key, grb_crdt:op_type(Operation1), SVC1]),

    ok = erpc:call(Node, grb, update, [Partition, TxId2, Key, Operation2]),
    {ok, _} = erpc:call(Node, grb, sync_key_vsn, [Partition, TxId1, Key, grb_crdt:op_type(Operation2), SVC2]),

    {ok, _} = erpc:call(Node, grb, sync_commit_red, [Partition, TxId1, Label, SVC1, [{Partition, [Key], #{Key => Operation1}}]]),
    {abort, _} = erpc:call(Node, grb, sync_commit_red, [Partition, TxId2, Label, SVC2, [{Partition, [Key], #{Key => Operation2}}]]),

    %% Aborted transactions are flushed immediately at the leader
    0 = erpc:call(Node, grb_oplog_vnode, transaction_ops, [Partition, TxId2]),

    %% Give time for TxId1 to be delivered
    timer:sleep(100),

    %% Committed transactions might be flushed out later, when they are made visible
    0 = erpc:call(Node, grb_oplog_vnode, transaction_ops, [Partition, TxId1]).
-else.
abort_ops_flush_test(_C) ->
    %% No red transactions
    ok.
-endif.

known_replicas_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    ?foreach_node(ClusterMap, fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = knows_replicas(Node, Partitions, Replicas, known_vc),
        ok = knows_replicas(Node, Partitions, Replicas, stable_vc),
        ok = knows_replicas(Node, Partitions, Replicas, uniform_vc)
    end).

-ifdef(STABLE_SNAPSHOT).
advance_clocks_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    TestFun = fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = advance_clock(Node, Partitions, Replicas, known_vc),
        ok = advance_clock(Node, Partitions, Replicas, stable_vc)
    end,
    advance_clocks_test(ClusterMap, TestFun, ?repeat_test_limit).
-else.
-ifdef(UNIFORM_SNAPSHOT).
advance_clocks_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    TestFun = fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = advance_clock(Node, Partitions, Replicas, known_vc),
        ok = advance_clock(Node, Partitions, Replicas, stable_vc),
        ok = advance_clock(Node, Partitions, Replicas, uniform_vc)
    end,
    advance_clocks_test(ClusterMap, TestFun, ?repeat_test_limit).
-else.
advance_clocks_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    TestFun = fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = advance_clock(Node, Partitions, [?RED_REPLICA | Replicas], known_vc),
        ok = advance_clock(Node, Partitions, Replicas, stable_vc),
        ok = advance_stable_red_entry(Node, Partitions),
        ok = advance_clock(Node, Partitions, Replicas, uniform_vc)
    end,
    advance_clocks_test(ClusterMap, TestFun, ?repeat_test_limit).

advance_stable_red_entry(Node, Partitions) ->
    lists:foreach(fun(P) ->
        Old = erpc:call(Node, grb_propagation_vnode, stable_red, [P]),
        timer:sleep(500),
        New = erpc:call(Node, grb_propagation_vnode, stable_red, [P]),
        true = (New > Old)
    end, Partitions).

-endif.
-endif.

advance_clocks_test(_ClusterMap, _TestFun, 0) ->
    error;
advance_clocks_test(ClusterMap, TestFun, Attempt) ->
    try
        ok = ?foreach_node(ClusterMap, TestFun)
    catch _:_ ->
        advance_clocks_test(ClusterMap, TestFun, Attempt - 1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

knows_replicas(Node, Partitions, Replicas, ClockName) ->
    lists:foreach(fun(P) ->
        Clock = erpc:call(Node, grb_propagation_vnode, ClockName, [P]),
        true = lists:all(fun(R) -> maps:is_key(R, Clock) end, Replicas)
    end, Partitions).

advance_clock(Node, Partitions, Replicas, ClockName) ->
    lists:foreach(fun(P) ->
        true = advance_clock_single(Node, P, Replicas, ClockName)
    end, Partitions).

advance_clock_single(Node, P, Replicas, ClockName) ->
    Old = erpc:call(Node, grb_propagation_vnode, ClockName, [P]),
    timer:sleep(500),
    New = erpc:call(Node, grb_propagation_vnode, ClockName, [P]),
    lists:all(fun(R) -> maps:get(R, Old) < maps:get(R, New) end, Replicas).

-spec uniform_barrier(replica_id(), node(), partition_id(), vclock()) -> ok.
uniform_barrier(_Replica, Node, Partition, Clock) ->
    ok = erpc:call(Node, grb, sync_uniform_barrier, [Partition, Clock]).

-spec read_only_transaction(node(), partition_id(), term(), key(), crdt(), vclock()) -> {snapshot(), vclock()}.
read_only_transaction(Node, Partition, TxId, Key, Type, Clock) ->
    SVC = erpc:call(Node, grb, start_transaction, [Partition, Clock]),
    {ok, Val} = erpc:call(Node, grb, sync_key_vsn, [Partition, TxId, Key, Type, SVC]),
    {Val, SVC}.

-spec read_only_operation_transaction(node(), partition_id(), term(), key(), operation(), vclock()) -> {term(), vclock()}.
read_only_operation_transaction(Node, Partition, TxId, Key, Operation, Clock) ->
    SVC = erpc:call(Node, grb, start_transaction, [Partition, Clock]),
    {ok, Val} = erpc:call(Node, grb, sync_key_operation, [Partition, TxId, Key, Operation, SVC]),
    {Val, SVC}.

-spec update_transaction(replica_id(), node(), partition_id(), term(), key(), operation(), vclock()) -> vclock().
update_transaction(Replica, Node, Partition, TxId, Key, Operation, Clock) ->
    SVC = erpc:call(Node, grb, start_transaction, [Partition, Clock]),
    ok = erpc:call(Node, grb, update, [Partition, TxId, Key, Operation]),
    {ok, _} = erpc:call(Node, grb, sync_key_vsn, [Partition, TxId, Key, grb_crdt:op_type(Operation), SVC]),
    PT = erpc:call(Node, grb, prepare_blue, [Partition, TxId, SVC]),
    CVC = SVC#{Replica => PT},
    ok = erpc:call(Node, grb, decide_blue, [Partition, TxId, CVC]),
    CVC.

-spec put_conflicts(node(), conflict_relations()) -> ok.
-ifndef(BLUE_KNOWN_VC).
put_conflicts(Node, Conflicts) ->
    erpc:call(Node, grb, put_conflicts, [Conflicts]).
-else.
put_conflicts(_Node, _Conflicts) ->
    ok.
-endif.

-spec update_red_transaction(node(), partition_id(), term(), tx_label(), key(), operation(), vclock()) -> {ok, term(), vclock()} | {error, term(), term()}.
-ifndef(BLUE_KNOWN_VC).
update_red_transaction(Node, Partition, TxId, Label, Key, Operation, Clock) ->
    SVC = erpc:call(Node, grb, start_transaction, [Partition, Clock]),
    ok = erpc:call(Node, grb, update, [Partition, TxId, Key, Operation]),
    {ok, Value} = erpc:call(Node, grb, sync_key_vsn, [Partition, TxId, Key, grb_crdt:op_type(Operation), SVC]),
    case erpc:call(Node, grb, sync_commit_red, [Partition, TxId, Label, SVC, [{Partition, [Key], #{Key => Operation}}]]) of
        {ok, CVC} ->
            {ok, Value, CVC};
        {abort, Reason} ->
            {error, Reason, Value}
    end.
-else.
update_red_transaction(_Node, _Partition, _TxId, _Label, _Key, Operation, Clock) ->
    Value = grb_crdt:value(grb_crdt:apply_op_raw(Operation, grb_crdt:new(grb_crdt:op_type(Operation)))),
    {ok, Value, Clock}.
-endif.

-spec random_replica(#{}) -> replica_id().
random_replica(ClusterMap) ->
    AllReplicas = maps:keys(ClusterMap),
    Size = length(AllReplicas),
    lists:nth(rand:uniform(Size), AllReplicas).

-spec key_location(key(), replica_id(), #{}) -> index_node().
key_location(Key, Replica, ClusterMap) ->
    #{Replica := #{main_node := Node}} = ClusterMap,
    erpc:call(Node, grb_dc_utils, key_location, [Key]).

-spec foreach_replica(#{}, fun((replica_id()) -> ok)) -> ok.
foreach_replica(Map, Fun) ->
    lists:foreach(fun({Replica, _}) ->
        ok = Fun(Replica)
    end, maps:to_list(Map)).
