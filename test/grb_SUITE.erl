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
         propagate_updates_test/1,
         replication_queue_flush_test/1,
         uniform_barrier_flush_test/1,
         known_replicas_test/1,
         advance_clocks_test/1]).

-define(foreach_node(Map, Fun),
    lists:foreach(fun({Replica, #{nodes := Nodes}}) ->
        ok = lists:foreach(fun(N) -> Fun(Replica, N) end, Nodes)
    end, maps:to_list(Map))).

-define(random_key, crypto:strong_rand_bytes(64)).
-define(random_val, crypto:strong_rand_bytes(256)).

all() -> [{group, all_tests}].

groups() ->
    [
        {pure_operations,
            [parallel, {repeat_until_ok, 100}],
            [sanity_check_test, empty_read_test, known_replicas_test, advance_clocks_test]},

        {basic_operations,
            [sequence],
            [{group, pure_operations}, read_your_writes_test]},

        {single_dc,
            [sequence],
            [{group, basic_operations}]},

        %% todo(borja, red): Add tests here
        {single_dc_red,
            [sequence, {repeat_until_ok, 100}],
            []},

        {multi_dc,
            [sequence],
            [{group, basic_operations}]},

        {replication,
            [sequence, {repeat_until_ok, 100}],
            [propagate_updates_test, replication_queue_flush_test, uniform_barrier_flush_test]},

        {all_tests,
            [sequence],
            [ {group, single_dc},
              %% {group, single_dc_red},
              {group, multi_dc},
              {group, replication}] }
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
    grb_utils:init_single_dc(?MODULE, [dev1, dev2], C);

init_per_group(multi_dc, C) ->
    grb_utils:init_multi_dc(?MODULE, [[clusterdev1, clusterdev2], [clusterdev3, clusterdev4]], C);

init_per_group(replication, C0) ->
    C1 = grb_utils:init_multi_dc(?MODULE, [[clusterdev1, clusterdev2], [clusterdev3, clusterdev4]], C0),
    ClusterMap = ?config(cluster_info, C1),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),
    CVC = update_transaction(Replica, Node, Partition, Key, Val, #{}),
    [ {propagate_info, {Key, Val, CVC}} | C1 ];

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
    {<<>>, _} = read_only_transaction(Replica, Node, Partition, Key, #{}).

read_your_writes_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replica = random_replica(ClusterMap),
    Key = ?random_key,
    Val = ?random_val,
    {Partition, Node} = key_location(Key, Replica, ClusterMap),
    CVC = update_transaction(Replica, Node, Partition, Key, Val, #{}),
    {Val, _} = read_only_transaction(Replica, Node, Partition, Key, CVC).

-ifndef(BASIC_REPLICATION).
propagate_updates_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {Key, Val, CommitVC} = ?config(propagate_info, C),
    foreach_replica(ClusterMap, fun(Replica) ->
        {Partition, Node} = key_location(Key, Replica, ClusterMap),
        ok = uniform_barrier(Replica, Node, Partition, CommitVC),
        {Val, _} = read_only_transaction(Replica, Node, Partition, Key, CommitVC),
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
    {Key, _, _} = ?config(propagate_info, C),
    ok = foreach_replica(ClusterMap, fun(RemoteReplica) ->
        {Partition, Node} = key_location(Key, RemoteReplica, ClusterMap),
        CommitLog = erpc:call(Node, grb_propagation_vnode, get_commit_log, [Partition]),
        [] = grb_blue_commit_log:to_list(CommitLog),
        ok
    end).

uniform_barrier_flush_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    {Key, _, CommitVC} = ?config(propagate_info, C),
    foreach_replica(ClusterMap, fun(Replica) ->
        {Partition, Node} = key_location(Key, Replica, ClusterMap),
        ok = uniform_barrier(Replica, Node, Partition, CommitVC),
        Barriers = erpc:call(Node, grb_propagation_vnode, get_uniform_barrier, [Partition]),
        [] = orddict:to_list(Barriers),
        ok
    end).

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

-ifdef(BASIC_REPLICATION).
advance_clocks_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    ?foreach_node(ClusterMap, fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = advance_clock(Node, Partitions, Replicas, known_vc),
        ok = advance_clock(Node, Partitions, Replicas, stable_vc)
    end).
-else.
-ifdef(UNIFORM_BLUE).
advance_clocks_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    ?foreach_node(ClusterMap, fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = advance_clock(Node, Partitions, Replicas, known_vc),
        ok = advance_clock(Node, Partitions, Replicas, stable_vc),
        ok = advance_clock(Node, Partitions, Replicas, uniform_vc)
    end).
-else.
advance_clocks_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    Replicas = lists:sort(maps:keys(ClusterMap)),
    ?foreach_node(ClusterMap, fun(_, Node) ->
        Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
        Replicas = lists:sort(erpc:call(Node, grb_dc_manager, all_replicas, [])),
        ok = advance_clock(Node, Partitions, [?RED_REPLICA | Replicas], known_vc),
        ok = advance_clock(Node, Partitions, Replicas, stable_vc),
        ok = advance_stable_red_entry(Node, Partitions),
        ok = advance_clock(Node, Partitions, Replicas, uniform_vc)
    end).

advance_stable_red_entry(Node, Partitions) ->
    lists:foreach(fun(P) ->
        Old = erpc:call(Node, grb_propagation_vnode, stable_red, [P]),
        timer:sleep(500),
        New = erpc:call(Node, grb_propagation_vnode, stable_red, [P]),
        true = (New > Old)
    end, Partitions).

-endif.
-endif.

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

-spec read_only_transaction(replica_id(), node(), partition_id(), key(), vclock()) -> {val(), vclock()}.
read_only_transaction(_Replica, Node, Partition, Key, Clock) ->
    SVC = erpc:call(Node, grb, start_transaction, [Partition, Clock]),
    {ok, Val} = erpc:call(Node, grb, sync_perform_op, [Partition, Key, SVC, <<>>]),
    {Val, SVC}.

-spec update_transaction(replica_id(), node(), partition_id(), key(), val(), vclock()) -> vclock().
update_transaction(Replica, Node, Partition, Key, Value, Clock) ->
    SVC = erpc:call(Node, grb, start_transaction, [Partition, Clock]),
    {ok, Value} = erpc:call(Node, grb, sync_perform_op, [Partition, Key, SVC, Value]),
    PT = erpc:call(Node, grb, prepare_blue, [Partition, ignore, #{Key => Value}, SVC]),
    CVC = SVC#{Replica => PT},
    ok = erpc:call(Node, grb, decide_blue, [Partition, ignore, CVC]),
    CVC.

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
