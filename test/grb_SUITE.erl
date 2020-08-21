-module(grb_SUITE).
-include_lib("common_test/include/ct.hrl").

%% CT exports
-export([all/0,
         groups/0,
         init_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         end_per_suite/1]).

%% Test exports

-export([basic_test/1,
         basic_dc_test/1,
         known_replicas_test/1,
         advance_clocks_test/1]).

-define(foreach_node(Map, Fun),
    lists:foreach(fun({Replica, #{nodes := Nodes}}) ->
        ok = lists:foreach(fun(N) -> Fun(Replica, N) end, Nodes)
    end, maps:to_list(Map))).

all() -> [{group, multi_dc}].

groups() ->
    [
        {all_tests, [sequence], [{group, single_node_dc}, {group, single_dc}, {group, multi_dc}]},
        {single_node_dc, [sequence], [basic_test]},
        {single_dc, [sequence], [basic_dc_test]},
        {multi_dc, [parallel, {repeat_until_ok, 100}], [known_replicas_test, advance_clocks_test]}
    ].

init_per_suite(C) -> C.
end_per_suite(C) -> C.

init_per_group(single_node_dc, C) ->
    grb_utils:init_single_node_dc(?MODULE, C);

init_per_group(single_dc, C) ->
    grb_utils:init_single_dc(?MODULE, [dev1, dev2], C);

init_per_group(multi_dc, C) ->
    grb_utils:init_multi_dc(?MODULE, [[clusterdev1, clusterdev2], [clusterdev3, clusterdev4]], C);

init_per_group(_, C) ->
    C.

end_per_group(single_node_dc, C) ->
    [#{main_node := Node}] = maps:values(?config(cluster_info, C)),
    ok = grb_utils:stop_node(Node),
    C;

end_per_group(single_dc, C) ->
    ok = grb_utils:stop_clusters(?config(cluster_info, C)),
    C;

end_per_group(multi_dc, C) ->
    ok = grb_utils:stop_clusters(?config(cluster_info, C)),
    C;

end_per_group(_, C) -> C.

basic_test(C) ->
    [#{main_node := Node}] = maps:values(?config(cluster_info, C)),
    ReplicaId = erpc:call(Node, grb_dc_manager, replica_id, []),
    NodeSpec = [ {P, Node} || P <- erpc:call(Node, grb_dc_utils, my_partitions, [])],
    {ok, ReplicaId, 64, NodeSpec} = erpc:call(Node, grb, connect, []).

basic_dc_test(C) ->
    ClusterMap = ?config(cluster_info, C),
    ?foreach_node(ClusterMap, fun(ReplicaId, Node) ->
        ReplicaId = erpc:call(Node, grb_dc_manager, replica_id, []),
        {ok, ReplicaId, 64, _} = erpc:call(Node, grb, connect, [])
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
