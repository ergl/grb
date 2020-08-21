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
         basic_dc_test/1]).

all() -> [{group, all_tests}].

groups() ->
    [
        {all_tests, [sequence], [{group, single_node_dc}, {group, single_dc}]},
        {single_node_dc, [sequence], [basic_test]},
        {single_dc, [sequence], [basic_dc_test]}
    ].

init_per_suite(C) -> C.
end_per_suite(C) -> C.

init_per_group(single_node_dc, C) ->
    grb_utils:init_single_node_dc(?MODULE, C);

init_per_group(single_dc, C) ->
    grb_utils:init_single_dc(?MODULE, [dev1, dev2], C);

init_per_group(_, C) ->
    C.

end_per_group(single_node_dc, C) ->
    [#{main_node := Node}] = maps:values(?config(cluster_info, C)),
    ok = grb_utils:stop_node(Node),
    C;

end_per_group(single_dc, C) ->
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
    ok = lists:foreach(fun({ReplicaId, #{nodes := Nodes}}) ->
        ok = lists:foreach(fun(Node) ->
            ReplicaId = erpc:call(Node, grb_dc_manager, replica_id, []),
            {ok, ReplicaId, 64, _} = erpc:call(Node, grb, connect, [])
        end, Nodes)
    end, maps:to_list(ClusterMap)).
