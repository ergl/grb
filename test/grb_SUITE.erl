-module(grb_SUITE).
-include_lib("common_test/include/ct.hrl").

%% CT exports
-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).

%% Test exports

-export([test/1]).

all() -> [test].

init_per_suite(C) ->
    grb_utils:init_single_dc(?MODULE, C).

end_per_suite(C) ->
    #{node := Node} = ?config(cluster_info, C),
    grb_utils:kill_node(Node),
    C.

test(C) ->
    #{node := Node} = ?config(cluster_info, C),
    NodeSpec = [ {P, Node} || P <- erpc:call(Node, grb_dc_utils, my_partitions, [])],
    {ok, _, 64, NodeSpec} = erpc:call(Node, grb, connect, []).
