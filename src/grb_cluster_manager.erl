-module(grb_cluster_manager).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([create_cluster/2]).

%% All functions are called through erpc
-ignore_xref([create_cluster/2]).

-spec create_cluster([node()], non_neg_integer()) -> ok | {error, term()}.
create_cluster(Nodes, TreeFanout) ->
    case riak_core_ring:ring_ready() of
        true ->
            join_nodes(Nodes, TreeFanout);
        false ->
            {error, ring_not_ready}
    end.

join_nodes(N=[SingleNode], Fanout) ->
    ?LOG_INFO("Checking that vnodes are ready...~n"),
    ok = wait_until_vnodes_ready(SingleNode),
    ?LOG_INFO("Node ready, starting background processes"),
    erpc:call(SingleNode, grb_dc_manager, start_background_processes, []),
    ?LOG_INFO("starting broadcast tree~n"),
    ok = start_broadcast_tree(N, Fanout),
    ok = wait_until_master_ready(SingleNode),
    ?LOG_INFO("Successfully joined nodes ~p", [N]),
    ok;

join_nodes([MainNode | _] = Nodes, Fanout) ->
    lists:foreach(fun(N) -> erlang:set_cookie(N, grb_cookie) end, Nodes),
    ok = join_cluster(Nodes),

    ?LOG_INFO("Starting background processes"),
    ok = start_background_processes(MainNode),

    ?LOG_INFO("Started background processes, starting broadcast tree"),
    ok = start_broadcast_tree(Nodes, Fanout),

    ?LOG_INFO("Started broadcast tree, checking master ready"),
    ok = wait_until_master_ready(MainNode),

    ?LOG_INFO("Successfully joined nodes ~p", [Nodes]),
    ok.

-spec start_background_processes(node()) -> ok.
start_background_processes(Node) ->
    BackgroundReady = fun() ->
        ?LOG_INFO("start_background_processes"),
        try
            ok = erpc:call(Node, grb_dc_manager, start_background_processes, []),
            true
        catch _:_ ->
            ?LOG_INFO("Error on start_background_processes, retrying"),
            false
        end
    end,
    wait_until(BackgroundReady).

%% @doc Build clusters out of the given node list
-spec join_cluster(list(atom())) -> ok | {error, node(), term()}.
join_cluster([MainNode | OtherNodes] = Nodes) ->
    case check_nodes_own_their_ring(Nodes) of
        {error, FaultyNode, Reason} ->
            ?LOG_ERROR("Bad node ~s on ownership check with reason ~p", [FaultyNode, Reason]),
            {error, FaultyNode, Reason};
        ok ->
            %% Do a plan/commit staged join, instead of sequential joins
            ok = lists:foreach(fun(N) -> request_join(N, MainNode) end, OtherNodes),
            ok = wait_plan_ready(MainNode),
            ok = commit_plan(MainNode),
            ok = try_cluster_ready(Nodes),

            ok = wait_until_nodes_ready(Nodes),

            %% Ensure each node owns a portion of the ring
            ok = wait_until_nodes_agree_about_ownership(Nodes),
            ok = wait_until_no_pending_changes(Nodes),
            ok = wait_until_ring_converged(Nodes)
    end.

%% @doc Retrieve a list of ring-owning physical nodes according to the MainNode
%%
%%      A node is ring-owning if a partition is stored on it
%%
-spec sorted_ring_owners(node()) -> {ok, list(node())} | {badrpc, term()}.
sorted_ring_owners(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- rpc:call(Node, riak_core_ring, all_owners, [Ring])],
            SortedOwners = lists:usort(Owners),
            ?LOG_INFO("Owners at ~p: ~p", [Node, SortedOwners]),
            {ok, SortedOwners};

        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Ensure that all nodes are the sole owner of their rings
-spec check_nodes_own_their_ring(list(atom())) -> ok | {error, node(), term()}.
check_nodes_own_their_ring([]) -> ok;
check_nodes_own_their_ring([H | T]) ->
    case sorted_ring_owners(H) of
        {ok, [H]} ->
            check_nodes_own_their_ring(T);
        Reason ->
            {error, H, Reason}
    end.

%% @doc Make `Node` request joining with `MasterNode`
-spec request_join(node(), node()) -> ok.
request_join(Node, MasterNode) ->
    timer:sleep(5000),
    R = rpc:call(Node, riak_core, staged_join, [MasterNode]),
    ?LOG_INFO("[join request] ~p to ~p: (result ~p)", [Node, MasterNode, R]),
    ok.

-spec wait_plan_ready(node()) -> ok.
wait_plan_ready(Node) ->
    ?LOG_INFO("[ring plan] Will start plan on ~p", [Node]),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            ?LOG_INFO("[ring plan] Ring not ready, retrying..."),
            timer:sleep(5000),
            ok = wait_until_no_pending_changes(Node),
            wait_plan_ready(Node);

        {ok, _, _} ->
            ok
    end.

-spec commit_plan(node()) -> ok.
commit_plan(Node) ->
    ?LOG_INFO("[ring commit] Will start commit on ~p", [Node]),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            ?LOG_INFO("[ring commit] Plan changed, retrying..."),
            timer:sleep(100),
            ok = wait_until_no_pending_changes(Node),
            ok = wait_plan_ready(Node),
            commit_plan(Node);

        {error, ring_not_ready} ->
            ?LOG_INFO("[ring commit] Ring not ready, retrying..."),
            timer:sleep(100),
            wait_until_no_pending_changes(Node),
            commit_plan(Node);

        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;

        ok ->
            ok
    end.

%% @doc Wait until all nodes agree about ready nodes in their rings
-spec try_cluster_ready([node()]) -> ok.
try_cluster_ready(Nodes) ->
    try_cluster_ready(Nodes, 3, 500).

-spec try_cluster_ready([node()], non_neg_integer(), non_neg_integer()) -> ok.
try_cluster_ready([MainNode | _] = _Nodes, 0, _SleepMs) ->
    ?LOG_INFO("[cluster ready] Still not ready, will retry plan"),
    ok = wait_plan_ready(MainNode),
    commit_plan(MainNode);

try_cluster_ready([MainNode | _] = Nodes, Retries, SleepMs) ->
    AllReady = lists:all(fun(Node) -> is_ready(Node, MainNode) end, Nodes),
    case AllReady of
        true ->
            ok;
        false ->
            timer:sleep(SleepMs),
            try_cluster_ready(Nodes, Retries - 1, SleepMs)
    end.

%% @doc Check if `Node` is ready according to `MainNode`
%% @private
-spec is_ready(node(), node()) -> boolean().
is_ready(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            ReadyMembers = rpc:call(MainNode, riak_core_ring, ready_members, [Ring]),
            lists:member(Node, ReadyMembers);

        _ ->
            false
    end.

%% @doc Given a list of nodes, wait until all nodes believe there are no
%%      on-going or pending ownership transfers.
%%
-spec wait_until_no_pending_changes([node()] | node()) -> ok | fail.
wait_until_no_pending_changes([MainNode | _] = Nodes) when is_list(Nodes) ->
    ?LOG_INFO("~p", [?FUNCTION_NAME]),
    NoPendingHandoffs = fun() ->
        rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
        {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
        ?LOG_INFO("Check no pending handoffs (badnodes: ~p)...", [BadNodes]),
        case BadNodes of
            [] ->
                Res = lists:all(fun({ok, Ring}) ->
                    [] =:= rpc:call(MainNode, riak_core_ring, pending_changes, [Ring])
                end, Rings),
                ?LOG_INFO("All nodes with no pending changes: ~p", [Res]),
                Res;
            _ ->
                false
        end
    end,

    wait_until(NoPendingHandoffs);

wait_until_no_pending_changes(Node) ->
    wait_until_no_pending_changes([Node]).

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%
%%      See {@link wait_until_ready/1} for definition of ready.
%%
-spec wait_until_nodes_ready([node()]) -> ok.
wait_until_nodes_ready([MainNode | _] = Nodes) ->
    true = lists:all(fun(Node) ->
        case wait_until(fun() -> is_ready(Node, MainNode) end) of
            ok ->
                true;
            Res ->
                ?LOG_INFO("wait_until_nodes_ready got ~p", [Res]),
                false
        end
                     end, Nodes),
    ok.

%% @doc Wait until all nodes agree about all ownership views
-spec wait_until_nodes_agree_about_ownership([node()]) -> ok.
wait_until_nodes_agree_about_ownership(Nodes) ->
    ?LOG_INFO("~p", [?FUNCTION_NAME]),
    SortedNodes = lists:usort(Nodes),
    true = lists:all(fun(Node) ->
        Res = wait_until(fun() ->
            case sorted_ring_owners(Node) of
                {ok, SortedNodes} ->
                    true;
                _ ->
                    false
            end
                         end),
        case Res of
            ok ->
                true;
            Res ->
                ?LOG_INFO("wait_until_nodes_agree_about_ownership got ~p", [Res]),
                false
        end
    end, Nodes),
    ok.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
-spec wait_until_ring_converged([node()]) -> ok.
wait_until_ring_converged([MainNode | _] = Nodes) ->
    ?LOG_INFO("~p", [?FUNCTION_NAME]),
    true = lists:all(fun(Node) ->
        case wait_until(fun() -> is_ring_ready(Node, MainNode) end) of
            ok ->
                true;
            Res ->
                ?LOG_INFO("wait_until_ring_converged got ~p", [Res]),
                false
        end
                     end, Nodes),
    ok.

%% @private
is_ring_ready(Node, MainNode) ->
    ?LOG_INFO("~p", [?FUNCTION_NAME]),
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            rpc:call(MainNode, riak_core_ring, ring_ready, [Ring]);
        _ ->
            false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% broadcast tree
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Build the local broadcast tree with the given fanout, and start the nodes
-spec start_broadcast_tree([node()], non_neg_integer()) -> ok.
start_broadcast_tree([Node], _Fanout) ->
    erpc:call(Node, grb_local_broadcast, start_as_singleton, []);

start_broadcast_tree(Nodes, Fanout) ->
    {Root, IntNodes, Leafs} = build_broadcast_tree(Nodes, Fanout),
    {RootNode, RootChildren} = Root,
    ok = erpc:call(RootNode, grb_local_broadcast, start_as_root, [RootChildren]),
    lists:foreach(fun({IntNode, IntParent, IntChildren}) ->
        ok = erpc:call(IntNode, grb_local_broadcast, start_as_node, [IntParent, IntChildren])
    end, IntNodes),
    lists:foreach(fun({LeafNode, LeafParent}) ->
        ok = erpc:call(LeafNode, grb_local_broadcast, start_as_leaf, [LeafParent])
    end, Leafs).

%% @doc Convert the list of given nodes and fanout into an n-ary tree of nodes
-spec build_broadcast_tree(Nodes :: [node()],
                           Fanout :: non_neg_integer()) -> {Root :: {node(), [node()]},
                                                            Nodes :: [{node(), node(), [node()]}],
                                                            Leafs :: [{node(), node()}]}.

%% Seems like dialyzer gets confused by the match on line 327
-dialyzer({no_match, build_broadcast_tree/2}).
build_broadcast_tree(Nodes, Fanout) ->
    Depth = trunc(math:ceil(math:log(length(Nodes) * (Fanout - 1) + 1) / math:log(Fanout))),
    ListTable = ets:new(values, [ordered_set]),
    AccTable = ets:new(tree_repr, [duplicate_bag]),
    true = ets:insert(ListTable, [ {N, ignore} || N <- Nodes ]),
    _ = build_broadcast_tree(ets:first(ListTable), ListTable, Fanout, Depth, AccTable),
    Res = lists:foldl(fun(Node, {Root, IntNodes, Leafs}) ->
        Parent = get_parent(Node, AccTable),
        Children = get_children(Node, AccTable),
        case {Parent, Children} of
            {root, ChildNodes} -> {{Node, ChildNodes}, IntNodes, Leafs};
            {ParentNode, leaf} -> {Root, IntNodes, [{Node, ParentNode} | Leafs]};
            {ParentNode, ChildNodes} -> {Root, [{Node, ParentNode, ChildNodes} | IntNodes], Leafs}
        end
    end, {ignore, [], []}, Nodes),
    true = ets:delete(ListTable),
    true = ets:delete(AccTable),
    Res.

-spec get_parent(node(), ets:tid()) -> node() | root.
get_parent(Node, AccTable) ->
    Par = ets:select(AccTable, [{{'$1', '$2'}, [{'=:=', '$2', {const, Node}}], ['$1']}]),
    case Par of
        [] -> root;
        [ParentNode] -> ParentNode
    end.

-spec get_children(node(), ets:tid()) -> node() | leaf.
get_children(Node, AccTable) ->
    Ch = ets:select(AccTable, [{{'$1', '$2'}, [{'=:=', '$1', {const, Node}}], ['$2']}]),
    case Ch of
        [] -> leaf;
        ChildNodes -> ChildNodes
    end.

%% Hacked-together imperative version of https://pastebin.com/0gVATpRa
-spec build_broadcast_tree(node() | atom(), ets:tid(), non_neg_integer(), non_neg_integer(), ets:tid()) -> node() | ignore.
build_broadcast_tree(_, _, _, 0, _) -> ignore;
build_broadcast_tree('$end_of_table', _, _, _, _) -> ignore;
build_broadcast_tree(Head, List, Fanout, Depth, AccTable) ->
    true = ets:delete(List, Head),
    add_node_children(Fanout, Head, List, Fanout, Depth, AccTable),
    Head.

-spec add_node_children(non_neg_integer(), node(), ets:tid(), non_neg_integer(), non_neg_integer(), ets:tid()) -> ok.
add_node_children(0, _, _, _, _, _) -> ok;
add_node_children(N, Head, List, Fanout, Depth, AccTable) ->
    Root = build_broadcast_tree(ets:next(List, Head), List, Fanout, Depth - 1, AccTable),
    case Root of
        ignore -> ok;
        Other ->
            true = ets:insert(AccTable, {Head, Other}),
            ok
    end,
    add_node_children(N - 1, Head, List, Fanout, Depth, AccTable).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc This function provides the same functionality as wait_ready_nodes
%% except it takes as input a sinlge physical node instead of a list
-spec check_ready(node()) -> boolean().
check_ready(Node) ->
    ?LOG_INFO("[master ready] Checking ~p~n", [Node]),

    VnodesReady = check_vnodes(Node),
    Res3 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_oplog_vnode_master, readers_ready]),
    ReadReplicasReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res3),

    NodeReady = VnodesReady andalso ReadReplicasReady,
    case NodeReady of
        true -> ?LOG_INFO("Node ~w is ready! ~n~n", [Node]);
        false -> ?LOG_INFO("Node ~w is not ready ~n~n", [Node])
    end,

    NodeReady.

-ifdef(BLUE_KNOWN_VC).
check_vnodes(Node) ->
    ?LOG_INFO("[vnodes ready] Checking ~p~n", [Node]),

    Res0 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_propagation_vnode_master, is_ready]),
    Res1 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_oplog_vnode_master, is_ready]),

    PropServiceReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res0),
    BlueTxServiceReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res1),

    VnodesReady = PropServiceReady andalso BlueTxServiceReady,
    case VnodesReady of
        true -> ?LOG_INFO("Vnodes ready at ~w~n", [Node]);
        false -> ?LOG_INFO("Vnodes not yet ready at ~w~n", [Node])
    end,
    VnodesReady.
-else.
check_vnodes(Node) ->
    ?LOG_INFO("[vnodes ready] Checking ~p~n", [Node]),

    Res0 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_paxos_vnode_master, is_ready]),
    Res1 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_propagation_vnode_master, is_ready]),
    Res2 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_oplog_vnode_master, is_ready]),

    PaxosServiceReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res0),
    PropServiceReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res1),
    BlueTxServiceReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res2),

    VnodesReady = PaxosServiceReady andalso PropServiceReady andalso BlueTxServiceReady,
    case VnodesReady of
        true -> ?LOG_INFO("Vnodes ready at ~w~n", [Node]);
        false -> ?LOG_INFO("Vnodes not yet ready at ~w~n", [Node])
    end,
    VnodesReady.
-endif.

-spec wait_until_master_ready(node()) -> ok.
wait_until_master_ready(MasterNode) ->
    wait_until(fun() -> check_ready(MasterNode) end).

-spec wait_until_vnodes_ready(node()) -> ok | {fail, boolean()}.
wait_until_vnodes_ready(Node) ->
    wait_until(fun() -> check_vnodes(Node) end).

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached.
%%
%% @TODO Use config for this
-spec wait_until(fun(() -> boolean())) -> ok | {fail, boolean()}.
wait_until(Fun) when is_function(Fun) ->
    MaxTime = 600000,
    Delay = 1000,
    Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).

-spec wait_until(
    fun(() -> boolean()),
    non_neg_integer(),
    non_neg_integer()
) -> ok | {fail, boolean()}.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    wait_until_result(Fun, true, Retry, Delay).

-spec wait_until_result(
    fun(() -> any()),
    any(),
    non_neg_integer(),
    non_neg_integer()
) -> ok | {fail, any()}.

wait_until_result(Fun, Result, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        Result ->
            ok;

        _ when Retry == 1 ->
            {fail, Res};

        _ ->
            timer:sleep(Delay),
            wait_until_result(Fun, Result, Retry-1, Delay)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

grb_cluster_manager_broadcast_tree_test() ->
    Nodes0 = [a,b,c,d,e,f,g],
    %%        a
    %%      /   \
    %%     b     e
    %%    / \   / \
    %%   c   d  f  g
    {Root0, IntNodes0, Leafs0} = build_broadcast_tree(Nodes0, 2),
    ?assertMatch({a, [b, e]}, Root0),
    ?assertEqual(
        lists:usort([{b, a, [c, d]}, {e, a, [f, g]}]),
        lists:usort(IntNodes0)
    ),
    ?assertEqual(
        lists:usort([{c, b}, {d, b}, {f, e}, {g, e}]),
        lists:usort(Leafs0)
    ),

    Nodes1 = [a,b,c,d,e,f,g,h],
    %%        a
    %%        |
    %%        b
    %%      /   \
    %%     c     f
    %%    / \   / \
    %%   d   e g   h
    {Root1, IntNodes1, Leafs1} = build_broadcast_tree(Nodes1, 2),
    ?assertMatch({a, [b]}, Root1),
    ?assertEqual(
        lists:usort([{b, a, [c, f]}, {c, b, [d, e]}, {f, b, [g, h]}]),
        lists:usort(IntNodes1)
    ),
    ?assertEqual(
        lists:usort([{d, c}, {e, c}, {g, f}, {h, f}]),
        lists:usort(Leafs1)
    ),

    Nodes2 = [a,b,c,d,e,f,g,h,i,j,k,l,m],
    %%       _______a_______
    %%      /       |       \
    %%     b        f        j
    %%   / | \    / | \    / | \
    %%  c  d  e   g h  i   k l  m
    {Root2, IntNodes2, Leafs2} = build_broadcast_tree(Nodes2, 3),
    ?assertMatch({a, [b, f, j]}, Root2),
    ?assertEqual(
        lists:usort([{b, a, [c, d, e]}, {f, a, [g, h, i]}, {j, a, [k, l, m]}]),
        lists:usort(IntNodes2)
    ),
    ?assertEqual(
        lists:usort([{c, b}, {d, b}, {e, b}, {g, f}, {h, f}, {i, f}, {k, j}, {l, j}, {m, j}]),
        lists:usort(Leafs2)
    ).

-endif.
