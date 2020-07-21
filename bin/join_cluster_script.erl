#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -hidden -name join_cluster@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).

-define(DEFAULT_TREE_FANOUT, 2).
-include_lib("eunit/include/eunit.hrl").

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "Usage: ~s [-c --cluster cluster_name] [-f config_file] | 'node_1@host_1' ... 'node_n@host_n'~n", [Name]),
    halt(1).

main(["eunit"]) ->
    eunit:test(fun test/0);

main(Args) ->
    case parse_args(Args, []) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, Opts} ->
            io:format("~p~n", [Opts]),
            case maps:is_key(config, Opts) of
                true -> prepare_from_config(Opts);
                false -> prepare_from_args(maps:get(rest, Opts))
            end
    end.

prepare_from_args(NodeListString) ->
    prepare(validate(parse_node_list(NodeListString))).

prepare_from_config(#{config := Config, cluster := ClusterName}) ->
    prepare(validate(parse_node_config(ClusterName, Config)));
prepare_from_config(#{config := Config}) ->
    prepare(validate(parse_node_config(Config))).

%% @doc Parse a literal node list passed as argument
-spec parse_node_list(list(string())) -> {ok, [node()], non_neg_integer()} | error.
parse_node_list([]) ->
    error;

parse_node_list([_|_]=NodeListString) ->
    try
        Nodes = lists:foldl(fun(NodeString, Acc) ->
            Node = list_to_atom(NodeString),
            [Node | Acc]
                            end, [], NodeListString),
        {ok, lists:reverse(Nodes), ?DEFAULT_TREE_FANOUT}
    catch
        _:_ -> error
    end.

-spec parse_node_config(ClusterName :: atom(), ConfigFilePath :: string()) -> {ok, [node()], non_neg_integer()} | {error, term()}.
parse_node_config(ClusterName, ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {error, Reason} ->
            {error, Reason};
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            Fanout = case lists:keyfind(tree_fanout, 1, Terms) of
                false -> ?DEFAULT_TREE_FANOUT;
               {tree_fanout, TreeFanout} -> TreeFanout
            end,
            case maps:is_key(ClusterName, ClusterMap) of
                false ->
                    {error, unicode:characters_to_list(io_lib:format("No cluster named ~p", [ClusterName]))};
                true ->
                    #{servers := Servers} = maps:get(ClusterName, ClusterMap),
                    {ok, build_erlang_node_names(lists:usort(Servers)), Fanout}
            end
    end.

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) -> {ok, #{atom() => [node()]}, non_neg_integer()} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            Fanout = case lists:keyfind(tree_fanout, 1, Terms) of
                false -> ?DEFAULT_TREE_FANOUT;
                {tree_fanout, TreeFanout} -> TreeFanout
            end,
            Nodes = maps:fold(fun(Cluster, #{servers := Servers}, Acc) ->
                Acc#{Cluster => build_erlang_node_names(lists:usort(Servers))}
            end, #{}, ClusterMap),
            {ok, Nodes, Fanout};
        _ ->
            error
    end.

-spec build_erlang_node_names([atom()]) -> [atom()].
build_erlang_node_names(NodeNames) ->
    [begin
         {ok, Addr} = inet:getaddr(Node, inet),
         IPString = inet:ntoa(Addr),
         list_to_atom("grb@" ++ IPString)
     end || Node <- NodeNames].

%% @doc Validate parsing, then proceed
-spec validate({ok, [node()] | #{atom() => [node()]}} | error | {error, term()}) -> ok | no_return().
validate(error) ->
    usage();

validate({error, Reason}) ->
    io:fwrite(standard_error, "Validate error: ~p~n", [Reason]),
    usage();

validate({ok, Nodes, Fanout}) ->
    {ok, Nodes, Fanout}.

-spec prepare({ok, [node()] | #{atom() => [node()]}}) -> ok.
prepare({ok, ClusterMap, Fanout}) when is_map(ClusterMap) ->
    maps:fold(fun(ClusterName, NodeList, ok) ->
        io:format("Starting clustering at ~p (fanout ~p) of nodes ~p~n", [ClusterName, Fanout, NodeList]),
        ok = do_join(NodeList, Fanout)
    end, ok, ClusterMap);

prepare({ok, Nodes, Fanout}) ->
    io:format("Starting clustering (fanout ~p) of nodes ~p~n", [Fanout, Nodes]),
    do_join(Nodes, Fanout).

do_join(N=[SingleNode], Fanout) ->
    io:format("Started background processes, checking master ready~n"),
    erpc:call(SingleNode, grb_dc_manager, start_background_processes, []),
    ok = start_broadcast_tree(N, Fanout),
    ok = wait_until_master_ready(SingleNode),
    io:format("Successfully joined nodes ~p~n", [N]),
    ok;

do_join([MainNode | _] = Nodes, Fanout) ->
    lists:foreach(fun(N) -> erlang:set_cookie(N, grb_cookie) end, Nodes),
    ok = join_cluster(Nodes),
    io:format("Starting background processes~n"),
    Result = erpc:multicall(Nodes, grb_dc_manager, start_background_processes, []),
    case lists:all(fun({ok, ok}) -> true; (_) -> false end, Result) of
        true ->
            io:format("Started background processes, starting broadcast tree~n"),
            ok = start_broadcast_tree(Nodes, Fanout),
            io:format("Started broadcast tree, checking master ready~n"),
            ok = wait_until_master_ready(MainNode),
            io:format("Successfully joined nodes ~p~n", [Nodes]);
        false ->
            io:fwrite(standard_error, "start_background_processes failed with ~p, aborting~n", [Result]),
            halt(1)
    end.

%% @doc Build clusters out of the given node list
-spec join_cluster(list(atom())) -> ok.
join_cluster([MainNode | OtherNodes] = Nodes) ->
    ok = case check_nodes_own_their_ring(Nodes) of
             ok ->
                 ok;
             {error, FaultyNode, Reason} ->
                 io:fwrite(standard_error, "Bad node ~s on ownership check with reason ~p", [FaultyNode, Reason]),
                 halt(1)
         end,

    %% Do a plan/commit staged join, instead of sequential joins
    ok = lists:foreach(fun(N) -> request_join(N, MainNode) end, OtherNodes),
    ok = wait_plan_ready(MainNode),
    ok = commit_plan(MainNode),
    ok = try_cluster_ready(Nodes),

    ok = wait_until_nodes_ready(Nodes),

    %% Ensure each node owns a portion of the ring
    ok = wait_until_nodes_agree_about_ownership(Nodes),
    ok = wait_until_no_pending_changes(Nodes),
    ok = wait_until_ring_converged(Nodes).

%% @doc Ensure that all nodes are the sole owner of their rings
-spec check_nodes_own_their_ring(list(atom())) -> ok | {error, atom()}.
check_nodes_own_their_ring([]) -> ok;
check_nodes_own_their_ring([H | T]) ->
    case sorted_ring_owners(H) of
        {ok, [H]} ->
            check_nodes_own_their_ring(T);
        Reason ->
            {error, H, Reason}
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
            io:format("Owners at ~p: ~p~n", [Node, SortedOwners]),
            {ok, SortedOwners};

        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Make `Node` request joining with `MasterNode`
-spec request_join(node(), node()) -> ok.
request_join(Node, MasterNode) ->
    timer:sleep(5000),
    R = rpc:call(Node, riak_core, staged_join, [MasterNode]),
    io:format("[join request] ~p to ~p: (result ~p)~n", [Node, MasterNode, R]),
    ok.

-spec wait_plan_ready(node()) -> ok.
wait_plan_ready(Node) ->
    io:format("[ring plan] Will start plan on ~p~n", [Node]),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            io:format("[ring plan] Ring not ready, retrying...~n"),
            timer:sleep(5000),
            ok = wait_until_no_pending_changes(Node),
            wait_plan_ready(Node);

        {ok, _, _} ->
            ok
    end.

-spec commit_plan(node()) -> ok.
commit_plan(Node) ->
    io:format("[ring commit] Will start commit on ~p~n", [Node]),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            io:format("[ring commit] Plan changed, retrying...~n"),
            timer:sleep(100),
            ok = wait_until_no_pending_changes(Node),
            ok = wait_plan_ready(Node),
            commit_plan(Node);

        {error, ring_not_ready} ->
            io:format("[ring commit] Ring not ready, retrying...~n"),
            timer:sleep(100),
            wait_until_no_pending_changes(Node),
            commit_plan(Node);

        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;

        ok ->
            ok
    end.

%% @doc Given a list of nodes, wait until all nodes believe there are no
%%      on-going or pending ownership transfers.
%%
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes([MainNode | _] = Nodes) when is_list(Nodes) ->
    io:format("~p~n", [?FUNCTION_NAME]),
    NoPendingHandoffs = fun() ->
        rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
        {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
        io:format("Check no pending handoffs (badnodes: ~p)...~n", [BadNodes]),
        case BadNodes of
            [] ->
                Res = lists:all(fun({ok, Ring}) ->
                    [] =:= rpc:call(MainNode, riak_core_ring, pending_changes, [Ring])
                end, Rings),
                io:format("Pending changes: ~p~n", [Res]),
                Res;
            _ ->
                false
        end
                        end,

    wait_until(NoPendingHandoffs);

wait_until_no_pending_changes(Node) ->
    wait_until_no_pending_changes([Node]).

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

%% @doc Wait until all nodes agree about ready nodes in their rings
-spec try_cluster_ready([node()]) -> ok.
try_cluster_ready(Nodes) ->
    try_cluster_ready(Nodes, 3, 500).

-spec try_cluster_ready([node()], non_neg_integer(), non_neg_integer()) -> ok.
try_cluster_ready([MainNode | _] = _Nodes, 0, _SleepMs) ->
    io:format("[cluster ready] Still not ready, will retry plan~n"),
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
                io:format("wait_until_nodes_ready got ~p~n", [Res]),
                false
        end
                     end, Nodes),
    ok.

%% @doc Wait until all nodes agree about all ownership views
-spec wait_until_nodes_agree_about_ownership([node()]) -> boolean().
wait_until_nodes_agree_about_ownership(Nodes) ->
    io:format("~p~n", [?FUNCTION_NAME]),
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
                io:format("wait_until_nodes_agree_about_ownership got ~p~n", [Res]),
                false
        end
                     end, Nodes),
    ok.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
-spec wait_until_ring_converged([node()]) -> ok.
wait_until_ring_converged([MainNode | _] = Nodes) ->
    io:format("~p~n", [?FUNCTION_NAME]),
    true = lists:all(fun(Node) ->
        case wait_until(fun() -> is_ring_ready(Node, MainNode) end) of
            ok ->
                true;
            Res ->
                io:format("wait_until_ring_converged got ~p~n", [Res]),
                false
        end
                     end, Nodes),
    ok.

%% @private
is_ring_ready(Node, MainNode) ->
    io:format("~p~n", [?FUNCTION_NAME]),
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            rpc:call(MainNode, riak_core_ring, ring_ready, [Ring]);
        _ ->
            false
    end.

-spec wait_until_master_ready(node()) -> ok.
wait_until_master_ready(MasterNode) ->
    wait_until(fun() -> check_ready(MasterNode) end).

%% @doc This function provides the same functionality as wait_ready_nodes
%% except it takes as input a sinlge physical node instead of a list
-spec check_ready(node()) -> boolean().
check_ready(Node) ->
    io:format("[master ready] Checking ~p~n", [Node]),

    Res0 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_main_vnode_master, is_ready]),
    Res1 = erpc:call(Node, grb_dc_utils, bcast_vnode_sync, [grb_main_vnode_master, replicas_ready]),
    VNodeReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res0),
    ReadReplicasReady = lists:all(fun({_, true}) -> true; (_) -> false end, Res1),

    NodeReady = VNodeReady andalso ReadReplicasReady,
    case NodeReady of
        true ->
            io:format("Node ~w is ready! ~n~n", [Node]);
        false ->
            io:format("Node ~w is not ready ~n~n", [Node])
    end,

    NodeReady.

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
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_args([], _) -> {error, noargs};
parse_args(Args, Required) ->
    case parse_args_inner(Args, #{}) of
        {ok, Opts} -> required(Required, Opts);
        Err -> Err
    end.

parse_args_inner([], Acc) -> {ok, Acc};
parse_args_inner([ [$- | Flag] | Args], Acc) ->
    case Flag of
        [$c] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{cluster => list_to_atom(Arg)} end);
        "-cluster" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{cluster => list_to_atom(Arg)} end);
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        "-file" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        [$h] ->
            usage(),
            halt(0);
        _ ->
            {error, {badarg, Flag}}
    end;

parse_args_inner(Words, Acc) ->
    {ok, Acc#{rest => Words}}.

parse_flag(Flag, Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args_inner(Rest, Fun(FlagArg));
        _ -> {error, {noarg, Flag}}
    end.

required(Required, Opts) ->
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        true -> {ok, Opts};
        false -> {error, "Missing required fields"}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test() ->
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
