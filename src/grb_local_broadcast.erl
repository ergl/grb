-module(grb_local_broadcast).

-behaviour(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Called by supervisor machinery
-ignore_xref([start_link/0]).

-export([start_link/0]).

%% External API
-export([start_broadcast_tree/0,
         build_binary_tree/1,
         build_broadcast_tree/2,
         start_broadcast_tree/1]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(singleton_state, {
    broadcast_interval :: non_neg_integer(),
    broadcast_timer :: reference()
}).

-record(leaf_state, {
    parent :: atom(),
    broadcast_interval :: non_neg_integer(),
    broadcast_timer :: reference()
}).

-record(node_state, {
    parent :: atom(),
    children :: [atom()],
    children_to_ack :: non_neg_integer(),
    children_acc :: [vclock()]
}).

-record(root_state, {
    children :: [atom()],
    children_to_ack :: non_neg_integer(),
    children_acc :: [vclock()]
}).

-type tree_state() :: undefined
                    | #singleton_state{}
                    | #leaf_state{}
                    | #node_state{}
                    | #root_state{}.

-record(state, {
    self_replica = undefined :: replica_id() | undefined,
    self_partitions = [] :: [partition_id()],
    tree_state = undefined:: tree_state()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_name(node())}, ?MODULE, [], []).

-spec start_broadcast_tree() -> ok.
start_broadcast_tree() ->
    start_broadcast_tree(grb_dc_utils:all_nodes()).

-spec start_broadcast_tree([node()]) -> ok.
start_broadcast_tree([Node]) when Node =:= node() ->
    gen_server:call({global, generate_name(Node)}, init_singlenode);

start_broadcast_tree(Nodes) ->
    CurrentNode = node(),
    {ok, TreeFanout} = application:get_env(grb, local_broadcast_tree_fanout),
    {ParentMap, ChildMap} = build_broadcast_tree(Nodes, TreeFanout),
    Parent = maps:get(CurrentNode, ParentMap),
    Children = maps:get(CurrentNode, ChildMap),
    case {Parent, Children} of
        {root, _ChildNodes} -> ok; %% todo, init root
        {_ParentNode, leaf} -> ok; %% todo, init as leaf node
        {_ParentNode, _ChildNodes} -> ok %% todo, init as node
    end.

-spec build_binary_tree([node()]) -> {#{}, #{}}.
build_binary_tree(Nodes) ->
    build_binary_tree(Nodes, root, #{}, #{}).

build_binary_tree([], _, Parents, Children) -> {Parents, Children};
build_binary_tree(Nodes, PrevParent, Parents, Children) ->
    Mid = trunc(math:ceil(length(Nodes) / 2)),
    {Left, [Root | Right]} = lists:split(Mid - 1, Nodes),
    {LeftP, LeftC} = build_binary_tree(Left,
                                       Root,
                                       Parents#{Root => PrevParent},
                                       maps:update_with(PrevParent, fun(C) -> [Root | C] end, [Root], Children)),

    build_binary_tree(Right, Root, LeftP, LeftC).

%% @doc Convert the list of given nodes and fanout into an n-ary tree of nodes
-spec build_broadcast_tree([node()], non_neg_integer()) -> {#{}, #{}}.
build_broadcast_tree(Nodes, Fanout) ->
    Depth = trunc(math:ceil(math:log(length(Nodes) * (Fanout - 1) + 1) / math:log(Fanout))),
    ListTable = ets:new(values, [ordered_set]),
    AccTable = ets:new(tree_repr, [duplicate_bag]),
    true = ets:insert(ListTable, [ {N, ignore} || N <- Nodes ]),
    _ = build_broadcast_tree(ets:first(ListTable), ListTable, Fanout, Depth, AccTable),
    ets:delete(ListTable),
    Result = lists:foldl(fun(Node, {Parents, Children}) ->
        Par = ets:select(AccTable, [{{'$1', '$2'}, [{'=:=', '$2', {const, Node}}], ['$1']}]),
        Ch = ets:select(AccTable, [{{'$1', '$2'}, [{'=:=', '$1', {const, Node}}], ['$2']}]),
        NewPars = case Par of
            [] -> Parents#{Node => root};
            [ParentNode] -> Parents#{Node => ParentNode}
        end,

        NewChild = case Ch of
            [] -> Children#{Node => leaf};
            ChildNodes -> Children#{Node => ChildNodes}
        end,
        {NewPars, NewChild}
    end, {#{}, #{}}, Nodes),
    ets:delete(AccTable),
    Result.

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, #state{}}.

-spec init_state(#state{}) -> #state{}.
init_state(S) ->
    MyReplica = grb_dc_utils:replica_id(),
    MyPartitions = grb_dc_utils:my_partitions(),
    S#state{self_replica=MyReplica, self_partitions=MyPartitions}.

handle_call({init_root, Children}, _From, S=#state{tree_state=undefined}) ->
    InitState = init_state(S),
    RootState = #root_state{children=Children, children_acc=[], children_to_ack=length(Children)},
    {reply, ok, InitState#state{tree_state=RootState}};

handle_call({init_node, Parent, Children}, _From, S=#state{tree_state=undefined}) ->
    InitState = init_state(S),
    NodeState = #node_state{parent=Parent, children=Children, children_acc=[], children_to_ack=length(Children)},
    {reply, ok, InitState#state{tree_state=NodeState}};

handle_call({init_leaf, Parent}, _From, S=#state{tree_state=undefined}) ->
    InitState = init_state(S),
    {ok, Interval}  = application:get_env(grb, broadcast_clock_interval),
    TRef = erlang:send_after(Interval, self(), broadcast_clock),
    Leaf = #leaf_state{parent=Parent, broadcast_interval=Interval, broadcast_timer=TRef},
    {reply, ok, InitState#state{tree_state=Leaf}};

handle_call(init_singlenode, _From, S=#state{tree_state=undefined}) ->
    InitState = init_state(S),
    {ok, Interval}  = application:get_env(grb, broadcast_clock_interval),
    TRef = erlang:send_after(Interval, self(), broadcast_clock),
    Singleton = #singleton_state{broadcast_interval=Interval, broadcast_timer=TRef},
    {reply, ok, InitState#state{tree_state=Singleton}};

handle_call(E, _From, S) ->
    ?LOG_WARNING("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast({clock_event, _From, ChildSVC}, S=#state{self_replica=ReplicaId,
                                                     self_partitions=Partitions,
                                                     tree_state=RootState=#root_state{}}) ->

    #root_state{children=Children, children_to_ack=N, children_acc=Acc} = RootState,
    NewRootState = case N of
        1 ->
            LocalSVC = compute_local_svc(ReplicaId, Partitions),
            GlobalSVC = compute_children_svc(ReplicaId, [ChildSVC | Acc], LocalSVC),
            ok = handle_set_svc(node(), GlobalSVC, Partitions, RootState),
            RootState#root_state{children_acc=[], children_to_ack=length(Children)};
        _ ->
            RootState#root_state{children_acc=[ChildSVC | Acc], children_to_ack=N-1}
    end,
    {noreply, S#state{tree_state=NewRootState}};

handle_cast({clock_event, _From, ChildSVC}, S=#state{self_replica=ReplicaId,
                                                    self_partitions=Partitions,
                                                    tree_state=NodeState=#node_state{}}) ->

    #node_state{parent=Parent, children=Children, children_to_ack=N, children_acc=Acc} = NodeState,
    NewNodeState = case N of
        1 ->
            LocalSVC = compute_local_svc(ReplicaId, Partitions),
            ChildrenSVC = compute_children_svc(ReplicaId, [ChildSVC | Acc], LocalSVC),
            ok = gen_server:cast({global, Parent}, {clock_event, node(), ChildrenSVC}),
            NodeState#node_state{children_acc=[], children_to_ack=length(Children)};
        _ ->
            NodeState#node_state{children_acc=[ChildSVC | Acc], children_to_ack=N-1}
    end,
    {noreply, S#state{tree_state=NewNodeState}};

handle_cast({set_svc, Parent, ParentSVC}, S=#state{self_partitions=Partitions,
                                                   tree_state=TreeState}) ->

    ok = handle_set_svc(Parent, ParentSVC, Partitions, TreeState),
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(broadcast_clock, S=#state{self_replica=ReplicaId,
                                      self_partitions=Partitions,
                                      tree_state=Leaf=#leaf_state{}}) ->
    #leaf_state{parent=Parent,
                broadcast_timer=TRef,
                broadcast_interval=Int} = Leaf,

    erlang:cancel_timer(TRef),

    LocalSVC = compute_local_svc(ReplicaId, Partitions),
    ok = gen_server:cast({global, Parent}, {clock_event, node(), LocalSVC}),

    NewLeaf = Leaf#leaf_state{broadcast_timer=erlang:send_after(Int, self(), broadcast_clock)},
    {noreply, S#state{tree_state=NewLeaf}};

handle_info(broadcast_clock, S=#state{self_replica=ReplicaId,
                                      self_partitions=Partitions,
                                      tree_state=Single=#singleton_state{}}) ->

    #singleton_state{broadcast_timer=TRef,
                     broadcast_interval=Int} = Single,

    erlang:cancel_timer(TRef),

    LocalSVC = compute_local_svc(ReplicaId, Partitions),
    ok = handle_set_svc(node(), LocalSVC, Partitions, Single),

    NewSingle = Single#singleton_state{broadcast_timer=erlang:send_after(Int, self(), broadcast_clock)},
    {noreply, S#state{tree_state=NewSingle}};

handle_info(E, S) ->
    ?LOG_WARNING("unexpected info: ~p~n", [E]),
    {noreply, S}.

terminate(_Reason, #state{tree_state=#leaf_state{broadcast_timer=TRef}}) ->
    erlang:cancel_timer(TRef),
    ok;

terminate(_Reason, #state{tree_state=#singleton_state{broadcast_timer=TRef}}) ->
    erlang:cancel_timer(TRef),
    ok;

terminate(_Reason, _S) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec generate_name(node()) -> atom().
generate_name(Node) ->
    list_to_atom("grb_local_broadcast" ++ atom_to_list(Node)).

-spec compute_local_svc(replica_id(), [partition_id()]) -> vclock().
compute_local_svc(ReplicaId, Partitions) ->
    AllReplicas = [ReplicaId | grb_dc_connection_manager:connected_replicas()],
    lists:foldl(fun
        (Partition, undefined) ->
            grb_propagation_vnode:known_vc(Partition);

        (Partition, Acc) ->
            PSVC = grb_propagation_vnode:known_vc(Partition),
            grb_vclock:min_at(AllReplicas, PSVC, Acc)
    end, undefined, Partitions).

-spec compute_children_svc(replica_id(), [vclock()], vclock()) -> vclock().
compute_children_svc(ReplicaId, Children, AccSVC) ->
    AllReplicas = [ReplicaId | grb_dc_connection_manager:connected_replicas()],
    compute_svc(AllReplicas, Children, AccSVC).

-spec compute_svc([replica_id()], [vclock()], vclock()) -> vclock().
compute_svc(AllReplicas, VCs, AccSVC) ->
    lists:foldl(fun(SVC, Acc) ->
        grb_vclock:min_at(AllReplicas, SVC, Acc)
    end, AccSVC, VCs).

-spec handle_set_svc(atom(), vclock(), [partition_id()], tree_state()) -> ok.
handle_set_svc(_, _, _, undefined) -> ok;

handle_set_svc(Parent, SVC, Partitions, #leaf_state{parent=Parent}) ->
    lists:foreach(fun(Partition) ->
        ok = grb_propagation_vnode:update_stable_vc(Partition, SVC)
    end, Partitions);

handle_set_svc(_, SVC, Partitions, #singleton_state{}) ->
    lists:foreach(fun(Partition) ->
        ok = grb_propagation_vnode:update_stable_vc(Partition, SVC)
    end, Partitions);

handle_set_svc(Parent, SVC, Partitions, State) ->
    Children = case State of
        #root_state{children=C} -> C;
        #node_state{children=C, parent=Parent} -> C
    end,

    lists:foreach(fun(Child) ->
        ok = gen_server:cast({global, Child}, {set_svc, node(), SVC})
    end, Children),

    lists:foreach(fun(Partition) ->
        ok = grb_propagation_vnode:update_stable_vc(Partition, SVC)
    end, Partitions).

-ifdef(TEST).

grb_local_broadcast_build_binary_tree_test() ->
    Nodes0 = [a,b,c,d,e,f,g],
    %%        d
    %%      /   \
    %%     b     f
    %%    / \   / \
    %%   a   c  e  g
    {Parents0, Children0} = build_binary_tree(Nodes0),
    ?assertMatch(#{a := b, b := d, c := b, d := root, f := d, e := f, g := f}, Parents0),
    ?assertMatch(#{b := [c, a], d := [f, b], f := [g, e]}, Children0),

    Nodes2 = [a,b,c,d,e,f],
    %%        c
    %%      /  \
    %%     a    e
    %%    /    / \
    %%   b    d   f
    {Parents2, Children2} = build_binary_tree(Nodes2),
%%    {TriParents1, TriChildren1} = build_ternary_tree(Nodes1),
    ?assertMatch(#{a := c, b := a, c := root, d := e, e := c, f := e}, Parents2),
    ?assertMatch(#{a := [b], c := [e, a], e := [f, d]}, Children2).

grb_local_broadcast_build_broadcast_tree_tree_test() ->
    Nodes0 = [a,b,c,d,e,f,g],
    {Parents0, Children0} = build_broadcast_tree(Nodes0, 2),
    ?assertMatch(#{a := root, b := a, c := b, d := b, e := a, f := e, g := e}, Parents0),
    ?assertMatch(#{a := [b,e], b := [c,d], c := leaf, d := leaf, e := [f, g], f := leaf, g := leaf}, Children0),

    Nodes1 = [a,b,c,d,e,f,g,h],
    {Parents1, Children1} = build_broadcast_tree(Nodes1, 2),
    ?assertMatch(#{a := root, b := a, c := b, d := c, e := c, f := b, g := f, h := f}, Parents1),
    ?assertMatch(#{a := [b], b := [c,f], c := [d, e], d := leaf, e := leaf, f := [g, h], g := leaf, h := leaf}, Children1).

grb_local_broadcast_compute_stable_vc_test() ->
    Replicas = [dc_id1, dc_id2, dc_id3],
    SVCs = [
        #{dc_id1 => 0, dc_id2 => 0, dc_id3 => 10},
        #{dc_id1 => 5, dc_id2 => 3, dc_id3 => 2},
        #{dc_id1 => 3, dc_id2 => 4, dc_id3 => 7},
        #{dc_id1 => 0, dc_id2 => 2, dc_id3 => 3}
    ],

    EmptySVC = compute_svc(Replicas, SVCs, grb_vclock:new()),
    lists:foreach(fun(R) ->
        ?assertEqual(0, grb_vclock:get_time(R, EmptySVC))
    end, Replicas),

    [Head | Rest] = SVCs,
    ResultSVC = compute_svc(Replicas, Rest, Head),
    ?assertEqual(#{dc_id1 => 0, dc_id2 => 0, dc_id3 => 2}, ResultSVC).

-endif.
