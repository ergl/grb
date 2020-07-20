-module(grb_local_broadcast).

-behaviour(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Called by supervisor machinery or through erpc
-ignore_xref([start_link/0,
              start_as_singleton/0,
              start_as_root/1,
              start_as_node/2,
              start_as_leaf/1]).

-export([start_link/0]).

%% External API
-export([start_as_singleton/0,
         start_as_root/1,
         start_as_node/2,
         start_as_leaf/1]).

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
    self_name :: atom(),
    self_replica = undefined :: replica_id() | undefined,
    self_partitions = [] :: [partition_id()],
    tree_state = undefined:: tree_state()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API (through RPC)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_name(node())}, ?MODULE, [], []).

-spec start_as_singleton() -> ok.
start_as_singleton() ->
    SelfNode = {global, generate_name(node())},
    gen_server:call(SelfNode, init_singlenode).

-spec start_as_root([node()]) -> ok.
start_as_root(Children) ->
    SelfNode = {global, generate_name(node())},
    gen_server:call(SelfNode, {init_root, [generate_name(N) || N <- Children]}).

-spec start_as_node(node(), [node()]) -> ok.
start_as_node(Parent, Children) ->
    SelfNode = {global, generate_name(node())},
    gen_server:call(SelfNode, {init_node, generate_name(Parent), [generate_name(N) || N <- Children]}).

-spec start_as_leaf(node()) -> ok.
start_as_leaf(Parent) ->
    SelfNode = {global, generate_name(node())},
    gen_server:call(SelfNode, {init_leaf, generate_name(Parent)}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, #state{self_name=generate_name(node())}}.

-spec init_state(#state{}) -> #state{}.
init_state(S) ->
    MyReplica = grb_dc_manager:replica_id(),
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
    {ok, Interval}  = application:get_env(grb, local_broadcast_interval),
    TRef = erlang:send_after(Interval, self(), broadcast_clock),
    Leaf = #leaf_state{parent=Parent, broadcast_interval=Interval, broadcast_timer=TRef},
    {reply, ok, InitState#state{tree_state=Leaf}};

handle_call(init_singlenode, _From, S=#state{tree_state=undefined}) ->
    InitState = init_state(S),
    {ok, Interval}  = application:get_env(grb, local_broadcast_interval),
    TRef = erlang:send_after(Interval, self(), broadcast_clock),
    Singleton = #singleton_state{broadcast_interval=Interval, broadcast_timer=TRef},
    {reply, ok, InitState#state{tree_state=Singleton}};

handle_call(E, _From, S) ->
    ?LOG_WARNING("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast({clock_event, From, ChildSVC}, S=#state{self_name=SelfNode,
                                                    self_partitions=Partitions,
                                                    tree_state=RootState=#root_state{}}) ->

    ?LOG_DEBUG("root node received stableVC ~p from child ~p", [ChildSVC, From]),

    #root_state{children=Children, children_to_ack=N, children_acc=Acc} = RootState,
    NewRootState = case N of
        1 ->
            LocalSVC = compute_local_svc(Partitions),
            GlobalSVC = compute_children_svc([ChildSVC | Acc], LocalSVC),
            ok = update_stableVC(Partitions, GlobalSVC),
            ok = send_to_children(SelfNode, Children, GlobalSVC),
            ?LOG_DEBUG("root node recomputing global stableVC as ~p, sending to ~p", [GlobalSVC, Children]),
            RootState#root_state{children_acc=[], children_to_ack=length(Children)};
        _ ->
            RootState#root_state{children_acc=[ChildSVC | Acc], children_to_ack=N-1}
    end,
    {noreply, S#state{tree_state=NewRootState}};

handle_cast({clock_event, From, ChildSVC}, S=#state{self_name=SelfNode,
                                                    self_partitions=Partitions,
                                                    tree_state=NodeState=#node_state{}}) ->

    ?LOG_DEBUG("int node received stableVC ~p from child ~p", [ChildSVC, From]),

    #node_state{parent=Parent, children=Children, children_to_ack=N, children_acc=Acc} = NodeState,
    NewNodeState = case N of
        1 ->
            LocalSVC = compute_local_svc(Partitions),
            ChildrenSVC = compute_children_svc([ChildSVC | Acc], LocalSVC),
            ?LOG_DEBUG("int node recomputing stableVC as ~p, sending to ~p", [ChildrenSVC, Parent]),
            ok = send_to_parent(SelfNode, Parent, ChildrenSVC),
            NodeState#node_state{children_acc=[], children_to_ack=length(Children)};
        _ ->
            NodeState#node_state{children_acc=[ChildSVC | Acc], children_to_ack=N-1}
    end,
    {noreply, S#state{tree_state=NewNodeState}};

handle_cast({set_svc, Parent, ParentSVC}, S=#state{self_name=SelfNode,
                                                   self_partitions=Partitions,
                                                   tree_state=#node_state{parent=Parent,
                                                                          children=Children}}) ->

    ?LOG_DEBUG("int node received stableVC ~p from parent ~p", [ParentSVC, Parent]),
    ok = update_stableVC(Partitions, ParentSVC),
    ok = send_to_children(SelfNode, Children, ParentSVC),
    {noreply, S};

handle_cast({set_svc, Parent, ParentSVC}, S=#state{self_partitions=Partitions,
                                                   tree_state=#leaf_state{}}) ->

    ?LOG_DEBUG("leaf node received stableVC ~p from parent ~p", [ParentSVC, Parent]),
    ok = update_stableVC(Partitions, ParentSVC),
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

%% Send our local stableVC to our parent periodically
handle_info(broadcast_clock, S=#state{self_name=SelfNode,
                                      self_partitions=Partitions,
                                      tree_state=Leaf=#leaf_state{}}) ->
    #leaf_state{parent=Parent,
                broadcast_timer=TRef,
                broadcast_interval=Int} = Leaf,

    erlang:cancel_timer(TRef),

    LocalSVC = compute_local_svc(Partitions),
    ok = send_to_parent(SelfNode, Parent, LocalSVC),

    ?LOG_DEBUG("leaf recomputing stableVC as ~p, sending to ~p", [LocalSVC, Parent]),

    NewLeaf = Leaf#leaf_state{broadcast_timer=erlang:send_after(Int, self(), broadcast_clock)},
    {noreply, S#state{tree_state=NewLeaf}};

%% If singleton, just recalculate our local stableVC
handle_info(broadcast_clock, S=#state{self_partitions=Partitions,
                                      tree_state=Single=#singleton_state{}}) ->

    #singleton_state{broadcast_timer=TRef,
                     broadcast_interval=Int} = Single,

    erlang:cancel_timer(TRef),

    LocalSVC = compute_local_svc(Partitions),
    ok = update_stableVC(Partitions, LocalSVC),

    ?LOG_DEBUG("singleton recomputing stableVC as ~p", [LocalSVC]),

    NewSingle = Single#singleton_state{broadcast_timer=erlang:send_after(Int, self(), broadcast_clock)},
    {noreply, S#state{tree_state=NewSingle}};

handle_info(E, S) ->
    ?LOG_WARNING("unexpected info: ~p at state ~p~n", [E, S]),
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
    list_to_atom("grb_local_broadcast_" ++ atom_to_list(Node)).

%% todo(borja, red): Add "red" entry when computing this
-spec compute_local_svc([partition_id()]) -> vclock().
compute_local_svc(Partitions) ->
    AllReplicas = grb_dc_manager:all_replicas(),
    lists:foldl(fun
        (Partition, undefined) ->
            grb_propagation_vnode:known_vc(Partition);

        (Partition, Acc) ->
            PSVC = grb_propagation_vnode:known_vc(Partition),
            grb_vclock:min_at(AllReplicas, PSVC, Acc)
    end, undefined, Partitions).

-spec compute_children_svc([vclock()], vclock()) -> vclock().
compute_children_svc(Children, AccSVC) ->
    AllReplicas = grb_dc_manager:all_replicas(),
    compute_svc(AllReplicas, Children, AccSVC).

%% todo(borja, red): Add "red" entry when computing this
-spec compute_svc([replica_id()], [vclock()], vclock()) -> vclock().
compute_svc(AllReplicas, VCs, AccSVC) ->
    lists:foldl(fun(SVC, Acc) ->
        grb_vclock:min_at(AllReplicas, SVC, Acc)
    end, AccSVC, VCs).

%% @doc Update the stableVC of the given partitions
%%
%%      Will also recompute the uniformVC of the given partitions
%%      as if we received the stableVC from a remote replica,
%%      and lift the appropriate barriers
-spec update_stableVC([partition_id()], vclock()) -> ok.
update_stableVC(Partitions, StableVC) ->
    lists:foreach(fun(Partition) ->
        ok = grb_propagation_vnode:update_stable_vc(Partition, StableVC)
    end, Partitions).

-spec send_to_parent(atom(), atom(), vclock()) -> ok.
send_to_parent(Self, Parent, StableVC) ->
    ok = gen_server:cast({global, Parent}, {clock_event, Self, StableVC}).

-spec send_to_children(atom(), [atom()], vclock()) -> ok.
send_to_children(Self, Children, StableVC) ->
    lists:foreach(fun(Child) ->
        ok = gen_server:cast({global, Child}, {set_svc, Self, StableVC})
    end, Children).

-ifdef(TEST).

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
