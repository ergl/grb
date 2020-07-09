-module(grb_dc_manager).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([start_background_processes/0,
         start_propagation_processes/0,
         enable_blue_append/0,
         disable_blue_append/0,
         replica_descriptor/0,
         connect_to_replicas/1,
         stop_background_processes/0,
         stop_propagation_processes/0]).

%% All functions are called through erpc
-ignore_xref([start_background_processes/0,
              start_propagation_processes/0,
              enable_blue_append/0,
              disable_blue_append/0,
              replica_descriptor/0,
              connect_to_replicas/1,
              stop_background_processes/0,
              stop_propagation_processes/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_background_processes() -> ok.
start_background_processes() ->
    Res0 = grb_dc_utils:bcast_vnode_sync(grb_main_vnode_master, start_blue_hb_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res0),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_main_vnode_master, start_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res1),
    Res2 = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, learn_dc_id),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res2),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

%% @doc Enable partitions appending transactions to committedBlue (enabled by default)
-spec enable_blue_append() -> ok.
enable_blue_append() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, enable_blue_append),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    ok.

%% @doc Disable partitions appending transactions to committedBlue (enabled by default)
%%
%%      This is useful if we know we'll never connect to other replicas, so we don't waste
%%      memory accumulating transactions that we'll never send.
-spec disable_blue_append() -> ok.
disable_blue_append() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, disable_blue_append),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    ok.

-spec start_propagation_processes() -> ok.
start_propagation_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, start_propagate_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

-spec stop_background_processes() -> ok.
stop_background_processes() ->
    Res0 = grb_dc_utils:bcast_vnode_sync(grb_main_vnode_master, stop_blue_hb_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res0),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_main_vnode_master, stop_replicas),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

-spec stop_propagation_processes() -> ok.
stop_propagation_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, stop_propagate_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

%% @doc Get the descriptor for this replica/cluster.
%%
%%      Contains information from all the nodes in the cluster
%%      so it is enough to call this function at a single node
%%      in an entire DC.
%%
-spec replica_descriptor() -> replica_descriptor().
replica_descriptor() ->
    {ok, Port} = application:get_env(grb, inter_dc_port),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Id = riak_core_ring:cluster_name(Ring),
    {NumPartitions, PartitionList} = riak_core_ring:chash(Ring),
    %% Convert a list of [{partition_id(), node()}, ...] into
    %% #{partition_id() => {inet:ip_address(), inet:port_number()}}
    {PartitionsWithInfo, _} = lists:foldl(fun({P, Node}, {PartitionInfo, IPMap}) ->
        IP = maps:get(Node, IPMap, erpc:call(Node, grb_dc_utils, my_bounded_ip, [])),
        {PartitionInfo#{P => {IP, Port}}, IPMap#{Node => IP}}
    end, {#{}, #{}}, PartitionList),
    #replica_descriptor{
        replica_id=Id,
        num_partitions=NumPartitions,
        remote_addresses=PartitionsWithInfo
    }.

%% @doc Commands this cluster to join to all given descriptors
%%
%%      This will take care of joining all nodes in the local
%%      cluster with all the appropriate nodes in the remote
%%      cluster, so it is enough to call this in a single node
%%      per DC.
%%
-spec connect_to_replicas([replica_descriptor()]) -> ok | {error, term()}.
connect_to_replicas(Descriptors) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalId = riak_core_ring:cluster_name(Ring),
    LocalNodes = riak_core_ring:all_members(Ring),
    {LocalNumPartitions, _} = riak_core_ring:chash(Ring),
    connect_to_replicas(Descriptors, LocalId, LocalNodes, LocalNumPartitions).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec connect_to_replicas([replica_descriptor()], replica_id(), [node()], non_neg_integer()) -> ok | {error, term()}.
connect_to_replicas([], _, _, _) -> ok;
connect_to_replicas([#replica_descriptor{replica_id=Id} | Rest], Id, Nodes, Num) ->
    %% Skip myself
    connect_to_replicas(Rest, Id, Nodes, Num);
connect_to_replicas([Desc | Rest], LocalId, LocalNodes, LocalNum) ->
    #replica_descriptor{replica_id=RemoteId, num_partitions=RemoteNum} = Desc,
    case RemoteNum =:= LocalNum of
        false ->
            ?LOG_ERROR("Cannot join DC ~p, partition mismatch ~p =/= ~p", [RemoteId, RemoteNum, LocalNum]),
            {error, {partition_mismatch, RemoteNum, LocalNum}};
        true ->
            ?LOG_INFO("Starting join DC ~p", [RemoteId]),
            case connect_nodes_to_descriptor(LocalNodes, Desc) of
                {error, Reason} ->
                    {error, {bad_remote_connect, Reason}};
                ok ->
                    connect_to_replicas(Rest, LocalId, LocalNodes, LocalNum)
            end
    end.

-spec connect_nodes_to_descriptor([node()], replica_descriptor()) -> ok | {error, term()}.
connect_nodes_to_descriptor(Nodes, Desc=#replica_descriptor{replica_id=RemoteId}) ->
    Returns = erpc:multicall(Nodes, grb_dc_connection_manager, connect_to, [Desc]),
    lists:foldl(fun({Resp, Node}, Acc) ->
        case Acc of
            {error, Reason} -> {error, Reason};
            ok ->
                case Resp of
                    {ok, ok} ->
                        ok;
                    {throw, Term} ->
                        ?LOG_ERROR("Remote node ~p threw ~p while connecting to DC ~p", [Node, Term, RemoteId]),
                        {error, Term};
                    {error, Reason} ->
                        ?LOG_ERROR("Remote node ~p errored with ~p while connecting to DC ~p", [Node, Reason, RemoteId]),
                        {error, Reason}
                end
        end
    end, ok, lists:zip(Returns, Nodes)).
