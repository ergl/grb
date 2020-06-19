-module(grb_dc_manager).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([start_background_processes/0,
         replica_descriptor/0,
         connect_to_replica/1,
         stop_background_processes/0]).

%% All functions are called through erpc
-ignore_xref([start_background_processes/0,
              replica_descriptor/0,
              connect_to_replica/1,
              stop_background_processes/0]).

start_background_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_propagate_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),
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
    %% #{inet:ip_address() => {inet:port_address(), [partition_id()]}}
    {NodesWithInfo, _} = lists:foldl(fun({P, Node}, {NodeInfo, IPMap}) ->
        IP = maps:get(Node, IPMap, erpc:call(Node, grb_dc_utils, my_bounded_ip, [])),
        InfoMap = maps:update_with(IP, fun({NodePort, Ps}) -> {NodePort, [P | Ps]} end, {Port, [P]}, NodeInfo),
        {InfoMap, IPMap#{Node => IP}}
    end, {#{}, #{}}, PartitionList),
    #replica_descriptor{
        replica_id=Id,
        num_partitions=NumPartitions,
        remote_addresses=NodesWithInfo
    }.

%% @doc Commands this cluster to join to all given descriptors
%%
%%      This will take care of joining all nodes in the local
%%      cluster with all the appropriate nodes in the remote
%%      cluster, so it is enough to call this in a single node
%%      per DC.
%%
-spec connect_to_replica([replica_descriptor()]) -> ok.
connect_to_replica(Descriptors) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalId = riak_core_ring:cluster_name(Ring),
    LocalNodes = riak_core_ring:all_members(Ring),
    {LocalNumPartitions, _} = riak_core_ring:chash(Ring),
    connect_to_replica(Descriptors, LocalId, LocalNodes, LocalNumPartitions).

-spec connect_to_replica([replica_descriptor()], replica_id(), non_neg_integer(), [node()]) -> ok | {error, term()}.
connect_to_replica([], _, _, _) -> ok;
connect_to_replica([#replica_descriptor{replica_id=Id} | Rest], Id, Nodes, Num) ->
    %% Skip myself
    connect_to_replica(Rest, Id, Nodes, Num);
connect_to_replica([Desc | Rest], LocalId, LocalNodes, LocalNum) ->
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
                    connect_to_replica(Rest, LocalId, LocalNodes, LocalNum)
            end
    end.

-spec connect_nodes_to_descriptor([node()], replica_descriptor()) -> ok | {error, term()}.
connect_nodes_to_descriptor(Nodes, Desc=#replica_descriptor{replica_id=RemoteId}) ->
    Returns = erpc:multicall(Nodes, grb_dc_connection_manager, connect_to, [Desc]),
    lists:foldl(fun({Resp, Node}) ->
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
    end, ok, lists:zip(Returns, Nodes)).

stop_background_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, stop_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_propagate_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.
