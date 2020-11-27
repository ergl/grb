-module(grb_dc_manager).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-define(MY_REPLICA, my_replica).
-define(ALL_REPLICAS, all_replicas).
-define(REMOTE_REPLICAS, remote_replicas).

-define(PARTITION, partition_key).

%% Node API
-export([replica_id/0,
         all_replicas/0,
         all_replicas_red/0,
         remote_replicas/0]).

%% Remote API
-export([create_replica_groups/1,
         start_background_processes/0,
         persist_self_replica_info/0,
         start_propagation_processes/0,
         single_replica_processes/0,
         persist_replica_info/0,
         enable_blue_append/0,
         disable_blue_append/0,
         replica_descriptor/0,
         connect_to_replicas/1,
         stop_background_processes/0,
         stop_propagation_processes/0,
         start_paxos_unique_leader/0,
         start_paxos_leader/0,
         start_paxos_follower/1]).

%% All functions here are called through erpc
-ignore_xref([create_replica_groups/1,
              start_background_processes/0,
              persist_self_replica_info/0,
              start_propagation_processes/0,
              single_replica_processes/0,
              persist_replica_info/0,
              enable_blue_append/0,
              disable_blue_append/0,
              replica_descriptor/0,
              connect_to_replicas/1,
              stop_background_processes/0,
              stop_propagation_processes/0,
              start_paxos_unique_leader/0,
              start_paxos_leader/0,
              start_paxos_follower/1]).

-spec replica_id() -> replica_id().
replica_id() ->
    persistent_term:get({?MODULE, ?MY_REPLICA}).

-spec all_replicas() -> [replica_id()].
all_replicas() ->
    persistent_term:get({?MODULE, ?ALL_REPLICAS}, [replica_id()]).

-spec all_replicas_red() -> [all_replica_id()].
-ifdef(BLUE_KNOWN_VC).
all_replicas_red() -> all_replicas().
-else.
all_replicas_red() -> [?RED_REPLICA | all_replicas()].
-endif.

-spec remote_replicas() -> [replica_id()].
remote_replicas() ->
    persistent_term:get({?MODULE, ?REMOTE_REPLICAS}, []).

-spec create_replica_groups([node()]) -> {ok, [replica_id()]} | {error, term()}.
create_replica_groups([SingleNode]) ->
    ?LOG_INFO("Single-replica ~p, disabling blue append~n", [SingleNode]),
    ok = erpc:call(SingleNode, ?MODULE, single_replica_processes, []),
    ok = erpc:call(SingleNode, ?MODULE, start_paxos_unique_leader, []),
    {ok, [replica_id()]};

create_replica_groups(Nodes) ->
    ?LOG_INFO("Starting clustering of nodes ~p~n", [Nodes]),
    Results0 = erpc:multicall(Nodes, ?MODULE, replica_descriptor, []),
    ReplicaResult = lists:foldl(fun
        (_, {error, Reason}) -> {error, Reason};
        ({ok, D}, {ok, Acc}) -> {ok, [D | Acc]};
        ({error, Reason}, _) -> {error, Reason};
        ({throw, Reason}, _) -> {error, Reason}
    end, {ok, []}, Results0),
    case ReplicaResult of
        {error, Reason} ->
            ?LOG_ERROR("replica_descriptor error: ~p~n", [Reason]),
            {error, Reason};

        {ok, Descriptors} ->
            JoinResult0 = erpc:multicall(Nodes, ?MODULE, connect_to_replicas, [Descriptors]),
            JoinResult1 = lists:foldl(fun
                (_, {error, Reason}) -> {error, Reason};
                ({error, Reason}, _) -> {error, Reason};
                ({throw, Reason}, _) -> {error, Reason};
                ({ok, ok}, _) -> ok
            end, ok, JoinResult0),
            case JoinResult1 of
                {error, Reason} ->
                    ?LOG_ERROR("connect_to_replica error: ~p~n", [Reason]),
                    {error, Reason};
                ok ->
                    StartTimerRes0 = erpc:multicall(Nodes, ?MODULE, start_propagation_processes, []),
                    StartTimerRes1 = lists:foldl(fun
                        (_, {error, Reason}) -> {error, Reason};
                        ({error, Reason}, _) -> {error, Reason};
                        ({throw, Reason}, _) -> {error, Reason};
                        ({ok, ok}, _) -> ok
                    end, ok, StartTimerRes0),
                    case StartTimerRes1 of
                        {error, Reason} ->
                            ?LOG_ERROR("start_propagation_processes failed with ~p, aborting~n", [Reason]),
                            {error, Reason};
                        ok ->
                            ok = start_red_processes(Nodes),
                            Ids = [Id || #replica_descriptor{replica_id=Id} <- Descriptors],
                            {ok, Ids}
                    end
            end
    end.

-spec start_red_processes([node()]) -> ok.
-ifdef(BLUE_KNOWN_VC).
start_red_processes(_) -> ok.
-else.
start_red_processes(Nodes) ->
    [Leader | Followers] =  lists:sort(Nodes),
    LeaderId = erpc:call(Leader, ?MODULE, replica_id, []),

    Res = erpc:multicall(Followers, ?MODULE, start_paxos_follower, [LeaderId]),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res),

    ok = erpc:call(Leader, ?MODULE, start_paxos_leader, []),
    ?LOG_INFO("started red processes, leader cluster: ~p", [LeaderId]),
    ok.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc This is only called at the master node, but it should propagate everywhere
-spec start_background_processes() -> ok.
start_background_processes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNodes = riak_core_ring:all_members(Ring),

    Res0 = erpc:multicall(LocalNodes, ?MODULE, persist_self_replica_info, []),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res0),

    Res1 = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, learn_dc_id, 1000),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),

    Res2 = grb_dc_utils:bcast_vnode_sync(grb_oplog_vnode_master, start_blue_hb_timer, 1000),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res2),

    ok = grb_oplog_vnode:start_readers_all(),

    %% if we're not in red mode, this won't do anything
    ok = grb_paxos_vnode:all_fetch_lastvc_table(),

    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

%% @doc Enable partitions appending transactions to committedBlue (enabled by default)
%%      Should only be called at the master node (and only be called when the cluster is only one node)
-spec enable_blue_append() -> ok.
enable_blue_append() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_oplog_vnode_master, enable_blue_append),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    ok.

%% @doc Disable partitions appending transactions to committedBlue (enabled by default)
%%
%%      This is useful if we know we'll never connect to other replicas, so we don't waste
%%      memory accumulating transactions that we'll never send.
-spec disable_blue_append() -> ok.
disable_blue_append() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_oplog_vnode_master, disable_blue_append),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    ok.

%% @doc Call if this cluster is the only replica in town
-spec single_replica_processes() -> ok.
single_replica_processes() ->
    Res0 = grb_dc_utils:bcast_vnode_sync(grb_oplog_vnode_master, disable_blue_append),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res0),

    ok = grb_oplog_vnode:learn_all_replicas_all(),

    SingleDCGroups = [[replica_id()]],
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, {learn_dc_groups, SingleDCGroups}),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),

    Res2 = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, populate_logs),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res2),

    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

%% @doc This is only called at the master node, but it should propagate everywhere
-spec start_propagation_processes() -> ok.
start_propagation_processes() ->
    %% Persist replica info at every node in the cluster
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNodes = riak_core_ring:all_members(Ring),

    Res0 = erpc:multicall(LocalNodes, ?MODULE, persist_replica_info, []),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res0),

    %% Tell oplog vnode to learn about all replicas
    ok = grb_oplog_vnode:learn_all_replicas_all(),

    %% Important, this is the same at all cluster nodes
    MyReplicaId = replica_id(),
    RemoteReplicas = grb_dc_connection_manager:connected_replicas(),

    {ok, MyGroups} = compute_groups(MyReplicaId, RemoteReplicas),
    ?LOG_INFO("Fault tolerant groups: ~p~n", [MyGroups]),

    Res1 = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, {learn_dc_groups, MyGroups}),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),

    Res2 = grb_dc_utils:bcast_vnode_sync(grb_propagation_vnode_master, populate_logs),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res2),

    ok = grb_propagation_vnode:start_propagate_timer_all(),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

-spec start_paxos_unique_leader() -> ok.
-ifdef(BLUE_KNOWN_VC).
start_paxos_unique_leader() -> ok.
-else.
start_paxos_unique_leader() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNodes = riak_core_ring:all_members(Ring),

    Res0 = erpc:multicall(LocalNodes, grb_red_manager, persist_unique_leader_info, []),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res0),

    ok = grb_paxos_vnode:init_leader_state(),

    Res1 = erpc:multicall(LocalNodes, grb_red_manager, start_red_coordinators, []),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.
-endif.

-spec start_paxos_leader() -> ok.
start_paxos_leader() ->
    %% Persist replica info at every node in the cluster
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNodes = riak_core_ring:all_members(Ring),

    Res0 = erpc:multicall(LocalNodes, grb_red_manager, persist_leader_info, []),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res0),

    ok = grb_paxos_vnode:init_leader_state(),

    Res1 = erpc:multicall(LocalNodes, grb_red_manager, start_red_coordinators, []),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

-spec start_paxos_follower(replica_id()) -> ok.
start_paxos_follower(LeaderReplica) ->
    %% Persist replica info at every node in the cluster
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNodes = riak_core_ring:all_members(Ring),

    Res0 = erpc:multicall(LocalNodes, grb_red_manager, persist_follower_info, [LeaderReplica]),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res0),

    ok = grb_paxos_vnode:init_follower_state(),

    Res1 = erpc:multicall(LocalNodes, grb_red_manager, start_red_coordinators, []),
    ok = lists:foreach(fun({ok, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

-spec persist_self_replica_info() -> ok.
persist_self_replica_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplicaId = riak_core_ring:cluster_name(Ring),
    MyPartitions = riak_core_ring:my_indices(Ring),

    ok = persistent_term:put({?MODULE, ?MY_REPLICA}, ReplicaId),
    lists:foldl(fun(P, N) ->
        persistent_term:put({?MODULE, ?PARTITION, N}, P),
        N + 1
    end, 1, MyPartitions),
    ok.

-spec persist_replica_info() -> ok.
persist_replica_info() ->
    MyReplicaId = replica_id(),
    RemoteReplicas = grb_dc_connection_manager:connected_replicas(),
    ok = persistent_term:put({?MODULE, ?REMOTE_REPLICAS}, RemoteReplicas),
    ok = persistent_term:put({?MODULE, ?ALL_REPLICAS}, [MyReplicaId | RemoteReplicas]),
    ?LOG_INFO("Persisted all replicas: ~p~n", [RemoteReplicas]),
    ok.

-spec stop_background_processes() -> ok.
stop_background_processes() ->
    ok = grb_oplog_vnode:stop_blue_hb_timer_all(),
    ok = grb_oplog_vnode:stop_readers_all(),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

-spec stop_propagation_processes() -> ok.
stop_propagation_processes() ->
    ok = grb_propagation_vnode:stop_propagate_timer_all(),
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
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Id = riak_core_ring:cluster_name(Ring),
    Chash = riak_core_ring:chash(Ring),
    PartitionsWithInfo = build_remote_addresses(chash:nodes(Chash)),
    #replica_descriptor{
        replica_id=Id,
        num_partitions=chash:size(Chash),
        remote_addresses=PartitionsWithInfo
    }.

-spec build_remote_addresses([index_node()]) -> #{partition_id() => {inet:ip_address(), inet:port_number()}}.
build_remote_addresses(Indices) ->
    %% Convert a list of [{partition_id(), node()}, ...] into
    %% #{partition_id() => {inet:ip_address(), inet:port_number()}}
    {PartitionInfo, _} = lists:foldl(fun({P, Node}, {PartitionInfo, IPMap}) ->
        {IP, Port} = maps:get(Node, IPMap, erpc:call(Node, grb_dc_utils, inter_dc_ip_port, [])),
        {PartitionInfo#{P => {IP, Port}}, IPMap#{Node => {IP, Port}}}
    end, {#{}, #{}}, Indices),
    PartitionInfo.

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
    NumPartitions = chash:size(riak_core_ring:chash(Ring)),
    connect_to_replicas(Descriptors, LocalId, LocalNodes, NumPartitions).

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


%% @doc Compute all groups of f+1 replicas including the given replica
%%
%%      First, it computes _all_ the possible groups, then selects the
%%      ones with the given id inside.
%%
%%      This may be expensive, but it is only computed once.
%%
%%      todo(borja): Change this if we ever support dynamic join of new replicas
-spec compute_groups(replica_id(), [replica_id()]) -> {ok, [[replica_id()]]} | {error, not_connected}.
compute_groups(_LocalId, []) -> {error, not_connected};
compute_groups(LocalId, RemoteReplicas) ->
    %% Pick length(Replicas), since N=f+1, f = N-1
    AllGroups = cnr(length(RemoteReplicas), [LocalId | RemoteReplicas]),
    {ok, lists:filter(fun(L) -> lists:member(LocalId, L) end, AllGroups)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Copyright 2016-2017 Jorgen Brandt
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% Source: https://github.com/joergen7/lib_combin/blob/b8ef6a0253c6680139aac95b136fee6d6559cf20/src/lib_combin.erl
%% @doc Enumerates all combinations (order does not matter) of length `N'
%%      without replacement by drawing elements from `SrcLst'.
%%
%%      Herein, `N` must be non-negative for the function clause to match.
%%
%%      Example:
%%      ```
%%      lib_combin:cnr( 2, [a,b,c] ).
%%      [[b,a],[c,a],[c,b]]
%%      '''
-spec cnr(non_neg_integer(), [any()]) -> [[any()]].

cnr(N, L) ->
    cnr2(N, L, []).

cnr2(0, _, Acc) -> [Acc];
cnr2(_, [], _) -> [];
cnr2(N, [H|T], Acc) ->
    case T of
        [] -> cnr2(N - 1, [], [H | Acc]);
        [_|_] -> cnr2(N - 1, T, [H | Acc]) ++ cnr2(N, T, Acc)
    end.
