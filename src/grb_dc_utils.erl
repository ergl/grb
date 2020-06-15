-module(grb_dc_utils).
-include("grb.hrl").

-export([replica_id/0,
         cluster_info/0,
         key_location/1,
         bcast_vnode_sync/2,
         bcast_vnode_local_sync/2]).

-define(BUCKET, <<"grb">>).

%% todo(borja): Should persist this, Antidote says it can change
-spec replica_id() -> replica_id().
replica_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

-spec cluster_info() -> {ok, replica_id(), non_neg_integer(), [index_node()]}.
cluster_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplicaID = riak_core_ring:cluster_name(Ring),
    {NumPartitions, Nodes} = riak_core_ring:chash(Ring),
    {ok, ReplicaID, NumPartitions, Nodes}.

-spec key_location(key()) -> index_node().
key_location(Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {Num, List} = riak_core_ring:chash(Ring),
    Pos = convert_key(Key) rem Num + 1,
    lists:nth(Pos, List).

%% @doc Broadcast a message to all vnodes of the given type
%%      in the current data center
-spec bcast_vnode_sync(atom(), any()) -> any().
bcast_vnode_sync(Master, Request) ->
    [begin
         {P, riak_core_vnode_master:sync_command(N, Request, Master)}
     end || {P, _} =  N <- get_index_nodes()].

%% @doc Broadcast a message to all vnodes of the given type
%%      in the current physical node.
-spec bcast_vnode_local_sync(atom(), any()) -> any().
bcast_vnode_local_sync(Master, Request) ->
    [begin
         {P, riak_core_vnode_master:sync_command({P, node()}, Request, Master)}
     end || P <- get_local_partitions()].

-spec get_local_partitions() -> [partition_id()].
get_local_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

-spec get_index_nodes() -> [index_node()].
get_index_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Chash = riak_core_ring:chash(Ring),
    chash:nodes(Chash).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec convert_key(key()) -> non_neg_integer().
convert_key(Key) when is_integer(Key) -> abs(Key);
convert_key(Key) when is_binary(Key) ->
    AsInt = (catch list_to_integer(binary_to_list(Key))),
    case is_integer(AsInt) of
        false ->
            HashKey = riak_core_util:chash_key({?BUCKET, Key}),
            abs(crypto:bytes_to_integer(HashKey));
        true -> abs(Key)
    end;
convert_key(Key) ->
    HashKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    abs(crypto:bytes_to_integer(HashKey)).
