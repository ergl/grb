-module(grb_dc_utils).
-include("grb.hrl").

-export([replica_id/0,
         cluster_info/0,
         my_bounded_ip/0,
         my_partitions/0,
         all_partitions/0,
         key_location/1,
         bcast_vnode_sync/2,
         bcast_vnode_async/2,
         bcast_vnode_async_noself/3,
         bcast_vnode_local_sync/2]).

%% For external script
-export([is_ring_owner/0,
         pending_ring_changes/0,
         ready_ring_members/0]).

%% Called via `erpc`
-ignore_xref([is_ring_owner/0,
              my_bounded_ip/0,
              pending_ring_changes/0,
              ready_ring_members/0]).

-define(BUCKET, <<"grb">>).

%% todo(borja, warn): Should persist this, Antidote says it can change
-spec replica_id() -> replica_id().
replica_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

-spec is_ring_owner() -> boolean().
is_ring_owner() ->
    SelfNode = node(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    lists:all(fun({_, N}) -> N =:= SelfNode end, riak_core_ring:all_owners(Ring)).

-spec pending_ring_changes() -> boolean().
pending_ring_changes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    [] =/= riak_core_ring:pending_changes(Ring).

-spec ready_ring_members() -> [node()].
ready_ring_members() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:ready_members(Ring).

-spec cluster_info() -> {ok, replica_id(), non_neg_integer(), [index_node()]}.
cluster_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplicaID = riak_core_ring:cluster_name(Ring),
    {NumPartitions, Nodes} = riak_core_ring:chash(Ring),
    {ok, ReplicaID, NumPartitions, Nodes}.

-spec my_bounded_ip() -> inet:ip_address().
my_bounded_ip() ->
    {ok, IPString} = application:get_env(grb, bounded_ip),
    {ok, IP} = inet:parse_address(IPString),
    IP.

-spec my_partitions() -> [partition_id()].
my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

-spec all_partitions() -> [partition_id()].
all_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Chash = riak_core_ring:chash(Ring),
    [ P || {P, _} <- chash:nodes(Chash)].

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

-spec bcast_vnode_async(atom(), any()) -> ok.
bcast_vnode_async(Master, Request) ->
    lists:foreach(fun(IndexNode) ->
        riak_core_vnode_master:command(IndexNode, Request, Master)
    end, get_index_nodes()).

-spec bcast_vnode_async_noself(atom(), partition_id(), any()) -> ok.
bcast_vnode_async_noself(Master, Self, Request) ->
    lists:foreach(fun
        ({P, _}) when P =:= Self -> ok;
        (IndexNode) ->  riak_core_vnode_master:command(IndexNode, Request, Master)
    end, get_index_nodes()).

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
        true -> abs(AsInt)
    end;
convert_key(Key) ->
    HashKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    abs(crypto:bytes_to_integer(HashKey)).
