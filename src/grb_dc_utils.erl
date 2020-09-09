-module(grb_dc_utils).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-export([cluster_info/0,
         inter_dc_ip_port/0,
         my_partitions/0,
         get_index_nodes/0,
         key_location/1,
         bcast_vnode_sync/2,
         bcast_vnode_sync/3,
         bcast_vnode_local_sync/2,
         bcast_vnode_local_sync/3]).

%% Managing ETS tables
-export([new_cache/2,
         new_cache/3,
         cache_name/2,
         safe_bin_to_atom/1]).

%% For external script
-export([is_ring_owner/0,
         pending_ring_changes/0,
         ready_ring_members/0]).

%% Called via `erpc` or for debug purposes
-ignore_xref([is_ring_owner/0,
              key_location/1,
              inter_dc_ip_port/0,
              pending_ring_changes/0,
              ready_ring_members/0,
              get_index_nodes/0]).

-define(BUCKET, <<"grb">>).

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

-spec inter_dc_ip_port() -> {inet:ip_address(), inet:port_number()}.
inter_dc_ip_port() ->
    {ok, IPString} = application:get_env(grb, bounded_ip),
    {ok, IP} = inet:parse_address(IPString),
    {ok, Port} = application:get_env(grb, inter_dc_port),
    {IP, Port}.

-spec my_partitions() -> [partition_id()].
my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

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

-spec bcast_vnode_sync(atom(), any(), non_neg_integer()) -> any().
bcast_vnode_sync(Master, Request, Timeout) ->
    [begin
         {P, riak_core_vnode_master:sync_command(N, Request, Master, Timeout)}
     end || {P, _} =  N <- get_index_nodes()].

%% @doc Broadcast a message to all vnodes of the given type
%%      in the current physical node.
-spec bcast_vnode_local_sync(atom(), any()) -> any().
bcast_vnode_local_sync(Master, Request) ->
    [begin
         {P, riak_core_vnode_master:sync_command({P, node()}, Request, Master)}
     end || P <- get_local_partitions()].

%% @doc Broadcast a message to all vnodes of the given type
%%      in the current physical node.
-spec bcast_vnode_local_sync(atom(), any(), non_neg_integer()) -> any().
bcast_vnode_local_sync(Master, Request, Timeout) ->
    [begin
         {P, riak_core_vnode_master:sync_command({P, node()}, Request, Master, Timeout)}
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

-spec new_cache(partition_id(), atom()) -> cache_id().
new_cache(Partition, Name) ->
    new_cache(Partition, Name, [set, protected, named_table, {read_concurrency, true}]).

-spec new_cache(partition_id(), atom(), [term()]) -> cache_id().
new_cache(Partition, Name, Options) ->
    CacheName = cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            ?LOG_INFO("Unable to create cache ~p at ~p, retrying", [Name, Partition]),
            timer:sleep(100),
            try ets:delete(CacheName) catch _:_ -> ok end,
            new_cache(Partition, Name, Options)
    end.

-spec cache_name(partition_id(), atom()) -> cache_id().
cache_name(Partition, Name) ->
    BinNode = atom_to_binary(node(), latin1),
    BinName = atom_to_binary(Name, latin1),
    BinPart = integer_to_binary(Partition),
    TableName = <<BinName/binary, <<"-">>/binary, BinPart/binary, <<"@">>/binary, BinNode/binary>>,
    safe_bin_to_atom(TableName).

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

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
