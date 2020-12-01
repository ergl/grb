-module(grb_dc_utils).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-export([get_default_snapshot/0,
         set_default_snapshot/1,
         cluster_info/0,
         inter_dc_ip_port/0,
         my_partitions/0,
         get_index_nodes/0,
         key_location/1,
         bcast_vnode_sync/2,
         bcast_vnode_sync/3,
         bcast_vnode_local_sync/2]).

%% Managing ETS tables
-export([safe_bin_to_atom/1]).

%% Starting timers
-export([maybe_send_after/2]).

%% For external script
-export([is_ring_owner/0,
         pending_ring_changes/0,
         ready_ring_members/0]).

%% Util API
-export([convert_key/1]).

%% Called via `erpc` or for debug purposes
-ignore_xref([set_default_snapshot/1,
              get_default_snapshot/0,
              is_ring_owner/0,
              key_location/1,
              inter_dc_ip_port/0,
              pending_ring_changes/0,
              ready_ring_members/0,
              get_index_nodes/0]).

-define(BUCKET, <<"grb">>).
-define(BOTTOM, bottom_value_key).

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

-spec get_default_snapshot() -> snapshot().
get_default_snapshot() ->
    persistent_term:get({?MODULE, ?BOTTOM}, grb_crdt:new(grb_lww)).

-spec set_default_snapshot(snapshot()) -> ok.
set_default_snapshot(Snapshot) ->
    persistent_term:put({?MODULE, ?BOTTOM}, Snapshot).

-spec cluster_info() -> {ok, replica_id(), non_neg_integer(), [index_node()]}.
cluster_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplicaID = riak_core_ring:cluster_name(Ring),
    Chash = riak_core_ring:chash(Ring),
    {ok, ReplicaID, chash:size(Chash), chash:nodes(Chash)}.

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
    Chash = riak_core_ring:chash(Ring),
    Pos = convert_key(Key) rem chash:size(Chash) + 1,
    lists:nth(Pos, chash:nodes(Chash)).

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

-spec get_local_partitions() -> [partition_id()].
get_local_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

-spec get_index_nodes() -> [index_node()].
get_index_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Chash = riak_core_ring:chash(Ring),
    chash:nodes(Chash).

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

-spec maybe_send_after(non_neg_integer(), term()) -> reference() | undefined.
maybe_send_after(0, _) ->
    undefined;
maybe_send_after(Time, Msg) ->
    erlang:send_after(Time, self(), Msg).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec convert_key(key()) -> non_neg_integer().
convert_key(Key) ->
    if
        is_integer(Key) -> convert_key_int(Key);
        is_binary(Key) -> convert_key_binary(Key);
        is_tuple(Key) -> convert_key_hash(element(1, Key));
        true -> convert_key_hash(Key)
    end.

-spec convert_key_int(integer()) -> non_neg_integer().
convert_key_int(Int) ->
    abs(Int).

-spec convert_key_binary(binary()) -> non_neg_integer().
convert_key_binary(Bin) ->
    AsInt = (catch list_to_integer(binary_to_list(Bin))),
    if
        is_integer(AsInt) ->
            convert_key_int(AsInt);
        true ->
            convert_key_hash(Bin)
    end.

-spec convert_key_hash(term()) -> non_neg_integer().
convert_key_hash(Key) ->
    HashKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    abs(crypto:bytes_to_integer(HashKey)).
