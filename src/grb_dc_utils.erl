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
         bcast_vnode_local_sync/2]).

-export([send_cast/4]).

-export([get_vnode_pid/2,
         register_vnode_pid/3,
         vnode_command/3,
         vnode_command/4]).

%% Managing ETS tables
-export([safe_bin_to_atom/1]).

%% Starting timers
-export([maybe_send_after/2,
         maybe_send_after/3,
         maybe_cancel_timer/1]).

%% For external script
-export([is_ring_owner/0,
         pending_ring_changes/0,
         ready_ring_members/0]).

%% Util API
-export([convert_key/1]).

%% Called via `erpc` or for debug purposes
-ignore_xref([is_ring_owner/0,
              key_location/1,
              inter_dc_ip_port/0,
              pending_ring_changes/0,
              ready_ring_members/0,
              get_index_nodes/0]).

-define(BUCKET, <<"grb">>).
-define(header(Packet, Size), (Size):(Packet)/unit:8-integer-big-unsigned).

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
    Chash = riak_core_ring:chash(Ring),
    {ok, grb_dc_manager:replica_id(), chash:size(Chash), chash:nodes(Chash)}.

-spec inter_dc_ip_port() -> {inet:ip_address(), inet:port_number()}.
inter_dc_ip_port() ->
    {ok, IPString} = application:get_env(grb, inter_dc_ip),
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

-spec maybe_send_after(non_neg_integer(), pid(), term()) -> reference() | undefined.
maybe_send_after(0, _, _) ->
    undefined;
maybe_send_after(Time, Dest, Msg) ->
    erlang:send_after(Time, Dest, Msg).

-spec maybe_cancel_timer(reference() | undefined) -> ok.
maybe_cancel_timer(Ref) when is_reference(Ref) ->
    ?CANCEL_TIMER_FAST(Ref);
maybe_cancel_timer(_) ->
    ok.

%% Optimized erpc:cast/4, peeked into its internals.
-spec send_cast(node(), module(), atom(), [term()]) -> ok.
send_cast(N, M, F, A) ->
    _ = erts_internal:dist_spawn_request(N, {M, F, A}, [{reply, no}], spawn_request),
    ok.

-spec register_vnode_pid(atom(), partition_id(), pid()) -> ok.
register_vnode_pid(VMaster, Partition, Pid) ->
    persistent_term:put({?MODULE, VMaster, Partition}, Pid).

-spec get_vnode_pid(atom(), partition_id()) -> pid().
get_vnode_pid(VMaster, Partition) ->
    persistent_term:get({?MODULE, VMaster, Partition}).

%% Optimized riak_core_vnode:command/3 when we're in the same Erlang node.
-spec vnode_command(partition_id(), term(), atom()) -> ok.
vnode_command(Partition, Request, VMaster) ->
    vnode_command(Partition, Request, ignore, VMaster).

%% Optimized riak_core_vnode:command/4 when we're in the same Erlang node.
%%
%% Riak core will always route requests through proxy processes, unless we pass
%% the process pid of the target vnode process. If we register that identifier
%% when the process starts, we can bypass proxies, and simply send a gen_fsm
%% message to the vnode pid.
%%
%% It also avoids allocating tuples / lists having to do with preference lists,
%% as we always have the size of the preflist set to 1.
%%
%% We looked at the internals of riak_core_vnode_master for this.
%%
-spec vnode_command(partition_id(), term(), _, atom()) -> ok.
vnode_command(Partition, Request, Sender, VMaster) ->
    gen_fsm_compat:send_event(get_vnode_pid(VMaster, Partition),
                              riak_core_vnode_master:make_request(Request, Sender, Partition)).

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
    HashKey = riak_core_util:chash_key({?BUCKET, Key}),
    abs(crypto:bytes_to_integer(HashKey)).
