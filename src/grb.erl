-module(grb).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% External API for console use
-export([start/0,
         stop/0]).

%% API for applications
-export([connect/0,
         load/1,
         uniform_barrier/3,
         start_transaction/2,
         update/4,
         partition_ready/2,
         async_key_snapshot/6,
         key_snapshot_bypass/5,
         prepare_blue/3,
         decide_blue/3,
         commit_red/5]).

-ifdef(TEST).
-export([sync_uniform_barrier/2,
         sync_key_vsn/5]).
-endif.

%% Called by rel
-ignore_xref([start/0,
              stop/0]).

%% Public API

-spec start() -> {ok, _} | {error, term()}.
start() ->
    application:ensure_all_started(grb).

-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(grb).

-spec connect() -> {ok, replica_id(), non_neg_integer(), [index_node()]}.
connect() ->
    grb_dc_utils:cluster_info().

-spec load(non_neg_integer()) -> ok.
load(Size) ->
    Value = crypto:strong_rand_bytes(Size),
    BaseSnapshot = grb_crdt:apply_op_raw(
        grb_crdt:make_op(grb_lww, Value),
        grb_crdt:new(grb_lww)
    ),

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LocalNodes = riak_core_ring:all_members(Ring),

    Res = erpc:multicall(LocalNodes, grb_dc_utils, set_default_snapshot, [BaseSnapshot]),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),

    Res1 = lists:zip(LocalNodes, erpc:multicall(LocalNodes, grb_dc_utils, get_default_snapshot, [])),
    true = lists:all(fun({Node, {ok, DefaultSnapshot}}) ->
        case grb_crdt:value(DefaultSnapshot) =:= Value of
            true ->
                true;
            false ->
                ?LOG_WARNING("Couldn't load at node ~p", [Node]),
                false
        end
    end, Res1),
    ok.

-spec uniform_barrier(grb_promise:t(), partition_id(), vclock()) -> ok.
-ifdef(BASIC_REPLICATION).

uniform_barrier(Promise, _Partition, _CVC) ->
    grb_promise:resolve(ok, Promise).

-else.

uniform_barrier(Promise, Partition, CVC) ->
    ReplicaId = grb_dc_manager:replica_id(),
    Timestamp = grb_vclock:get_time(ReplicaId, CVC),
    UniformTimestamp = grb_vclock:get_time(ReplicaId, grb_propagation_vnode:uniform_vc(Partition)),
    case Timestamp =< UniformTimestamp of
        true ->
            %% fast-path uniform barrier if the check is true, don't wait until
            %% the next uniformVC update to check
            grb_promise:resolve(ok, Promise);
        false ->
            grb_propagation_vnode:register_uniform_barrier(Promise, Partition, Timestamp)
    end.

-endif.

-spec start_transaction(partition_id(), vclock()) -> vclock().
-ifdef(BASIC_REPLICATION).

start_transaction(Partition, ClientVC) ->
    UpdatedStableVC = grb_propagation_vnode:merge_remote_stable_vc(Partition, ClientVC),
    grb_vclock:max(ClientVC, UpdatedStableVC).

-else.
-ifdef(UNIFORM_BLUE).

start_transaction(Partition, ClientVC) ->
    UpdatedUniformVC = grb_propagation_vnode:merge_remote_uniform_vc(Partition, ClientVC),
    grb_vclock:max(ClientVC, UpdatedUniformVC).

-else.

start_transaction(Partition, ClientVC) ->
    UpdatedUniformVC = grb_propagation_vnode:merge_remote_uniform_vc(Partition, ClientVC),
    SVC = grb_vclock:max(ClientVC, UpdatedUniformVC),
    StableRed = grb_propagation_vnode:stable_red(Partition),
    grb_vclock:set_max_time(?RED_REPLICA, StableRed, SVC).

-endif.
-endif.

-spec partition_ready(partition_id(), vclock()) -> boolean().
partition_ready(Partition, SnapshotVC) ->
    ok = read_snapshot_prologue(Partition, SnapshotVC),
    ready =:= grb_propagation_vnode:partition_ready(Partition, SnapshotVC).

-spec async_key_snapshot(grb_promise:t(), partition_id(), term(), key(), crdt(), vclock()) -> ok.
async_key_snapshot(Promise, Partition, TxId, Key, Type, SnapshotVC) ->
    grb_oplog_reader:async_key_snapshot(Promise, Partition, TxId, Key, Type, SnapshotVC).

-spec key_snapshot_bypass(partition_id(), term(), key(), crdt(), vclock()) -> {ok, snapshot()}.
key_snapshot_bypass(Partition, TxId, Key, Type, SnapshotVC) ->
    grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, SnapshotVC).

-spec update(partition_id(), term(), key(), operation()) -> ok.
update(Partition, TxId, Key, Operation) ->
    grb_oplog_vnode:put_client_op(Partition, TxId, Key, Operation).

-spec prepare_blue(partition_id(), term(), vclock()) -> non_neg_integer().
prepare_blue(Partition, TxId, VC) ->
    grb_oplog_vnode:prepare_blue(Partition, TxId, VC).

-spec decide_blue(partition_id(), any(), vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    grb_oplog_reader:decide_blue(Partition, TxId, VC).

-spec commit_red(grb_promise:t(), partition_id(), term(), vclock(), [{partition_id(), readset(), writeset()}]) -> ok.
-ifdef(BLUE_KNOWN_VC).
commit_red(Promise, _, _, VC, _) -> grb_promise:resolve({ok, VC}, Promise).
-else.
commit_red(Promise, TargetPartition, TxId, SnapshotVC, Prepares) ->
    Coordinator = grb_red_manager:register_coordinator(TxId),
    grb_red_coordinator:commit(Coordinator, Promise, TargetPartition, TxId, SnapshotVC, Prepares).
-endif.

%%%===================================================================
%%% Internal
%%%===================================================================

-spec read_snapshot_prologue(partition_id(), vclock()) -> ok.
-ifdef(BASIC_REPLICATION).
read_snapshot_prologue(Partition, SnapshotVC) ->
    grb_propagation_vnode:merge_into_stable_vc(Partition, SnapshotVC).
-else.
read_snapshot_prologue(Partition, SnapshotVC) ->
    grb_propagation_vnode:merge_into_uniform_vc(Partition, SnapshotVC).
-endif.

%%%===================================================================
%%% Test
%%%===================================================================

-ifdef(TEST).
-spec sync_uniform_barrier(partition_id(), vclock()) -> ok.
sync_uniform_barrier(Partition, CVC) ->
    Ref = make_ref(),
    uniform_barrier(grb_promise:new(self(), Ref), Partition, CVC),
    receive
        {'$grb_promise_resolve', Result, Ref} ->
            Result
    end.

-spec sync_key_vsn(partition_id(), term(), key(), crdt(), vclock()) -> {ok, snapshot()}.
sync_key_vsn(Partition, TxId, Key, Type, VC) ->
    case partition_ready(Partition, VC) of
        true ->
            grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, VC);

        false ->
            Ref = make_ref(),
            async_key_snapshot(grb_promise:new(self(), Ref), Partition, TxId, Key, Type, VC),
            receive
                {'$grb_promise_resolve', Result, Ref} ->
                    Result
            end
    end.
-endif.
