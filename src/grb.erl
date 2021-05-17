-module(grb).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% External API for console use
-export([start/0,
         stop/0]).

%% Util API
-export([connect/0,
         put_direct/2,
         put_conflicts/1,
         partition_ready/2]).

%% Start / Barrier
-export([uniform_barrier/3,
         start_transaction/2]).

%% Regular read / write API
-export([update/4,
         partition_wait/4,
         key_snapshot/6,
         key_snapshot_bypass/5]).

%% Read operation API
-export([read_operation/7,
         read_operation_bypass/6]).

%% Multi-read API
-export([multikey_snapshot/5,
         multikey_snapshot_bypass/5]).

%% Multi-update API (reads back)
-export([multikey_update/5,
         multikey_update_bypass/5]).

%% Blue Transaction Commit
-export([prepare_blue/3,
         decide_blue/3]).

%% Red Transaction Commit
-export([commit_red/6]).

-ifdef(TEST).
-export([sync_uniform_barrier/2,
         sync_key_vsn/5,
         sync_key_operation/5,
         sync_commit_red/5]).
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

-spec put_direct(partition_id(), writeset()) -> ok.
put_direct(Partition, WS) ->
    grb_oplog_vnode:put_direct(Partition, WS).

-spec put_conflicts(conflict_relations()) -> ok.
-ifndef(USE_REDBLUE_SEQUENCER).
put_conflicts(Conflicts) ->
    grb_paxos_vnode:put_conflicts_all(Conflicts).
-else.
put_conflicts(Conflicts) ->
    grb_redblue_connection:send_conflicts(Conflicts).
-endif.

-spec uniform_barrier(grb_promise:t(), partition_id(), vclock()) -> ok.
-ifdef(UBARRIER_NOOP).

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
-ifdef(STABLE_SNAPSHOT).

start_transaction(Partition, ClientVC) ->
    UpdatedStableVC = grb_propagation_vnode:merge_remote_stable_vc(Partition, ClientVC),
    grb_vclock:max(ClientVC, UpdatedStableVC).

-else.
-ifdef(UNIFORM_SNAPSHOT).

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

-spec key_snapshot(grb_promise:t(), partition_id(), term(), key(), crdt(), vclock()) -> ok.
key_snapshot(Promise, Partition, TxId, Key, Type, SnapshotVC) ->
    grb_vnode_proxy:async_key_snapshot(Promise, Partition, TxId, Key, Type, SnapshotVC).

-spec key_snapshot_bypass(partition_id(), term(), key(), crdt(), vclock()) -> {ok, snapshot()}.
key_snapshot_bypass(Partition, TxId, Key, Type, SnapshotVC) ->
    grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, SnapshotVC).

-spec read_operation(grb_promise:t(), partition_id(), term(), key(), crdt(), operation(), vclock()) -> ok.
read_operation(Promise, Partition, TxId, Key, Type, ReadOp, SnapshotVC) ->
    grb_vnode_proxy:async_key_operation(Promise, Partition, TxId, Key, Type, ReadOp, SnapshotVC).

-spec read_operation_bypass(partition_id(), term(), key(), crdt(), operation(), vclock()) -> {ok, term()}.
read_operation_bypass(Partition, TxId, Key, Type, ReadOp, SnapshotVC) ->
    Vsn = grb_oplog_vnode:get_key_version(Partition, TxId, Key, Type, SnapshotVC),
    {ok, grb_crdt:apply_read_op(ReadOp, Vsn)}.

-spec multikey_snapshot(Promise :: grb_promise:t(),
                        Partition :: partition_id(),
                        TxId :: term(),
                        SnapshotVC :: vclock(),
                        KeyTypes :: [{key(), crdt()}]) -> ok.

multikey_snapshot(Promise, Partition, TxId, SnapshotVC, KeyTypes) ->
    grb_vnode_proxy:multikey_snapshot(Promise, Partition, TxId, SnapshotVC, {reads, KeyTypes}).

-spec multikey_snapshot_bypass(Promise :: grb_promise:t(),
                               Partition :: partition_id(),
                               TxId :: term(),
                               SnapshotVC :: vclock(),
                               KeyTypes :: [{key(), crdt()}]) -> ok.

multikey_snapshot_bypass(Promise, Partition, TxId, SnapshotVC, KeyTypes) ->
    grb_vnode_proxy:multikey_snapshot_bypass(Promise, Partition, TxId, SnapshotVC, {reads, KeyTypes}).

-spec multikey_update(Promise :: grb_promise:t(),
                      Partition :: partition_id(),
                      TxId :: term(),
                      SnapshotVC :: vclock(),
                      KeyOps :: [{key(), operation()}]) -> ok.

multikey_update(Promise, Partition, TxId, SnapshotVC, KeyOps) ->
    grb_vnode_proxy:multikey_snapshot(Promise, Partition, TxId, SnapshotVC, {updates, KeyOps}).

-spec multikey_update_bypass(Promise :: grb_promise:t(),
                               Partition :: partition_id(),
                               TxId :: term(),
                               SnapshotVC :: vclock(),
                               KeyOps :: [{key(), crdt()}]) -> ok.

multikey_update_bypass(Promise, Partition, TxId, SnapshotVC, KeyOps) ->
    grb_vnode_proxy:multikey_snapshot_bypass(Promise, Partition, TxId, SnapshotVC, {updates, KeyOps}).


-spec update(partition_id(), term(), key(), operation()) -> ok.
update(Partition, TxId, Key, Operation) ->
    grb_oplog_vnode:put_client_op(Partition, TxId, Key, Operation).

-spec partition_wait(grb_promise:t(), partition_id(), term(), vclock()) -> ok.
partition_wait(Promise, Partition, TxId, SnapshotVC) ->
    grb_vnode_proxy:partition_wait(Promise, Partition, TxId, SnapshotVC).

-spec prepare_blue(partition_id(), term(), vclock()) -> non_neg_integer().
prepare_blue(Partition, TxId, VC) ->
    grb_oplog_vnode:prepare_blue(Partition, TxId, VC).

-spec decide_blue(partition_id(), any(), vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    grb_oplog_vnode:decide_blue(Partition, TxId, VC).

-spec commit_red(Promise :: grb_promise:t(),
                 Partition :: partition_id(),
                 TxId :: term(),
                 Label :: tx_label(),
                 SnapshotVC :: vclock(),
                 Prepares :: [{partition_id(), readset(), writeset()}]) -> ok.

-ifdef(NO_STRONG_ENTRY_VC).
commit_red(Promise, _, _, _, VC, _) -> grb_promise:resolve({ok, VC}, Promise).
-else.
-ifdef(USE_REDBLUE_SEQUENCER).
commit_red(Promise, Partition, TxId, Label, VC, Prepares) ->
    %% Forward the message to the sequencer, who will be responsible
    %% for coordinating this transaction.
    %% We register the promise with the sequencer manager, and
    %% when the transaction is done, we will receive a message to
    %% grb_redblue_connection, who will look up the promise, and
    %% reply back to the client.
    ok = grb_redblue_manager:register_promise(Promise, TxId),
    grb_redblue_connection:send_red_commit(Partition, TxId, Label, VC, Prepares).
-else.
commit_red(Promise, TargetPartition, TxId, Label, SnapshotVC, Prepares) ->
    Coordinator = grb_red_manager:register_coordinator(TxId),
    grb_red_coordinator:commit(Coordinator, Promise, TargetPartition, TxId, Label, SnapshotVC, Prepares).
-endif.
-endif.

%%%===================================================================
%%% Internal
%%%===================================================================

-spec read_snapshot_prologue(partition_id(), vclock()) -> ok.
-ifdef(STABLE_SNAPSHOT).
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
    ok = read_snapshot_prologue(Partition, VC),
    Ref = make_ref(),
    key_snapshot(grb_promise:new(self(), Ref), Partition, TxId, Key, Type, VC),
    receive
        {'$grb_promise_resolve', Result, Ref} ->
            Result
    end.

-spec sync_key_operation(partition_id(), term(), key(), operation(), vclock()) -> {ok, term()}.
sync_key_operation(Partition, TxId, Key, Operation, VC) ->
    ok = read_snapshot_prologue(Partition, VC),
    Ref = make_ref(),
    read_operation(grb_promise:new(self(), Ref), Partition, TxId, Key, grb_crdt:op_type(Operation), Operation, VC),
    receive
        {'$grb_promise_resolve', Result, Ref} ->
            Result
    end.

-spec sync_commit_red(partition_id(), term(), tx_label(), vclock(), [{partition_id(), readset(), writeset()}]) -> {ok, vclock()} | {abort, term()}.
sync_commit_red(Partition, TxId, Label, VC, Prepares) ->
    Ref = make_ref(),
    ok = grb:commit_red(grb_promise:new(self(), Ref), Partition, TxId, Label, VC, Prepares),
    receive
        {'$grb_promise_resolve', Result, Ref} ->
            Result
    end.
-endif.
