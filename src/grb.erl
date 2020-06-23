-module(grb).
-include("grb.hrl").

%% External API for console use
-export([start/0,
         stop/0]).

%% API for applications
-export([connect/0,
         start_transaction/1,
         perform_op/5,
         prepare_blue/4,
         decide_blue/3]).

-ignore_xref([ping/0]).

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

%% todo(borja): Update uniform_vc
%% have to update everywhere but the current replica
start_transaction(ClientVC) ->
    UniformVC   = grb_replica_state:uniform_vc(),
    StableVC    = grb_replica_state:stable_vc(),
    SnapshotVC0 = grb_vclock:max(ClientVC, UniformVC),
    SnapshotVC1 = grb_vclock:max_at(red, SnapshotVC0, StableVC),
    SnapshotVC1.

-spec perform_op(grb_promise:t(), partition_id(), key(), vclock(), val()) -> ok.
perform_op(Promise, Partition, Key, SnapshotVC, Val) ->
    ok = grb_partition_replica:async_op(Promise, Partition, Key, SnapshotVC, Val).

-spec prepare_blue(partition_id(), any(), any(), vclock()) -> non_neg_integer().
prepare_blue(Partition, TxId, WriteSet, VC) ->
    grb_main_vnode:prepare_blue(Partition, TxId, WriteSet, VC).

-spec decide_blue(partition_id(), any(), vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    ok = grb_partition_replica:decide_blue(Partition, TxId, VC).
