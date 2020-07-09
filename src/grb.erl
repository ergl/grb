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
         perform_op/5,
         prepare_blue/4,
         decide_blue/3]).

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
    Val = crypto:strong_rand_bytes(Size),
    BottomRed = 0,
    Res = grb_dc_utils:bcast_vnode_sync(grb_main_vnode_master, {update_default, Val, BottomRed}),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_main_vnode_master, get_default),
    true = lists:all(fun({P, {DefaultVal, DefaultRed}}) ->
        case (DefaultVal =:= Val) andalso (DefaultRed =:= BottomRed) of
            true -> true;
            false ->
                ?LOG_WARNING("Couldn't load at partition ~p", [P]),
                false
        end
    end, Res1),
    ok.

-spec uniform_barrier(grb_promise:t(), partition_id(), vclock()) -> ok.
uniform_barrier(Promise, Partition, CVC) ->
    ok = grb_partition_replica:uniform_barrier(Promise, Partition, CVC).

%% todo(borja, uniformity): Have to update uniform_vc, not stable_vc
start_transaction(Partition, ClientVC) ->
    StableVC0 = grb_propagation_vnode:stable_vc(Partition),
    StableVC1 = grb_vclock:max_except(grb_dc_utils:replica_id(), StableVC0, ClientVC),
    ok = grb_propagation_vnode:update_stable_vc(Partition, StableVC1),
    grb_vclock:max(ClientVC, StableVC1).

-spec perform_op(grb_promise:t(), partition_id(), key(), vclock(), val()) -> ok.
perform_op(Promise, Partition, Key, SnapshotVC, Val) ->
    ok = grb_partition_replica:async_op(Promise, Partition, Key, SnapshotVC, Val).

-spec prepare_blue(partition_id(), any(), any(), vclock()) -> non_neg_integer().
prepare_blue(Partition, TxId, WriteSet, VC) ->
    grb_main_vnode:prepare_blue(Partition, TxId, WriteSet, VC).

-spec decide_blue(partition_id(), any(), vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    ok = grb_partition_replica:decide_blue(Partition, TxId, VC).
