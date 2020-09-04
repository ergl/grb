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

-spec perform_op(grb_promise:t(), partition_id(), key(), vclock(), val()) -> ok.
perform_op(Promise, Partition, Key, SnapshotVC, Val) ->
    ok = grb_partition_replica:async_op(Promise, Partition, Key, SnapshotVC, Val).

-spec prepare_blue(partition_id(), any(), any(), vclock()) -> non_neg_integer().
prepare_blue(Partition, TxId, WriteSet, VC) ->
    grb_main_vnode:prepare_blue(Partition, TxId, WriteSet, VC).

-spec decide_blue(partition_id(), any(), vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    ok = grb_partition_replica:decide_blue(Partition, TxId, VC).
