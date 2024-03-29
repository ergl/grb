-module(grb_oplog_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([transaction_ops/2]).
-endif.

%% Management API
-export([stop_blue_hb_timer_all/0,
         start_readers_all/0,
         stop_readers_all/0,
         learn_all_replicas_all/0]).

%% Unsafe load API
-export([put_direct/2,
         put_direct_vnode/3,
         append_direct_vnode/3]).

%% ETS table API
-export([op_log_table/1,
         last_vc_table/1,
         clean_transaction_ops/2]).

-export([process_pid/1]).

%% Public API
-export([get_key_snapshot/5,
         get_key_version/5,
         put_client_op/4,
         prepare_blue/3,
         decide_blue_ready/2,
         decide_blue/3,
         handle_replicate/4,
         handle_replicate_array/6,
         handle_replicate_array/10,
         handle_red_transaction/5,
         handle_red_heartbeat/2]).

%% riak_core_vnode callbacks
-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_overload_command/3,
         handle_overload_info/2,
         handle_coverage/4,
         handle_exit/3,
         handle_info/2]).

%% Called by vnode proxy
-ignore_xref([start_vnode/1,
              handle_info/2]).

-define(master, grb_oplog_vnode_master).
-define(blue_tick_req, blue_tick_event).
-define(kill_timer_req, kill_timer_event).
-define(blue_stall_check, blue_stall_check_event).

-define(OP_LOG_TABLE, op_log_table).
-define(OP_LOG_LAST_VC, op_log_last_vc_table).
-define(PREPARED_TABLE, prepared_blue_table).
-define(PREPARED_TABLE_IDX, prepared_blue_table_idx).
-define(PENDING_TX_OPS, pending_tx_ops).

-type op_log() :: cache(key(), grb_version_log:t()).
-type pending_tx_ops() :: cache({term(), key()}, operation()).
-type last_vc() :: cache({key(), tx_label()}, vclock()).

-record(state, {
    partition :: partition_id(),
    all_replicas :: [all_replica_id()],

    %% number of gen_servers replicating this vnode state
    replicas_n :: non_neg_integer(),

    prepared_blue :: cache_id(),
    prepared_blue_idx :: cache_id(),

    blue_tick_interval :: non_neg_integer(),
    blue_tick_pid = undefined :: pid() | undefined,

    %% check if there are stalled transactions in preparedBlue
    stalled_blue_check_interval :: non_neg_integer(),
    stalled_blue_check_timer = undefined :: reference() | undefined,

    op_log :: op_log(),
    op_log_size :: non_neg_integer(),
    op_last_vc :: last_vc(),

    pending_client_ops :: pending_tx_ops(),

    %% It doesn't make sense to append it if we're not connected to other clusters
    should_append_commit = true :: boolean()
}).

-type state() :: #state{}.

-export_type([last_vc/0]).

%%%===================================================================
%%% Management API
%%%===================================================================

-spec stop_blue_hb_timer_all() -> ok.
stop_blue_hb_timer_all() ->
    [try
        riak_core_vnode_master:command(N, stop_blue_hb_timer, ?master)
     catch
         _:_ -> ok
     end  || N <- grb_dc_utils:get_index_nodes() ],
    ok.

-spec start_readers_all() -> ok | error.
start_readers_all() ->
    Results = [try
    riak_core_vnode_master:sync_command(N, start_readers, ?master, 1000)
    catch
        _:_ -> false
    end || N <- grb_dc_utils:get_index_nodes() ],
    case lists:all(fun(Result) -> Result end, Results) of
        true ->
            ok;
        false ->
            error
    end.

-spec stop_readers_all() -> ok.
stop_readers_all() ->
    [try
        riak_core_vnode_master:command(N, stop_readers, ?master)
     catch
         _:_ -> ok
     end  || N <- grb_dc_utils:get_index_nodes() ],
    ok.

-spec learn_all_replicas_all() -> ok.
learn_all_replicas_all() ->
    Replicas = grb_dc_manager:all_replicas_red(),
    [try
        riak_core_vnode_master:command(N, {learn_all_replicas, Replicas}, ?master)
     catch
         _:_ -> ok
     end  || N <- grb_dc_utils:get_index_nodes() ],
    ok.

%%%===================================================================
%%% ETS API
%%%===================================================================

-spec op_log_table(partition_id()) -> cache_id().
op_log_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?OP_LOG_TABLE}).

-spec pending_ops_table(partition_id()) -> cache_id().
pending_ops_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?PENDING_TX_OPS}).

-spec last_vc_table(partition_id()) -> last_vc().
last_vc_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?OP_LOG_LAST_VC}).

-spec prepared_blue_table(partition_id()) -> cache_id().
prepared_blue_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?PREPARED_TABLE}).

-spec prepared_blue_idx_table(partition_id()) -> cache_id().
prepared_blue_idx_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?PREPARED_TABLE_IDX}).

-spec process_pid(partition_id()) -> pid().
process_pid(Partition) ->
    grb_dc_utils:get_vnode_pid(?master, Partition).

-ifdef(TEST).
-spec transaction_ops(partition_id(), term()) -> non_neg_integer().
transaction_ops(Partition, TxId) ->
    ets:select_count(pending_ops_table(Partition), [{ {{TxId, '_'}, '_'}, [], [true] }]).
-endif.

-spec clean_transaction_ops(partition_id(), term()) -> ok.
clean_transaction_ops(Partition, TxId) ->
    clean_transaction_ops_with_table(pending_ops_table(Partition), TxId).

-spec clean_transaction_ops_with_table(cache_id(), term()) -> ok.
clean_transaction_ops_with_table(PendingOps, TxId) ->
    _ = ets:select_delete(PendingOps, [{ {{TxId, '_'}, '_'}, [], [true] }]),
    ok.

%%%===================================================================
%%% Load API (unsafe)
%%%===================================================================

-spec put_direct(partition_id(), writeset()) -> ok.
put_direct(Partition, WS) ->
    AllReplicas = grb_dc_manager:all_replicas_red(),
    {ok, KeyLogSize} = application:get_env(grb, version_log_size),
    put_direct_internal(WS, op_log_table(Partition), KeyLogSize, AllReplicas).

-spec put_direct_internal(WS :: #{key() := {crdt(), non_neg_integer()}},
                          OpLogTable :: cache_id(),
                          Size :: non_neg_integer(),
                          AllReplicas :: [all_replica_id()]) -> ok.

put_direct_internal(WS, OpLogTable, Size, AllReplicas) ->
    Objects = maps:fold(fun(Key, {Type, Value}, Acc) ->
        Operation = grb_crdt:make_op(Type, Value),
        Base = grb_crdt:apply_op_raw(Operation, grb_crdt:new(Type)),
        Log = grb_version_log:new(Type, Base, AllReplicas, Size),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLogTable, Objects),
    ok.

-spec append_direct_internal(writeset(), cache_id(), non_neg_integer(), [all_replica_id()]) -> ok.
append_direct_internal(WS, OpLogTable, Size, AllReplicas) ->
    Objects = maps:fold(fun(Key, Operation, Acc) ->
        Log = case ets:lookup(OpLogTable, Key) of
            [{Key, PrevLog}] ->
                PrevLog;
              [] ->
                  Type = grb_crdt:op_type(Operation),
                  TypeBase = grb_crdt:new(Type),
                  grb_version_log:new(Type, TypeBase, AllReplicas, Size)
        end,
        [ { Key, grb_version_log:apply_raw(Operation, Log) } | Acc]
    end, [], WS),
    true = ets:insert(OpLogTable, Objects),
    ok.

-spec put_direct_vnode(async | sync, index_node(), #{key() => {crdt(), snapshot()}}) -> ok.
put_direct_vnode(async, IndexNode, WS) ->
    riak_core_vnode_master:command(IndexNode, {put_direct, WS}, self(), ?master);

put_direct_vnode(sync, {P, N}, WS) when N =:= node() ->
    AllReplicas = grb_dc_manager:all_replicas_red(),
    {ok, KeyLogSize} = application:get_env(grb, version_log_size),
    put_direct_internal(WS, op_log_table(P), KeyLogSize, AllReplicas);

put_direct_vnode(sync, IndexNode, WS) ->
    riak_core_vnode_master:sync_command(IndexNode, {put_direct, WS}, ?master, infinity).

-spec append_direct_vnode(async | sync, index_node(), #{key() => operation()}) -> ok.
append_direct_vnode(async, IndexNode, WS) ->
    riak_core_vnode_master:command(IndexNode, {append_direct, WS}, self(), ?master);

append_direct_vnode(sync, {P, N}, WS) when N =:= node() ->
    AllReplicas = grb_dc_manager:all_replicas_red(),
    {ok, KeyLogSize} = application:get_env(grb, version_log_size),
    append_direct_internal(WS, op_log_table(P), KeyLogSize, AllReplicas);

append_direct_vnode(sync, IndexNode, WS) ->
    riak_core_vnode_master:sync_command(IndexNode, {append_direct, WS}, ?master, infinity).

%%%===================================================================
%%% API
%%%===================================================================

-spec put_client_op(partition_id(), term(), key(), operation()) -> ok.
put_client_op(Partition, TxId, Key, Operation) ->
    ClientKey = {TxId, Key},
    Table = pending_ops_table(Partition),
    case ets:lookup(Table, ClientKey) of
        [{ClientKey, PrevOp}] ->
            true = ets:insert(Table, {ClientKey, grb_crdt:merge_ops(PrevOp, Operation)});
        [] ->
            true = ets:insert(Table, {ClientKey, Operation})
    end,
    ok.

-spec get_key_snapshot(partition_id(), term(), key(), crdt(), vclock()) -> {ok, snapshot()}.
get_key_snapshot(Partition, TxId, Key, Type, SnapshotVC) ->
    {ok, grb_crdt:value(get_key_version(Partition, TxId, Key, Type, SnapshotVC))}.

-spec get_key_version(partition_id(), term(), key(), crdt(), vclock()) -> grb_crdt:t().
get_key_version(Partition, TxId, Key, Type, SnapshotVC) ->
    case find_key_version(Partition, Key, Type, SnapshotVC) of
        {not_found, Base} ->
            %% todo(borja): Log miss?
            apply_tx_ops(Partition, TxId, Key, Base);

        {ok, KeyVersion} ->
            apply_tx_ops(Partition, TxId, Key, KeyVersion)
    end.

-spec find_key_version(partition_id(), key(), crdt(), vclock()) -> {ok, grb_crdt:t()} | {not_found, grb_crdt:t()}.
find_key_version(Partition, Key, Type, SnapshotVC) ->
    case ets:lookup(op_log_table(Partition), Key) of
        [] -> {not_found, grb_crdt:new(Type)};
        [{Key, VersionLog}] -> grb_version_log:snapshot_lower(SnapshotVC, VersionLog)
    end.

-spec apply_tx_ops(partition_id(), term(), key(), grb_crdt:t()) -> grb_crdt:t().
apply_tx_ops(Partition, TxId, Key, Snapshot) ->
    case ets:lookup(pending_ops_table(Partition), {TxId, Key}) of
        [] -> Snapshot;
        %% We don't care about metadata for the operation here, since we now
        %% our operations always come after the snapshot
        [{_, Operation}] -> grb_crdt:apply_op_raw(Operation, Snapshot)
    end.

-spec prepare_blue(partition_id(), term(), vclock()) -> grb_time:ts().
prepare_blue(Partition, TxId, SnapshotVC) ->
    Ts = grb_time:timestamp(),
    ok = update_prepare_clocks(Partition, SnapshotVC),
    ok = insert_prepared(Partition, TxId, Ts),
    Ts.

-spec decide_blue_ready(replica_id(), vclock()) -> ready | {not_ready, non_neg_integer()}.
decide_blue_ready(ReplicaId, CommitVC) ->
    Now = grb_time:timestamp(),
    Self = grb_vclock:get_time(ReplicaId, CommitVC),
    case Now >= Self of
        true ->
            ready;
        false ->
            %% todo(borja, stat): log miss
            {not_ready, grb_time:diff_ms(Now, Self)}
    end.

-spec decide_blue(partition_id(), term(), vclock()) -> ok.
decide_blue(Partition, TxId, CommitVC) ->
    case decide_blue_ready(grb_dc_manager:replica_id(), CommitVC) of
        ready ->
            erlang:send(process_pid(Partition), {decide_blue, TxId, CommitVC});
        {not_ready, DiffMs} ->
            erlang:send_after(DiffMs, process_pid(Partition), {decide_blue, TxId, CommitVC})
    end,
    ok.

-spec update_prepare_clocks(partition_id(), vclock()) -> ok.
-ifdef(STABLE_SNAPSHOT).
update_prepare_clocks(Partition, SnapshotVC) ->
    grb_propagation_vnode:merge_into_stable_vc(Partition, SnapshotVC).
-else.
update_prepare_clocks(Partition, SnapshotVC) ->
    grb_propagation_vnode:merge_into_uniform_vc(Partition, SnapshotVC),
    ok.
-endif.

-spec handle_replicate(partition_id(), replica_id(), #{}, vclock()) -> ok.
handle_replicate(Partition, SourceReplica, WS, VC) ->
    CommitTime = grb_vclock:get_time(SourceReplica, VC),
    KnownTime = grb_propagation_vnode:known_time(Partition, SourceReplica),
    case KnownTime < CommitTime of
        false ->
            ok; %% de-dup, we already received this
        true ->
            grb_dc_utils:vnode_command(Partition,
                                       {handle_remote_tx, SourceReplica, WS, CommitTime, VC},
                                       ?master)
    end.

-spec handle_replicate_array(partition_id(), replica_id(),
                             tx_entry(), tx_entry(),
                             tx_entry(), tx_entry()) -> ok.

handle_replicate_array(Partition, SourceReplica, Tx1, Tx2, Tx3, Tx4={_, VC}) ->
    CommitTime = grb_vclock:get_time(SourceReplica, VC),
    KnownTime = grb_propagation_vnode:known_time(Partition, SourceReplica),
    case KnownTime < CommitTime of
        false ->
            ok; %% de-dup, we already received this
        true ->
            grb_dc_utils:vnode_command(Partition,
                                       {handle_remote_tx_array, SourceReplica, Tx1, Tx2, Tx3, Tx4},
                                       ?master)
    end.

-spec handle_replicate_array(partition_id(), replica_id(),
                             tx_entry(), tx_entry(), tx_entry(), tx_entry(),
                             tx_entry(), tx_entry(), tx_entry(), tx_entry()) -> ok.

handle_replicate_array(Partition, SourceReplica, Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8={_, VC}) ->
    CommitTime = grb_vclock:get_time(SourceReplica, VC),
    KnownTime = grb_propagation_vnode:known_time(Partition, SourceReplica),
    case KnownTime < CommitTime of
        false ->
            ok; %% de-dup, we already received this
        true ->
            grb_dc_utils:vnode_command(Partition,
                                       {handle_remote_tx_array, SourceReplica, Tx1, Tx2, Tx3, Tx4},
                                       ?master),

            grb_dc_utils:vnode_command(Partition,
                                       {handle_remote_tx_array, SourceReplica, Tx5, Tx6, Tx7, Tx8},
                                       ?master)
    end.

-spec handle_red_transaction(partition_id(), term(), tx_label(), writeset(), vclock()) -> ok.
handle_red_transaction(Partition, TxId, Label, WS, VC) ->
    grb_dc_utils:vnode_command(Partition,
                               {handle_red_tx, TxId, Label, WS, VC},
                               ?master).

-spec handle_red_heartbeat(partition_id(), grb_time:ts()) -> ok.
handle_red_heartbeat(Partition, Ts) ->
    %% Route through oplog vnode to preserve causality with previous handle_red_tx messages
    %% If we call grb_propagation_vnode:handle_red_heartbeat/2 directly we could violate
    %% causality, since the latter updates an ETS table, which is made visible to other
    %% processes immediately.
    grb_dc_utils:vnode_command(Partition, {red_tx_done, Ts}, ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    ok = grb_dc_utils:register_vnode_pid(?master, Partition, self()),

    {ok, KeyLogSize} = application:get_env(grb, version_log_size),
    %% We're not using the timer:send_interval/2 or timer:send_after/2 functions for
    %% two reasons:
    %%
    %% - for timer:send_after/2, the timer is much more expensive to create, since
    %%   the timer is managed by an external process, and it can get overloaded
    %%
    %% - for timer:send_interval/2, messages will keep being sent even if we're
    %%   overloaded. Using the send_after / cancel_timer pattern, we can control
    %%   how far behind we fall, and we make sure we're always ready to handle an
    %%   event. We know, at least, that `BlueTickInterval` ms will occur between
    %%   events. If we were using timer:send_interval/2, if in one event we spend
    %%   more time than the specified interval, we are going to get pending jobs
    %%   in the process queue, and some events will be processed quicker. Since
    %%   we want to control the size of the queue, this allows us to do that.
    {ok, BlueTickInterval} = application:get_env(grb, self_blue_heartbeat_interval),
    NumReaders = application:get_env(grb, oplog_readers, ?OPLOG_READER_NUM),

    OpLogTable = ets:new(?OP_LOG_TABLE, [set, public, {read_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_TABLE}, OpLogTable),

    PendingOps = ets:new(?PENDING_TX_OPS, [ordered_set, public, {write_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?PENDING_TX_OPS}, PendingOps),

    LastKeyVC = ets:new(?OP_LOG_LAST_VC, [set, protected, {read_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_LAST_VC}, LastKeyVC),

    PreparedBlue = ets:new(?PREPARED_TABLE, [ordered_set, public, {write_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?PREPARED_TABLE}, PreparedBlue),

    PreparedIdx = ets:new(?PREPARED_TABLE_IDX, [set, public, {write_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?PREPARED_TABLE_IDX}, PreparedIdx),

    {ok, CheckStalledInterval} = application:get_env(grb, prepared_blue_stale_check_ms),

    State = #state{partition = Partition,
                   all_replicas=[], %% ok to do this, we'll overwrite it later
                   replicas_n=NumReaders,
                   prepared_blue=PreparedBlue,
                   prepared_blue_idx=PreparedIdx,
                   blue_tick_interval=BlueTickInterval,
                   stalled_blue_check_interval=CheckStalledInterval,
                   op_log_size = KeyLogSize,
                   op_log = OpLogTable,
                   op_last_vc = LastKeyVC,
                   pending_client_ops = PendingOps},

    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.op_log]),
    {reply, Ready, State};

handle_command({put_direct, WS}, Sender,
                State=#state{op_log=OpLog, op_log_size=Size, all_replicas=AllReplicas}) ->

    ok = put_direct_internal(WS, OpLog, Size, AllReplicas),
    if
        is_pid(Sender) ->
            Sender ! ok,
            {noreply, State};
        true ->
            {reply, ok, State}
    end;

handle_command({append_direct, WS}, Sender,
               State=#state{op_log=OpLog, op_log_size=Size, all_replicas=AllReplicas}) ->

    ok = append_direct_internal(WS, OpLog, Size, AllReplicas),
    if
        is_pid(Sender) ->
            Sender ! ok,
            {noreply, State};
        true ->
            {reply, ok, State}
    end;

handle_command(enable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=true}};

handle_command(disable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=false}};

handle_command(start_blue_hb_timer, _From, S = #state{partition=Partition,
                                                      blue_tick_interval=Int,
                                                      blue_tick_pid=undefined,
                                                      stalled_blue_check_interval=StalledInt,
                                                      stalled_blue_check_timer=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    PrepTable = prepared_blue_table(Partition),
    RawClockTable = grb_propagation_vnode:clock_table(Partition),
    TickProcess = erlang:spawn(fun Loop() ->
        erlang:send_after(Int, self(), ?blue_tick_req),
        receive
            ?kill_timer_req ->
                ok;
            ?blue_tick_req ->
                Ts = compute_new_known_time(PrepTable),
                grb_propagation_vnode:handle_blue_heartbeat_unsafe(ReplicaId, Ts, RawClockTable),
                Loop()
        end
    end),

    TimerRef = if
        StalledInt > 0 ->
            ?LOG_INFO(
                "Beware, ~p checking for stalled transactions every ~b milliseconds",
                [Partition, StalledInt]
            ),
            erlang:send_after(StalledInt, self(), ?blue_stall_check);
       true ->
           undefined
    end,

    {reply, ok, S#state{blue_tick_pid=TickProcess, stalled_blue_check_timer=TimerRef}};

handle_command(start_blue_hb_timer, _From, S = #state{blue_tick_pid=Pid}) when is_pid(Pid) ->
    {reply, ok, S};

handle_command(start_readers, _From, S = #state{partition=P,
                                                replicas_n=N}) ->

    Result = case grb_vnode_proxy:readers_ready(P, N) of
        true -> true;
        false ->
            ok = grb_vnode_proxy:start_readers(P, N),
            grb_vnode_proxy:readers_ready(P, N)
    end,
    {reply, Result, S};

handle_command(stop_blue_hb_timer, _From, S = #state{blue_tick_pid=undefined}) ->
    {noreply, S};

handle_command(stop_blue_hb_timer, _From, S = #state{blue_tick_pid=Pid,
                                                     stalled_blue_check_timer=Timer}) when is_pid(Pid) ->
    Pid ! ?kill_timer_req,
    if
        is_reference(Timer) ->
            erlang:cancel_timer(Timer);
        true ->
            ok
    end,
    {noreply, S#state{blue_tick_pid=undefined, stalled_blue_check_timer=undefined}};

handle_command(stop_readers, _From, S = #state{partition=P}) ->
    ok = grb_vnode_proxy:stop_readers(P),
    {noreply, S};

handle_command(readers_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = grb_vnode_proxy:readers_ready(P, N),
    {reply, Result, S};

handle_command({learn_all_replicas, Replicas}, _From, S) ->
    {noreply, S#state{all_replicas=Replicas}};

handle_command({handle_remote_tx, SourceReplica, WS, CommitTime, VC}, _From, State) ->
    ok = handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, State),
    {noreply, State};

handle_command({handle_remote_tx_array, SourceReplica, Tx1, Tx2, Tx3, Tx4}, _From, State) ->
    ok = handle_remote_tx_array_internal(SourceReplica, Tx1, Tx2, Tx3, Tx4, State),
    {noreply, State};

handle_command({handle_red_tx, TxId, Label, WS, VC}, _From, S=#state{all_replicas=AllReplicas,
                                                                     op_log=OperationLog,
                                                                     op_log_size=LogSize,
                                                                     op_last_vc=LastVC,
                                                                     pending_client_ops=PendingOpsTable}) ->
    ok = clean_transaction_ops_with_table(PendingOpsTable, TxId),
    ok = append_red_writeset(AllReplicas, Label, WS, VC, OperationLog, LogSize, LastVC),
    {noreply, S};

handle_command({red_tx_done, Ts}, _From, S=#state{partition=Partition}) ->
    grb_propagation_vnode:handle_red_heartbeat(Partition, Ts),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

handle_info({decide_blue, TxId, VC}, State) ->
    ok = decide_blue_internal(TxId, VC, State),
    {ok, State};

handle_info(?blue_stall_check, State=#state{partition=Partition,
                                            prepared_blue=PreparedBlue,
                                            prepared_blue_idx=PreparedBlueIdx,
                                            stalled_blue_check_timer=Timer,
                                            stalled_blue_check_interval=Interval}) ->
    ?CANCEL_TIMER_FAST(Timer),
    ok = remove_stalled_transactions(Partition, PreparedBlue, PreparedBlueIdx, Interval),
    {ok, State#state{stalled_blue_check_timer=erlang:send_after(Interval, self(), ?blue_stall_check)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec insert_prepared(partition_id(), term(), grb_time:ts()) -> ok.
insert_prepared(Partition, TxId, PrepareTime) ->
    true = ets:insert(prepared_blue_table(Partition),
                      {{PrepareTime, TxId}}),

    true = ets:insert_new(prepared_blue_idx_table(Partition),
                          {TxId, PrepareTime}),

    ok.

-spec handle_remote_tx_internal(replica_id(), #{}, grb_time:ts(), vclock(), state()) -> ok.
-ifdef(NO_FWD_REPLICATION).
handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, #state{all_replicas=AllReplicas,
                                                                    partition=Partition,
                                                                    op_log=OperationLog,
                                                                    op_log_size=LogSize}) ->
    ok = append_writeset(AllReplicas, WS, VC, OperationLog, LogSize),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, CommitTime),
    ok.

-else.

handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, #state{all_replicas=AllReplicas,
                                                                    partition=Partition,
                                                                    op_log=OperationLog,
                                                                    op_log_size=LogSize}) ->
    ok = append_writeset(AllReplicas, WS, VC, OperationLog, LogSize),
    ok = grb_propagation_vnode:append_remote_blue_commit(SourceReplica, Partition, CommitTime, WS, VC),
    ok.

-endif.

-spec handle_remote_tx_array_internal(replica_id(), tx_entry(), tx_entry(), tx_entry(), tx_entry(), state()) -> ok.
-ifdef(NO_FWD_REPLICATION).

handle_remote_tx_array_internal(SourceReplica, {WS1, VC1}, {WS2, VC2}, {WS3, VC3}, {WS4, VC4},
        #state{all_replicas=AllReplicas, partition=Partition, op_log=OperationLog, op_log_size=LogSize}) ->

    ok = append_writeset(AllReplicas, WS1, VC1, OperationLog, LogSize),
    ok = append_writeset(AllReplicas, WS2, VC2, OperationLog, LogSize),
    ok = append_writeset(AllReplicas, WS3, VC3, OperationLog, LogSize),
    ok = append_writeset(AllReplicas, WS4, VC4, OperationLog, LogSize),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, grb_vclock:get_time(SourceReplica, VC4)),
    ok.

-else.

handle_remote_tx_array_internal(SourceReplica, {WS1, VC1}, {WS2, VC2}, {WS3, VC3}, {WS4, VC4},
        #state{all_replicas=AllReplicas, partition=Partition, op_log=OperationLog, op_log_size=LogSize}) ->

    ok = append_writeset(AllReplicas, WS1, VC1, OperationLog, LogSize),
    ok = append_writeset(AllReplicas, WS2, VC2, OperationLog, LogSize),
    ok = append_writeset(AllReplicas, WS3, VC3, OperationLog, LogSize),
    ok = append_writeset(AllReplicas, WS4, VC4, OperationLog, LogSize),

    ok = grb_propagation_vnode:append_remote_blue_commit_no_hb(SourceReplica, Partition, WS1, VC1),
    ok = grb_propagation_vnode:append_remote_blue_commit_no_hb(SourceReplica, Partition, WS2, VC2),
    ok = grb_propagation_vnode:append_remote_blue_commit_no_hb(SourceReplica, Partition, WS3, VC3),
    ok = grb_propagation_vnode:append_remote_blue_commit(SourceReplica,
                                                         Partition,
                                                         grb_vclock:get_time(SourceReplica, VC4),
                                                         WS4,
                                                         VC4),
    ok.

-endif.

-spec decide_blue_internal(term(), vclock(), state()) -> ok.
decide_blue_internal(TxId, VC, #state{all_replicas=AllReplicas,
                                      partition=SelfPartition,
                                      op_log=OpLog,
                                      op_log_size=LogSize,
                                      pending_client_ops=PendingOps,
                                      prepared_blue=PreparedBlue,
                                      prepared_blue_idx=PreparedIdx,
                                      should_append_commit=ShouldAppend}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    WS = take_transaction_writeset(PendingOps, TxId),
    ok = append_writeset(AllReplicas, WS, VC, OpLog, LogSize),
    case ShouldAppend of
        false -> ok;
        true -> grb_propagation_vnode:append_blue_commit(SelfPartition, WS, VC)
    end,
    ok = remove_from_prepared(PreparedBlue, PreparedIdx, TxId),
    ok.

-spec take_transaction_writeset(cache_id(), term()) -> writeset().
take_transaction_writeset(PendingOps, TxId) ->
    Tuples = ets:select(PendingOps, [{ {{TxId, '$1'}, '$2'}, [], [{{'$1', '$2'}}] }]),
    lists:foldl(fun({Key, Op}, WS) ->
        true = ets:delete(PendingOps, {TxId, Key}),
        WS#{Key => Op}
    end, #{}, Tuples).

-spec remove_from_prepared(cache_id(), cache_id(), term()) -> ok.
remove_from_prepared(PreparedBlue, PreparedBlueIdx, TxId) ->
    case ets:take(PreparedBlueIdx, TxId) of
        [{TxId, PrepTime}] ->
            true = ets:delete(PreparedBlue, {PrepTime, TxId}),
            ok;
        _ ->
            ok
    end.

-spec append_writeset(AtReplicas :: [all_replica_id()],
                      WS :: writeset(),
                      CommitVC :: vclock(),
                      OpLog :: op_log(),
                      DefaultSize :: non_neg_integer()) -> ok.

append_writeset(AtReplicas, WS, CommitVC, OpLog, DefaultSize) ->
    Objects = maps:fold(fun(Key, Operation, Acc) ->
        Log = append_to_log(AtReplicas, Key, Operation, CommitVC, OpLog, DefaultSize),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.

-spec append_red_writeset(AtReplicas :: [all_replica_id()],
                          Label :: tx_label(),
                          WS :: writeset(),
                          CommitVC :: vclock(),
                          OpLog :: op_log(),
                          DefaultSize :: non_neg_integer(),
                          LastVC :: last_vc()) -> ok.

append_red_writeset(AtReplicas, Label, WS, CommitVC, OpLog, DefaultSize, LastVC) ->
    Objects = maps:fold(fun(Key, Operation, Acc) ->
        Log = append_to_log(AtReplicas, Key, Operation, CommitVC, OpLog, DefaultSize),
        ok = update_last_vc(Key, Label, AtReplicas, CommitVC, LastVC),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.

-spec append_to_log(AllReplicas :: [all_replica_id()],
                    Key :: key(),
                    Operation :: operation(),
                    CommitVC :: vclock(),
                    OpLog :: op_log(),
                    Size ::non_neg_integer()) -> grb_version_log:t().

append_to_log(AllReplicas, Key, Operation, CommitVC, OpLog, Size) ->
    Log = case ets:lookup(OpLog, Key) of
        [{Key, PrevLog}] ->
            PrevLog;
        [] ->
            Type = grb_crdt:op_type(Operation),
            TypeBase = grb_crdt:new(Type),
            grb_version_log:new(Type, TypeBase, AllReplicas, Size)
    end,
    grb_version_log:insert(Operation, CommitVC, Log).


%% LastVC contains, for each key and transaction label, its max commit vector.
%% Although maxing two vectors on each transaction could be slow, this only happens on red transactions,
%% which are already slow due to cross-dc 2PC. Adding a little bit of time doing this max shouldn't add
%% too much overhead on top.
-spec update_last_vc(Key :: key(),
                     Label :: tx_label(),
                     AtReplicas :: [replica_id()],
                     CommitVC :: vclock(),
                     LastVC :: last_vc()) -> ok.

update_last_vc(Key, Label, AtReplicas, CommitVC, LastVC) ->
    case ets:lookup(LastVC, {Key, Label}) of
        [{_, LastCommitVC}] ->
            true = ets:update_element(LastVC, {Key, Label},
                                      {2, grb_vclock:max_at_keys(AtReplicas, LastCommitVC, CommitVC)});
        [] ->
            true = ets:insert(LastVC, {{Key, Label}, CommitVC})
    end,
    ok.

-spec compute_new_known_time(cache_id()) -> grb_time:ts().
compute_new_known_time(PreparedBlue) ->
    case ets:first(PreparedBlue) of
        '$end_of_table' ->
            grb_time:timestamp();
        {Ts, _} ->
            ?LOG_DEBUG("knownVC[d] = min_prep (~b - 1)", [Ts]),
            Ts - 1
    end.

-spec remove_stalled_transactions(partition_id(), cache_id(), cache_id(), non_neg_integer()) -> ok.
remove_stalled_transactions(Partition, PreparedBlue, PreparedBlueIdx, After) ->
    remove_stalled_transactions(
        ets:first(PreparedBlue),
        PreparedBlue,
        PreparedBlueIdx,
        grb_time:timestamp(),
        After,
        Partition
    ).

-spec remove_stalled_transactions(Key :: {grb_time:ts(), term()} | '$end_of_table',
                                  PreparedBlue :: cache_id(),
                                  PreparedBlueIdx :: cache_id(),
                                  Now :: grb_time:ts(),
                                  After :: non_neg_integer(),
                                  AtPartition :: partition_id()) -> ok.

remove_stalled_transactions(Key={Ts, TxId}, PreparedBlue, PreparedBlueIdx, Now, After, AtPartition)
    when (Ts < Now) andalso ((Now - Ts) > After * 1000) ->
        ok = grb_measurements:log_counter({?MODULE, AtPartition, ?FUNCTION_NAME}),
        %% Transaction is old, and more than Interval ms have passed since this
        %% transaction was prepared. We can remove this transaction, since it will
        %% probably never be decided (client bailed out)
        true = ets:delete(PreparedBlueIdx, TxId),
        true = ets:delete(PreparedBlue, Key),
        remove_stalled_transactions(ets:first(PreparedBlue), PreparedBlue, PreparedBlueIdx, Now, After, AtPartition);

remove_stalled_transactions(_Key, _PrepBlue, _PrepBlueIdx, _Now, _After, _AtPartition) ->
    %% we reached the end of the table, or the transaction is fresh enough.
    ok.

%%%===================================================================
%%% Util Functions
%%%===================================================================

-spec is_ready(cache_id()) -> boolean().
is_ready(Table) ->
    undefined =/= ets:info(Table).

%%%===================================================================
%%% stub riak_core callbacks
%%%===================================================================

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handoff_starting(_, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_, State) ->
    {ok, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_data(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{op_log=OpLog}) ->
    try ets:delete(OpLog) catch _:_ -> ok end,
    ok.

delete(State=#state{op_log=OpLog}) ->
    try ets:delete(OpLog) catch _:_ -> ok end,
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.

-ifdef(TEST).

grb_oplog_vnode_compute_new_known_time_test() ->
    _ = ets:new(?PREPARED_TABLE, [ordered_set, named_table]),
    _ = ets:new(?PREPARED_TABLE_IDX, [set, named_table]),

    Entries = [{tx_1, 1}, {tx_2, 3}, {tx_3, 5}, {tx_4, 10}, {tx_5, 50}],
    [begin
        true = ets:insert(?PREPARED_TABLE, {{PrepTime, Tx}}),
        true = ets:insert(?PREPARED_TABLE_IDX, {Tx, PrepTime})
    end || {Tx, PrepTime} <- Entries],

    ?assertEqual(0, compute_new_known_time(?PREPARED_TABLE)),

    %% If we remove the lowest, now tx_2 is the lowest tx in the queue
    ?assertMatch(ok, remove_from_prepared(?PREPARED_TABLE, ?PREPARED_TABLE_IDX, tx_1)),
    ?assertEqual(2, compute_new_known_time(?PREPARED_TABLE)),

    %% tx_3 was removed earlier, but it has a higher ts than tx_2, so tx_2 is still the lowest
    ?assertMatch(ok, remove_from_prepared(?PREPARED_TABLE, ?PREPARED_TABLE_IDX, tx_3)),
    ?assertEqual(2, compute_new_known_time(?PREPARED_TABLE)),

    %% now, tx_4 is the next in the queue, at ts 10-1
    ?assertMatch(ok, remove_from_prepared(?PREPARED_TABLE, ?PREPARED_TABLE_IDX, tx_2)),
    ?assertEqual(9, compute_new_known_time(?PREPARED_TABLE)),

    %% same with tx_5
    ?assertMatch(ok, remove_from_prepared(?PREPARED_TABLE, ?PREPARED_TABLE_IDX, tx_4)),
    ?assertEqual(49, compute_new_known_time(?PREPARED_TABLE)),

    ?assertMatch(ok, remove_from_prepared(?PREPARED_TABLE, ?PREPARED_TABLE_IDX, tx_5)),
    %% Now that the queue is empty, the time is the current clock
    Ts = grb_time:timestamp(),
    Lowest = compute_new_known_time(?PREPARED_TABLE),
    ?assert(Ts =< Lowest),

    ets:delete(?PREPARED_TABLE),
    ets:delete(?PREPARED_TABLE_IDX),
    ok.

-endif.
