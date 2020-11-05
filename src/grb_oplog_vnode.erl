-module(grb_oplog_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Management API
-export([stop_blue_hb_timer_all/0,
         start_readers_all/0,
         stop_readers_all/0,
         start_writers_all/0,
         stop_writers_all/0]).

%% ETS table API
-export([op_log_table/1,
         op_log_table_size/1,
         last_vc_table/1,
         append_key_update_with_table/8]).

%% Public API
-export([get_key_version/3,
         get_key_version_with_table/3,
         prepare_blue/4,
         decide_blue_ready/2,
         decide_blue/3,
         handle_replicate/4,
         handle_replicate_array/6,
         handle_replicate_array/10,
         handle_red_transaction/3]).

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

-define(OP_LOG_TABLE, op_log_table).
-define(OP_LOG_TABLE_SIZE, op_log_table_size).
-define(OP_LOG_LAST_VC, op_log_last_vc_table).
-define(PREPARED_TABLE, prepared_blue_table).

-type op_log() :: cache(key(), grb_version_log:t()).
-type last_vc() :: cache(key(), vclock()).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n :: non_neg_integer(),
    writers_n :: non_neg_integer(),

    prepared_blue :: cache_id(),

    blue_tick_interval :: non_neg_integer(),
    blue_tick_pid = undefined :: pid() | undefined,

    %% todo(borja, crdt): change type of op_log when adding crdts
    op_log_size :: non_neg_integer(),
    op_log :: op_log(),
    op_last_vc :: last_vc(),

    %% It doesn't make sense to append it if we're not connected to other clusters
    should_append_commit = true :: boolean()
}).

%% How a prepared blue transaction is structured,
%% the key is a tuple of prepare time and transaction id.
%% Since the ETS table is ordered, lower prepare times will go
%% at the beggining of the table. A call to ets:first will get us
%% the lower prepare timestamp in the table.
-record(prepared_record, {
    key :: {grb_time:ts(), term()},
    writeset :: #{}
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

-spec start_writers_all() -> ok | error.
start_writers_all() ->
    Results = [try
    riak_core_vnode_master:sync_command(N, start_writers, ?master, 1000)
    catch
        _:_ -> false
    end || N <- grb_dc_utils:get_index_nodes() ],
    case lists:all(fun(Result) -> Result end, Results) of
        true ->
            ok;
        false ->
            error
    end.

-spec stop_writers_all() -> ok.
stop_writers_all() ->
    [try
        riak_core_vnode_master:command(N, stop_writers, ?master)
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

-spec op_log_table_size(partition_id()) -> non_neg_integer().
op_log_table_size(Partition) ->
    persistent_term:get({?MODULE, Partition, ?OP_LOG_TABLE_SIZE}).

-spec last_vc_table(partition_id()) -> last_vc().
last_vc_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?OP_LOG_LAST_VC}).

-spec prepared_blue_table(partition_id()) -> cache_id().
prepared_blue_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?PREPARED_TABLE}).

-spec append_key_update_with_table(TxType :: transaction_type(),
                                   Key :: key(),
                                   Value :: val(),
                                   CommitVC :: vclock(),
                                   OpLog :: cache_id(),
                                   LogSize :: non_neg_integer(),
                                   LastVC :: cache_id(),
                                   Replicas :: [replica_id()]) -> ok.

-ifdef(BLUE_KNOWN_VC).
append_key_update_with_table(TxType, Key, Value, CommitVC, OpLog, LogSize, _LastVC, _ActiveReplicas) ->
    NewLog = append_to_log(TxType, Key, Value, CommitVC, OpLog, LogSize),
    true = ets:insert(OpLog, {Key, NewLog}),
    ok.
-else.
append_key_update_with_table(TxType, Key, Value, CommitVC, OpLog, LogSize, LastVC, ActiveReplicas) ->
    NewLog = append_to_log(TxType, Key, Value, CommitVC, OpLog, LogSize),
    true = ets:insert(OpLog, {Key, NewLog}),
    ok = update_last_vc(TxType, Key, ActiveReplicas, CommitVC, LastVC),
    ok.
-endif.

%%%===================================================================
%%% API
%%%===================================================================

-spec get_key_version(partition_id(), key(), vclock()) -> {ok, val()}.
get_key_version(Partition, Key, SnapshotVC) ->
    get_key_version_with_table(op_log_table(Partition), Key, SnapshotVC).

%% todo(borja, crdts): Should use LWW, aggregate operations on top of given op
%%
%% Right now it only reads the last version below SnapshotVC, but it should aggregate
%% the chosen operations on top of the given value (or operation)
-spec get_key_version_with_table(cache_id(), key(), vclock()) -> {ok, val()}.
get_key_version_with_table(OpLogTable, Key, SnapshotVC) ->
    Bottom = grb_dc_utils:get_default_bottom_value(),
    case ets:lookup(OpLogTable, Key) of
        [] ->
            {ok, Bottom};

        [{Key, VersionLog}] ->
            case grb_version_log:get_first_lower(SnapshotVC, VersionLog) of
                undefined ->
                    {ok, Bottom};

                {_, LastValue, _LastCommitVC} ->
                    %% todo(borja, efficiency): Remove clock from return if we don't use it
                    {ok, LastValue}
            end
    end.

-spec prepare_blue(partition_id(), term(), #{}, vclock()) -> grb_time:ts().
prepare_blue(Partition, TxId, WriteSet, SnapshotVC) ->
    Ts = grb_time:timestamp(),
    ok = update_prepare_clocks(Partition, SnapshotVC),
    ok = insert_prepared(Partition, TxId, WriteSet, Ts),
    Ts.

-spec decide_blue_ready(replica_id(), vclock()) -> ready | not_ready.
decide_blue_ready(ReplicaId, CommitVC) ->
    Self = grb_vclock:get_time(ReplicaId, CommitVC),
    case grb_time:timestamp() >= Self of
        true -> ready;
        false -> not_ready %% todo(borja, stat): log miss
    end.

-spec decide_blue(partition_id(), term(), vclock()) -> ok.
decide_blue(Partition, TxId, CommitVC) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {decide_blue, TxId, CommitVC},
                                        ?master,
                                        infinity).

-spec update_prepare_clocks(partition_id(), vclock()) -> ok.
-ifdef(BASIC_REPLICATION).
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
            riak_core_vnode_master:command({Partition, node()},
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
            riak_core_vnode_master:command({Partition, node()},
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
            riak_core_vnode_master:command({Partition, node()},
                                           {handle_remote_tx_array, SourceReplica, Tx1, Tx2, Tx3, Tx4},
                                           ?master),

            riak_core_vnode_master:command({Partition, node()},
                                           {handle_remote_tx_array, SourceReplica, Tx5, Tx6, Tx7, Tx8},
                                           ?master)
    end.

-spec handle_red_transaction(partition_id(), writeset(), vclock()) -> ok.
handle_red_transaction(Partition, WS, VC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {handle_red_tx, WS, VC},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, KeyLogSize} = application:get_env(grb, version_log_size),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_TABLE_SIZE}, KeyLogSize),
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
    NumWriters = application:get_env(grb, oplog_writers, ?OPLOG_WRITER_NUM),

    OpLogTable = ets:new(?OP_LOG_TABLE, [set, protected, {read_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_TABLE}, OpLogTable),

    LastKeyVC = ets:new(?OP_LOG_LAST_VC, [set, protected, {read_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_LAST_VC}, LastKeyVC),

    PreparedBlue = ets:new(?PREPARED_TABLE, [ordered_set, public, {write_concurrency, true}, {keypos, #prepared_record.key}]),
    ok = persistent_term:put({?MODULE, Partition, ?PREPARED_TABLE}, PreparedBlue),

    State = #state{partition = Partition,
                   replicas_n=NumReaders,
                   writers_n=NumWriters,
                   prepared_blue=PreparedBlue,
                   blue_tick_interval=BlueTickInterval,
                   op_log_size = KeyLogSize,
                   op_log = OpLogTable,
                   op_last_vc= LastKeyVC},

    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.op_log]),
    {reply, Ready, State};

handle_command(enable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=true}};

handle_command(disable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=false}};

handle_command(start_blue_hb_timer, _From, S = #state{partition=Partition,
                                                      blue_tick_interval=Int,
                                                      blue_tick_pid=undefined}) ->
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
    {reply, ok, S#state{blue_tick_pid=TickProcess}};

handle_command(start_blue_hb_timer, _From, S = #state{blue_tick_pid=Pid}) when is_pid(Pid) ->
    {reply, ok, S};

handle_command(start_readers, _From, S = #state{partition=P,
                                                 replicas_n=N}) ->

    Result = case grb_oplog_reader:readers_ready(P, N) of
        true -> true;
        false ->
            ok = grb_oplog_reader:start_readers(P, N),
            grb_oplog_reader:readers_ready(P, N)
    end,
    {reply, Result, S};

handle_command(start_writers, _From, S = #state{partition=P,
                                                writers_n=N}) ->

    Result = case grb_oplog_writer:writers_ready(P, N) of
        true -> true;
        false ->
            ok = grb_oplog_writer:start_writers(P, N),
            grb_oplog_writer:writers_ready(P, N)
    end,
    {reply, Result, S};

handle_command(stop_blue_hb_timer, _From, S = #state{blue_tick_pid=undefined}) ->
    {noreply, S};

handle_command(stop_blue_hb_timer, _From, S = #state{blue_tick_pid=Pid}) when is_pid(Pid) ->
    Pid ! ?kill_timer_req,
    {noreply, S#state{blue_tick_pid=undefined}};

handle_command(stop_readers, _From, S = #state{partition=P}) ->
    ok = grb_oplog_reader:stop_readers(P),
    {noreply, S};

handle_command(stop_writers, _From, S = #state{partition=P}) ->
    ok = grb_oplog_writer:stop_writers(P),
    {noreply, S};

handle_command(readers_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = grb_oplog_reader:readers_ready(P, N),
    {reply, Result, S};

handle_command(writers_ready, _From, S = #state{partition=P, writers_n=N}) ->
    Result = grb_oplog_writer:writers_ready(P, N),
    {reply, Result, S};

handle_command({decide_blue, TxId, VC}, _From, State) ->
    ok = decide_blue_internal(TxId, VC, State),
    {reply, ok, State};

handle_command({handle_remote_tx, SourceReplica, WS, CommitTime, VC}, _From, State) ->
    ok = handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, State),
    {noreply, State};

handle_command({handle_remote_tx_array, SourceReplica, Tx1, Tx2, Tx3, Tx4}, _From, State) ->
    ok = handle_remote_tx_array_internal(SourceReplica, Tx1, Tx2, Tx3, Tx4, State),
    {noreply, State};

handle_command({handle_red_tx, WS, VC}, _From, S=#state{op_log=OperationLog,
                                                        op_log_size=LogSize,
                                                        op_last_vc=LastVC}) ->
    ok = update_partition_state(?RED_REPLICA, WS, VC, OperationLog, LogSize, LastVC),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec insert_prepared(partition_id(), term(), writeset(), grb_time:ts()) -> ok.
insert_prepared(Partition, TxId, WriteSet, PrepareTime) ->
    true = ets:insert(prepared_blue_table(Partition),
                      #prepared_record{key={PrepareTime, TxId}, writeset=WriteSet}),
    ok.

-spec handle_remote_tx_internal(replica_id(), #{}, grb_time:ts(), vclock(), state()) -> ok.
-ifdef(NO_REMOTE_APPEND).
handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, #state{partition=Partition,
                                                                       op_log=OperationLog,
                                                                       op_log_size=LogSize,
                                                                       op_last_vc=LastVC}) ->
    ok = update_partition_state(WS, VC, OperationLog, LogSize, LastVC),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, CommitTime),
    ok.

-else.

handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, #state{partition=Partition,
                                                                          op_log=OperationLog,
                                                                          op_log_size=LogSize,
                                                                          op_last_vc=LastVC}) ->
    ok = update_partition_state(WS, VC, OperationLog, LogSize, LastVC),
    ok = grb_propagation_vnode:append_remote_blue_commit(SourceReplica, Partition, CommitTime, WS, VC),
    ok.

-endif.

-spec handle_remote_tx_array_internal(replica_id(), tx_entry(), tx_entry(), tx_entry(), tx_entry(), state()) -> ok.
-ifdef(NO_REMOTE_APPEND).

handle_remote_tx_array_internal(SourceReplica, {WS1, VC1}, {WS2, VC2}, {WS3, VC3}, {WS4, VC4},
        #state{partition=Partition, op_log=OperationLog, op_log_size=LogSize, op_last_vc=LastVC}) ->

    ok = update_partition_state(WS1, VC1, OperationLog, LogSize, LastVC),
    ok = update_partition_state(WS2, VC2, OperationLog, LogSize, LastVC),
    ok = update_partition_state(WS3, VC3, OperationLog, LogSize, LastVC),
    ok = update_partition_state(WS4, VC4, OperationLog, LogSize, LastVC),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, grb_vclock:get_time(SourceReplica, VC4)),
    ok.

-else.

handle_remote_tx_array_internal(SourceReplica, {WS1, VC1}, {WS2, VC2}, {WS3, VC3}, {WS4, VC4},
        #state{partition=Partition, op_log=OperationLog, op_log_size=LogSize, op_last_vc=LastVC}) ->

    ok = update_partition_state(WS1, VC1, OperationLog, LogSize, LastVC),
    ok = update_partition_state(WS2, VC2, OperationLog, LogSize, LastVC),
    ok = update_partition_state(WS3, VC3, OperationLog, LogSize, LastVC),
    ok = update_partition_state(WS4, VC4, OperationLog, LogSize, LastVC),

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
%% Caused by get_prepared_writeset/2
-dialyzer({no_return, decide_blue_internal/3}).
decide_blue_internal(TxId, VC, #state{partition=SelfPartition,
                                      op_log=OpLog,
                                      op_log_size=LogSize,
                                      op_last_vc=LastVC,
                                      prepared_blue=PreparedBlue,
                                      should_append_commit=ShouldAppend}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    {ok, WS, PreparedAt} = get_prepared_writeset(PreparedBlue, TxId),
    ok = update_partition_state(WS, VC, OpLog, LogSize, LastVC),
    ok = remove_from_prepared(TxId, PreparedAt, PreparedBlue),
    KnownTime = compute_new_known_time(PreparedBlue),
    case ShouldAppend of
        true ->
            grb_propagation_vnode:append_blue_commit(SelfPartition, KnownTime, WS, VC);
        false ->
            grb_propagation_vnode:handle_self_blue_heartbeat(SelfPartition, KnownTime)
    end,
    ok.

-spec get_prepared_writeset(cache_id(), term()) -> {ok, writeset(), grb_time:ts()} | false.
%% Dialyzer and record matches, the return
-dialyzer({nowarn_function, get_prepared_writeset/2}).
get_prepared_writeset(PreparedBlue, TxId) ->
    case ets:select(PreparedBlue, [{ #prepared_record{key={'$1', TxId}, writeset='$2'}, [], [{{'$1', '$2'}}] }]) of
        [{PrepareTs, WS}] ->
            {ok, WS, PrepareTs};
        _ ->
            %% todo(borja): Warn if more than one result?
            false
    end.

-spec remove_from_prepared(term(), grb_time:ts(), cache_id()) -> ok.
%% Caused by get_prepared_writeset/2 on decide_internal
-dialyzer({no_unused, remove_from_prepared/3}).
remove_from_prepared(TxId, PrepareTime, PreparedBlue) ->
    true = ets:delete(PreparedBlue, {PrepareTime, TxId}),
    ok.

-spec update_partition_state(WS :: #{},
                             CommitVC :: vclock(),
                             OpLog :: op_log(),
                             DefaultSize :: non_neg_integer(),
                             LastVC :: last_vc()) -> ok.

update_partition_state(WS, CommitVC, OpLog, DefaultSize, LastVC) ->
    update_partition_state(blue, WS, CommitVC, OpLog, DefaultSize, LastVC).

-spec update_partition_state(TxType :: transaction_type(),
                             WS :: #{},
                             CommitVC :: vclock(),
                             OpLog :: op_log(),
                             DefaultSize :: non_neg_integer(),
                             LastVC :: last_vc()) -> ok.

-ifdef(BLUE_KNOWN_VC).
update_partition_state(TxType, WS, CommitVC, OpLog, DefaultSize, _LastVC) ->
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = append_to_log(TxType, Key, Value, CommitVC, OpLog, DefaultSize),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.
-else.
update_partition_state(TxType, WS, CommitVC, OpLog, DefaultSize, LastVC) ->
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = append_to_log(TxType, Key, Value, CommitVC, OpLog, DefaultSize),
        ok = update_last_vc(TxType, Key, AllReplicas, CommitVC, LastVC),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.

%% LastVC contains, for each key, its max commit vector.
%% Although maxing two vectors on each transaction could be slow, this only happens on red transactions,
%% which are already slow due to cross-dc 2PC. Adding a little bit of time doing this max shouldn't add
%% too much overhead on top.
-spec update_last_vc(TxType :: transaction_type(),
                      Key :: key(),
                      AtReplicas :: [replica_id()],
                      CommitVC :: vclock(),
                      LastVC :: last_vc()) -> ok.

-ifndef('RED_BLUE_CONFLICT').
update_last_vc(blue, _, _, _, _) ->
    ok;
update_last_vc(red, Key, AtReplicas, CommitVC, LastVC) ->
    update_last_vc_log(Key, AtReplicas, CommitVC, LastVC).
-else.
update_last_vc(_TxType, Key, AtReplicas, CommitVC, LastVC) ->
    update_last_vc_log(Key, AtReplicas, CommitVC, LastVC).
-endif.

-spec update_last_vc_log(Key :: key(),
                         AtReplicas :: [replica_id()],
                         CommitVC :: vclock(),
                         LastVC :: last_vc()) -> ok.

update_last_vc_log(Key, AtReplicas, CommitVC, LastVC) ->
    case ets:lookup(LastVC, Key) of
        [{Key, LastCommitVC}] ->
            true = ets:update_element(LastVC, Key,
                                      {2, grb_vclock:max_at_keys(AtReplicas, LastCommitVC, CommitVC)});
        [] ->
            true = ets:insert(LastVC, {Key, CommitVC})
    end,
    ok.

-endif.

-spec append_to_log(Type :: transaction_type(),
                    Key :: key(),
                    Value :: val(),
                    CommitVC :: vclock(),
                    OpLog :: op_log(),
                    Size ::non_neg_integer()) -> grb_version_log:t().

append_to_log(Type, Key, Value, CommitVC, OpLog, Size) ->
    Log = case ets:lookup(OpLog, Key) of
        [{Key, PrevLog}] -> PrevLog;
        [] -> grb_version_log:new(Size)
    end,
    grb_version_log:append({Type, Value, CommitVC}, Log).

-spec compute_new_known_time(cache_id()) -> grb_time:ts().
compute_new_known_time(PreparedBlue) ->
    case ets:first(PreparedBlue) of
        '$end_of_table' ->
            grb_time:timestamp();
        {Ts, _} ->
            ?LOG_DEBUG("knownVC[d] = min_prep (~b - 1)", [Ts]),
            Ts - 1
    end.

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

-spec get_prepared_remove(cache_id(), term()) -> {ok, writeset()}.
get_prepared_remove(PreparedTable, TxId) ->
    {ok, WS, Time} = get_prepared_writeset(PreparedTable, TxId),
    ok = remove_from_prepared(TxId, Time, PreparedTable),
    {ok, WS}.

grb_oplog_vnode_compute_new_known_time_test() ->
    _ = ets:new(?PREPARED_TABLE, [ordered_set, named_table, {keypos, #prepared_record.key}]),
    true = ets:insert(?PREPARED_TABLE, [
        #prepared_record{key={1, tx_1}, writeset=#{}},
        #prepared_record{key={3, tx_2}, writeset=#{}},
        #prepared_record{key={10, tx_4}, writeset=#{}},
        #prepared_record{key={50, tx_5}, writeset=#{}},
        #prepared_record{key={5, tx_3}, writeset=#{}}
    ]),

    ?assertEqual(0, compute_new_known_time(?PREPARED_TABLE)),

    %% If we remove the lowest, now tx_2 is the lowest tx in the queue
    ?assertEqual({ok, #{}}, get_prepared_remove(?PREPARED_TABLE, tx_1)),
    ?assertEqual(2, compute_new_known_time(?PREPARED_TABLE)),

    %% tx_3 was removed earlier, but it has a higher ts than tx_2, so tx_2 is still the lowest
    ?assertEqual({ok, #{}}, get_prepared_remove(?PREPARED_TABLE, tx_3)),
    ?assertEqual(2, compute_new_known_time(?PREPARED_TABLE)),

    %% now, tx_4 is the next in the queue, at ts 10-1
    ?assertEqual({ok, #{}}, get_prepared_remove(?PREPARED_TABLE, tx_2)),
    ?assertEqual(9, compute_new_known_time(?PREPARED_TABLE)),

    %% same with tx_5
    ?assertEqual({ok, #{}}, get_prepared_remove(?PREPARED_TABLE, tx_4)),
    ?assertEqual(49, compute_new_known_time(?PREPARED_TABLE)),

    ?assertEqual({ok, #{}}, get_prepared_remove(?PREPARED_TABLE, tx_5)),
    %% Now that the queue is empty, the time is the current clock
    Ts = grb_time:timestamp(),
    Lowest = compute_new_known_time(?PREPARED_TABLE),
    ?assert(Ts < Lowest),

    ets:delete(?PREPARED_TABLE),
    ok.

-endif.
