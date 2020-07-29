-module(grb_main_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Public API
-export([cache_name/2,
         prepare_blue/4,
         decide_blue/4,
         handle_replicate/5]).

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

-define(master, grb_main_vnode_master).
-define(blue_tick_req, blue_tick_event).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n = ?READ_CONCURRENCY :: non_neg_integer(),

    prepared_blue :: #{any() => {#{}, vclock()}},

    blue_tick_interval :: non_neg_integer(),
    blue_tick_timer = undefined :: reference() | undefined,

    %% todo(borja, crdt): change type of op_log when adding crdts
    op_log_size :: non_neg_integer(),
    op_log :: cache(key(), cache(key(), grb_version_log:t())),
    default_bottom_value = <<>> :: term(),
    default_bottom_red = 0 :: grb_time:ts()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec prepare_blue(partition_id(), term(), #{}, vclock()) -> grb_time:ts().
prepare_blue(Partition, TxId, WriteSet, SnapshotVC) ->
    Ts = grb_time:timestamp(),
    ok = update_prepare_clocks(Partition, SnapshotVC),
    ok = riak_core_vnode_master:command({Partition, node()},
                                        {prepare_blue, TxId, WriteSet, Ts},
                                        ?master),
    Ts.

-spec decide_blue(partition_id(), replica_id(), term(), vclock()) -> ok.
decide_blue(Partition, ReplicaId, TxId, CommitVC) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {decide_blue, ReplicaId, TxId, CommitVC},
                                        ?master,
                                        infinity).

-spec update_prepare_clocks(partition_id(), vclock()) -> ok.
-ifdef(BASIC_REPLICATION).
update_prepare_clocks(Partition, SnapshotVC) ->
    _ = grb_propagation_vnode:merge_remote_stable_vc(Partition, SnapshotVC),
    ok.
-else.
update_prepare_clocks(Partition, SnapshotVC) ->
    _ = grb_propagation_vnode:merge_remote_uniform_vc(Partition, SnapshotVC),
    ok.
-endif.

-spec handle_replicate(partition_id(), replica_id(), term(), #{}, vclock()) -> ok.
handle_replicate(Partition, SourceReplica, TxId, WS, VC) ->
    CommitTime = grb_vclock:get_time(SourceReplica, VC),
    KnownTime = grb_vclock:get_time(SourceReplica, grb_propagation_vnode:known_vc(Partition)),
    case KnownTime < CommitTime of
        false ->
            ok; %% de-dup, we already received this
        true ->
            riak_core_vnode_master:sync_command({Partition, node()},
                                                {handle_remote_tx, SourceReplica, TxId, WS, CommitTime, VC},
                                                ?master,
                                                infinity)
    end.

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
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
    State = #state{partition = Partition,
                   prepared_blue = #{},
                   blue_tick_interval=BlueTickInterval,
                   op_log_size = KeyLogSize,
                   op_log = new_cache(Partition, ?OP_LOG_TABLE)},

    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.op_log]),
    {reply, Ready, State};

handle_command(start_blue_hb_timer, _From, S = #state{blue_tick_interval=Int, blue_tick_timer=undefined}) ->
    TRef = erlang:send_after(Int, self(), ?blue_tick_req),
    {reply, ok, S#state{blue_tick_timer=TRef}};

handle_command(start_blue_hb_timer, _From, S = #state{blue_tick_timer=_Tref}) ->
    {reply, ok, S};

handle_command(start_replicas, _From, S = #state{partition=P,
                                                 replicas_n=N,
                                                 default_bottom_value=Val,
                                                 default_bottom_red=RedTs}) ->

    Result = case grb_partition_replica:replica_ready(P, N) of
        true -> true;
        false ->
            ok = grb_partition_replica:start_replicas(P, N, Val, RedTs),
            grb_partition_replica:replica_ready(P, N)
    end,
    {reply, Result, S};

handle_command(stop_blue_hb_timer, _From, S = #state{blue_tick_timer=undefined}) ->
    {reply, ok, S};

handle_command(stop_blue_hb_timer, _From, S = #state{blue_tick_timer=TRef}) ->
    erlang:cancel_timer(TRef),
    {reply, ok, S#state{blue_tick_timer=undefined}};

handle_command(stop_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    ok = grb_partition_replica:stop_replicas(P, N),
    {reply, ok, S};

handle_command(replicas_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = grb_partition_replica:replica_ready(P, N),
    {reply, Result, S};

%% called from grb:load/1 to verify loading mechanism
handle_command(get_default, _From, S=#state{default_bottom_value=Val, default_bottom_red=RedTs}) ->
    {reply, {Val, RedTs}, S};

handle_command({update_default, DefaultVal, DefaultRed}, _From, S=#state{partition=P, replicas_n=N}) ->
    Result = grb_partition_replica:update_default(P, N, DefaultVal, DefaultRed),
    {reply, Result, S#state{default_bottom_value=DefaultVal, default_bottom_red=DefaultRed}};

handle_command({prepare_blue, TxId, WS, Ts}, _From, S=#state{prepared_blue=PB}) ->
    ?LOG_DEBUG("prepare_blue ~p wtih time ~p", [TxId, Ts]),
    {noreply, S#state{prepared_blue=PB#{TxId => {WS, Ts}}}};

handle_command({decide_blue, ReplicaId, TxId, VC}, _From, State) ->
    NewState = decide_blue_internal(ReplicaId, TxId, VC, State),
    {reply, ok, NewState};

handle_command({handle_remote_tx, SourceReplica, TxId, WS, CommitTime, VC}, _From, State) ->
    ok = handle_remote_tx_internal(SourceReplica, TxId, WS, CommitTime, VC, State),
    {reply, ok, State};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_info(?blue_tick_req, State=#state{partition=P,
                                         blue_tick_timer=Timer,
                                         blue_tick_interval=Interval,
                                         prepared_blue=PreparedBlue}) ->
    erlang:cancel_timer(Timer),
    KnownTime = compute_new_known_time(PreparedBlue),
    ok = grb_propagation_vnode:handle_self_blue_heartbeat_sync(P, KnownTime),
    {ok, State#state{blue_tick_timer=erlang:send_after(Interval, self(), ?blue_tick_req)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("unhandled_info ~p", [Msg]),
    {ok, State}.

-spec handle_remote_tx_internal(replica_id(), term(), #{}, grb_time:ts(), vclock(), state()) -> ok.
-ifdef(BASIC_REPLICATION).

handle_remote_tx_internal(SourceReplica, TxId, WS, CommitTime, VC, #state{partition=Partition,
                                                                          op_log=OperationLog,
                                                                          op_log_size=LogSize}) ->
    ok = update_partition_state(TxId, WS, VC, OperationLog, LogSize),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, CommitTime),
    ok.

-else.

handle_remote_tx_internal(SourceReplica, TxId, WS, CommitTime, VC, #state{partition=Partition,
                                                                          op_log=OperationLog,
                                                                          op_log_size=LogSize}) ->
    ok = update_partition_state(TxId, WS, VC, OperationLog, LogSize),
    ok = grb_propagation_vnode:append_blue_commit(SourceReplica, Partition, CommitTime, TxId, WS, VC),
    ok.

-endif.

-spec decide_blue_internal(replica_id(), term(), vclock(), #state{}) -> #state{}.
decide_blue_internal(ReplicaId, TxId, VC, S=#state{partition=SelfPartition,
                                                   op_log=OpLog,
                                                   op_log_size=LogSize,
                                                   prepared_blue=PreparedBlue}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    {{WS, _}, PreparedBlue1} = maps:take(TxId, PreparedBlue),
    ok = update_partition_state(TxId, WS, VC, OpLog, LogSize),
    KnownTime = compute_new_known_time(PreparedBlue1),
    ok = grb_propagation_vnode:append_blue_commit(ReplicaId, SelfPartition, KnownTime, TxId, WS, VC),
    S#state{prepared_blue=PreparedBlue1}.

-spec update_partition_state(TxId :: term(),
                             WS :: #{},
                             CommitVC :: vclock(),
                             OpLog :: cache(key(), grb_version_log:t()),
                             DefaultSize :: non_neg_integer()) -> ok.

update_partition_state(_TxId, WS, CommitVC, OpLog, DefaultSize) ->
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = case ets:lookup(OpLog, Key) of
            [{Key, PrevLog}] -> PrevLog;
            [] -> grb_version_log:new(DefaultSize)
        end,
        NewLog = grb_version_log:append({blue, Value, CommitVC}, Log),
        [{Key, NewLog} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.

-spec compute_new_known_time(#{any() => {#{}, vclock()}}) -> grb_time:ts().
compute_new_known_time(PreparedBlue) when map_size(PreparedBlue) =:= 0 ->
    grb_time:timestamp();

compute_new_known_time(PreparedBlue) ->
    MinPrep = maps:fold(fun
        (_, {_, Ts}, ignore) -> Ts;
        (_, {_, Ts}, Acc) -> erlang:min(Ts, Acc)
    end, ignore, PreparedBlue),
    ?LOG_DEBUG("knownVC[d] = min_prep (~p - 1)", [MinPrep]),
    MinPrep - 1.

%%%===================================================================
%%% Util Functions
%%%===================================================================

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

-spec new_cache(partition_id(), atom()) -> cache_id().
new_cache(Partition, Name) ->
    new_cache(Partition, Name, [set, protected, named_table, {read_concurrency, true}]).

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
