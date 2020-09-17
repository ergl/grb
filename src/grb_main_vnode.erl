-module(grb_main_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Public API
-export([prepare_blue/4,
         decide_blue/3,
         handle_replicate/5,
         handle_red_transaction/4]).

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

-type op_log() :: cache(key(), grb_version_log:t()).
-type last_red() :: cache(key(), #last_red_record{}).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n = ?READ_CONCURRENCY :: non_neg_integer(),

    prepared_blue :: #{any() => {#{}, vclock()}},

    blue_tick_interval :: non_neg_integer(),
    blue_tick_timer = undefined :: reference() | undefined,

    %% todo(borja, crdt): change type of op_log when adding crdts
    op_log_size :: non_neg_integer(),
    op_log :: op_log(),
    op_last_red :: last_red(),

    %% It doesn't make sense to append it if we're not connected to other clusters
    should_append_commit = true :: boolean(),

    default_bottom_value = <<>> :: term(),
    default_bottom_red = 0 :: grb_time:ts()
}).

-type state() :: #state{}.

-export_type([last_red/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec prepare_blue(partition_id(), term(), #{}, vclock()) -> grb_time:ts().
prepare_blue(Partition, TxId, WriteSet, SnapshotVC) ->
    Ts = grb_time:timestamp(),
    ok = riak_core_vnode_master:command({Partition, node()},
                                        {prepare_blue, TxId, WriteSet, SnapshotVC, Ts},
                                        ?master),
    Ts.

-spec decide_blue(partition_id(), term(), vclock()) -> ok.
decide_blue(Partition, TxId, CommitVC) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {decide_blue, TxId, CommitVC},
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
    KnownTime = grb_propagation_vnode:known_time(Partition, SourceReplica),
    case KnownTime < CommitTime of
        false ->
            ok; %% de-dup, we already received this
        true ->
            riak_core_vnode_master:command({Partition, node()},
                                           {handle_remote_tx, SourceReplica, TxId, WS, CommitTime, VC},
                                           ?master)
    end.

-spec handle_red_transaction(partition_id(), {}, grb_time:ts(), vclock()) -> ok.
handle_red_transaction(Partition, WS, RedTime, VC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {handle_red_tx, WS, RedTime, VC},
                                   ?master).

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
    LastRed = grb_dc_utils:new_cache(Partition,
                                     ?OP_LOG_LAST_RED,
                                     [set, protected, named_table,
                                      {read_concurrency, true}, {keypos, #last_red_record.key}]),

    State = #state{partition = Partition,
                   prepared_blue = #{},
                   blue_tick_interval=BlueTickInterval,
                   op_log_size = KeyLogSize,
                   op_log = grb_dc_utils:new_cache(Partition, ?OP_LOG_TABLE),
                   op_last_red = LastRed},

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

handle_command({prepare_blue, TxId, WS, SnapshotVC, Ts}, _From, S=#state{partition=Partition, prepared_blue=PB}) ->
    ?LOG_DEBUG("prepare_blue ~p wtih time ~p", [TxId, Ts]),
    ok = update_prepare_clocks(Partition, SnapshotVC),
    {noreply, S#state{prepared_blue=PB#{TxId => {WS, Ts}}}};

handle_command({decide_blue, TxId, VC}, _From, State) ->
    NewState = decide_blue_internal(TxId, VC, State),
    {reply, ok, NewState};

handle_command({handle_remote_tx, SourceReplica, TxId, WS, CommitTime, VC}, _From, State) ->
    ok = handle_remote_tx_internal(SourceReplica, TxId, WS, CommitTime, VC, State),
    {noreply, State};

handle_command({handle_red_tx, WS, RedTime, VC}, _From, S=#state{partition=Partition,
                                                                 op_log=OperationLog,
                                                                 op_log_size=LogSize,
                                                                 op_last_red=LastRed}) ->

    ok = update_partition_state(?RED_REPLICA, WS, VC, OperationLog, LogSize, LastRed),
    ok = grb_propagation_vnode:handle_red_heartbeat(Partition, RedTime),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

handle_info(?blue_tick_req, State=#state{partition=P,
                                         blue_tick_timer=Timer,
                                         blue_tick_interval=Interval,
                                         prepared_blue=PreparedBlue}) ->
    erlang:cancel_timer(Timer),
    KnownTime = compute_new_known_time(PreparedBlue),
    ok = grb_propagation_vnode:handle_self_blue_heartbeat(P, KnownTime),
    {ok, State#state{blue_tick_timer=erlang:send_after(Interval, self(), ?blue_tick_req)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

-spec handle_remote_tx_internal(replica_id(), term(), #{}, grb_time:ts(), vclock(), state()) -> ok.
-ifdef(NO_REMOTE_APPEND).
handle_remote_tx_internal(SourceReplica, _, WS, CommitTime, VC, #state{partition=Partition,
                                                                       op_log=OperationLog,
                                                                       op_log_size=LogSize,
                                                                       op_last_red=LastRed}) ->
    ok = update_partition_state(WS, VC, OperationLog, LogSize, LastRed),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, CommitTime),
    ok.

-else.

handle_remote_tx_internal(SourceReplica, TxId, WS, CommitTime, VC, #state{partition=Partition,
                                                                          op_log=OperationLog,
                                                                          op_log_size=LogSize,
                                                                          op_last_red=LastRed}) ->
    ok = update_partition_state(WS, VC, OperationLog, LogSize, LastRed),
    ok = grb_propagation_vnode:append_remote_blue_commit(SourceReplica, Partition, CommitTime, TxId, WS, VC),
    ok.

-endif.

-spec decide_blue_internal(term(), vclock(), state()) -> state().
decide_blue_internal(TxId, VC, S=#state{partition=SelfPartition,
                                        op_log=OpLog,
                                        op_log_size=LogSize,
                                        op_last_red=LastRed,
                                        prepared_blue=PreparedBlue,
                                        should_append_commit=ShouldAppend}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    {{WS, _}, PreparedBlue1} = maps:take(TxId, PreparedBlue),
    ok = update_partition_state(WS, VC, OpLog, LogSize, LastRed),
    KnownTime = compute_new_known_time(PreparedBlue1),
    case ShouldAppend of
        true ->
            grb_propagation_vnode:append_blue_commit(SelfPartition, KnownTime, TxId, WS, VC);
        false ->
            grb_propagation_vnode:handle_self_blue_heartbeat(SelfPartition, KnownTime)
    end,
    S#state{prepared_blue=PreparedBlue1}.

-spec update_partition_state(WS :: #{},
                             CommitVC :: vclock(),
                             OpLog :: op_log(),
                             DefaultSize :: non_neg_integer(),
                             LastRed :: last_red()) -> ok.

update_partition_state(WS, CommitVC, OpLog, DefaultSize, LastRed) ->
    update_partition_state(blue, WS, CommitVC, OpLog, DefaultSize, LastRed).

-spec update_partition_state(TxType :: transaction_type(),
                             WS :: #{},
                             CommitVC :: vclock(),
                             OpLog :: op_log(),
                             DefaultSize :: non_neg_integer(),
                             LastRed :: last_red()) -> ok.

-ifdef(BLUE_KNOWN_VC).
update_partition_state(TxType, WS, CommitVC, OpLog, DefaultSize, _LastRed) ->
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = append_to_log(TxType, Key, Value, CommitVC, OpLog, DefaultSize),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.
-else.
%% Dialyzer really doesn't like what's happening inside update_last_red/6, so it bottles upwards to this
%% function.
-dialyzer({no_return, update_partition_state/6}).
update_partition_state(TxType, WS, CommitVC, OpLog, DefaultSize, LastRed) ->
    Vsn = grb_vclock:get_time(?RED_REPLICA, CommitVC),
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = append_to_log(TxType, Key, Value, CommitVC, OpLog, DefaultSize),
        ok = update_last_red(Key, Vsn, AllReplicas, CommitVC, LastRed, DefaultSize),
        [{Key, Log} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.

%% LastRed contains, for each key, its max red version and (small) list of commit vectors
%% We could have only one clock, and max it with the one we have on each transaction, but this
%% would slow the commit of each transaction. Instead, normally a transaction will either insert
%% a new entry (if there was no info about it before), or append a new commit vector to the list.
%%
%% Once the list hits a certain size, we can fetch it, prune it, and add it again. This should
%% only happen from time to time, if we tune the size correctly (25 clocks by default).
-spec update_last_red(Key :: key(),
                      Vsn :: grb_time:ts(),
                      AtReplicas :: [replica_id()],
                      CommitVC :: vclock(),
                      LastRed :: last_red(),
                      LogSize :: non_neg_integer()) -> ok.

%% dialyzer doesn't like the calls to red_clocks_match/1 / append_clocks_match/4
-dialyzer({nowarn_function, update_last_red/6}).
update_last_red(Key, Vsn, AtReplicas, CommitVC, LastRed, LogSize) ->
    case ets:select(LastRed, red_clocks_match(Key)) of
        [{OldRed, ListSize}] when ListSize > LogSize ->
            %% Compact the list of vector clocks when we reach the limit
            Reduced = compact_clocks(AtReplicas,
                                     CommitVC,
                                     ets:lookup_element(LastRed, Key, #last_red_record.clocks)),

            Record = #last_red_record{key=Key, red=max(Vsn, OldRed), length=1, clocks=[Reduced]},
            true = ets:insert(LastRed, Record);

        [{OldRed, ListSize}] ->
            %% if we're under the limit, simply append the clock
            1 = ets:select_replace(LastRed,
                                   append_clocks_match(Key, max(OldRed, Vsn), ListSize + 1, CommitVC));

        [] ->
            %% otherwise, simply insert a new recor
            true = ets:insert(LastRed, #last_red_record{key=Key, red=Vsn, length=1, clocks=[CommitVC]})
    end,
    ok.

%% dialyzer doesn't like constructing a record like this
-dialyzer({nowarn_function, red_clocks_match/1}).
-spec red_clocks_match(key()) -> ets:match_spec().
red_clocks_match(Key) ->
    [{ #last_red_record{key=Key, red='$1', length='$2', _='_'}, [], [{{'$1', '$2'}}] }].

%% dialyzer thinks this will never be called from update_last_red, since it thinks the match_specs are
%% wrong
-dialyzer({no_unused, compact_clocks/3}).
-spec compact_clocks([replica_id()], vclock(), [vclock()]) -> vclock().
compact_clocks(AtReplicas, Init, Clocks) ->
    lists:foldl(fun(VC, Acc) ->
        grb_vclock:max_at_keys(AtReplicas, VC, Acc)
    end, Init, Clocks).

%% dialyzer doesn't like appending to '$1' even if it's correct
-dialyzer({nowarn_function, append_clocks_match/4}).
-spec append_clocks_match(key(), grb_time:ts(), non_neg_integer(), vclock()) -> ets:match_spec().
append_clocks_match(Key, Time, Length, VC) ->
    [{
        #last_red_record{key=Key, clocks='$1', _='_'},
        [],
        [{ #last_red_record{key=Key, red={const, Time}, length={const, Length}, clocks=[ VC | '$1' ]} }]
    }].

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
