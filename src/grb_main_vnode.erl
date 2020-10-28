-module(grb_main_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ETS table API
-export([op_log_table/1,
         last_red_table/1]).

%% Public API
-export([get_key_version/3,
         get_key_version_with_table/3,
         prepare_blue/4,
         decide_blue_ready/1,
         decide_blue_ready/2,
         decide_blue/3,
         handle_replicate/4,
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

-define(master, grb_main_vnode_master).
-define(blue_tick_req, blue_tick_event).

-define(OP_LOG_TABLE, op_log_table).
-define(OP_LOG_LAST_RED, op_log_last_red_table).
-define(PREPARED_TABLE, prepared_blue_table).

-type op_log() :: cache(key(), grb_version_log:t()).
-type last_red() :: cache(key(), #last_red_record{}).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n :: non_neg_integer(),

    prepared_blue :: cache_id(),

    blue_tick_interval :: non_neg_integer(),
    blue_tick_timer = undefined :: reference() | undefined,

    %% todo(borja, crdt): change type of op_log when adding crdts
    op_log_size :: non_neg_integer(),
    op_log :: op_log(),
    op_last_red :: last_red(),

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

-export_type([last_red/0]).

%%%===================================================================
%%% ETS API
%%%===================================================================

-spec op_log_table(partition_id()) -> cache_id().
op_log_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?OP_LOG_TABLE}).

-spec last_red_table(partition_id()) -> cache_id().
last_red_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?OP_LOG_LAST_RED}).

-spec prepared_blue_table(partition_id()) -> cache_id().
prepared_blue_table(Partition) ->
    persistent_term:get({?MODULE, Partition, ?PREPARED_TABLE}).

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

-spec decide_blue_ready(vclock()) -> ready | not_ready.
decide_blue_ready(CommitVC) ->
    decide_blue_ready(grb_dc_manager:replica_id(), CommitVC).

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
    NumReplicas = application:get_env(grb, op_log_replicas, ?READ_CONCURRENCY),

    OpLogTable = ets:new(?OP_LOG_TABLE, [set, protected, {read_concurrency, true}]),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_TABLE}, OpLogTable),

    LastRedTable = ets:new(?OP_LOG_LAST_RED, [set, protected, {read_concurrency, true}, {keypos, #last_red_record.key}]),
    ok = persistent_term:put({?MODULE, Partition, ?OP_LOG_LAST_RED}, LastRedTable),

    PreparedBlue = ets:new(?PREPARED_TABLE, [ordered_set, public, {write_concurrency, true}, {keypos, #prepared_record.key}]),
    ok = persistent_term:put({?MODULE, Partition, ?PREPARED_TABLE}, PreparedBlue),

    State = #state{partition = Partition,
                   replicas_n=NumReplicas,
                   prepared_blue=PreparedBlue,
                   blue_tick_interval=BlueTickInterval,
                   op_log_size = KeyLogSize,
                   op_log = OpLogTable,
                   op_last_red = LastRedTable},

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
                                                 replicas_n=N}) ->

    Result = case grb_partition_replica:replica_ready(P, N) of
        true -> true;
        false ->
            ok = grb_partition_replica:start_replicas(P, N),
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

handle_command({decide_blue, TxId, VC}, _From, State) ->
    ok = decide_blue_internal(TxId, VC, State),
    {reply, ok, State};

handle_command({handle_remote_tx, SourceReplica, WS, CommitTime, VC}, _From, State) ->
    ok = handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, State),
    {noreply, State};

handle_command({handle_red_tx, WS, VC}, _From, S=#state{op_log=OperationLog,
                                                        op_log_size=LogSize,
                                                        op_last_red=LastRed}) ->
    ok = update_partition_state(?RED_REPLICA, WS, VC, OperationLog, LogSize, LastRed),
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
                                                                       op_last_red=LastRed}) ->
    ok = update_partition_state(WS, VC, OperationLog, LogSize, LastRed),
    ok = grb_propagation_vnode:handle_blue_heartbeat(Partition, SourceReplica, CommitTime),
    ok.

-else.

handle_remote_tx_internal(SourceReplica, WS, CommitTime, VC, #state{partition=Partition,
                                                                          op_log=OperationLog,
                                                                          op_log_size=LogSize,
                                                                          op_last_red=LastRed}) ->
    ok = update_partition_state(WS, VC, OperationLog, LogSize, LastRed),
    ok = grb_propagation_vnode:append_remote_blue_commit(SourceReplica, Partition, CommitTime, WS, VC),
    ok.

-endif.

-spec decide_blue_internal(term(), vclock(), state()) -> ok.
decide_blue_internal(TxId, VC, #state{partition=SelfPartition,
                                      op_log=OpLog,
                                      op_log_size=LogSize,
                                      op_last_red=LastRed,
                                      prepared_blue=PreparedBlue,
                                      should_append_commit=ShouldAppend}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    {ok, WS} = get_prepared_writeset(PreparedBlue, TxId),
    ok = update_partition_state(WS, VC, OpLog, LogSize, LastRed),
    KnownTime = compute_new_known_time(PreparedBlue),
    case ShouldAppend of
        true ->
            grb_propagation_vnode:append_blue_commit(SelfPartition, KnownTime, WS, VC);
        false ->
            grb_propagation_vnode:handle_self_blue_heartbeat(SelfPartition, KnownTime)
    end,
    ok.

-spec get_prepared_writeset(cache_id(), term()) -> {ok, writeset()} | false.
get_prepared_writeset(PreparedBlue, TxId) ->
    case ets:select(PreparedBlue, [{ #prepared_record{key={'$1', TxId}, writeset='$2'}, [], [{{'$1', '$2'}}] }]) of
        [{PrepareTs, WS}] ->
            true = ets:delete(PreparedBlue, {PrepareTs, TxId}),
            {ok, WS};
        _ ->
            %% todo(borja): Warn if more than one result?
            false
    end.

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
%% Dialyzer really doesn't like what's happening inside update_last_red/7, so it bottles upwards to this
%% function.
-dialyzer({no_return, update_partition_state/6}).
update_partition_state(TxType, WS, CommitVC, OpLog, DefaultSize, LastRed) ->
    Vsn = grb_vclock:get_time(?RED_REPLICA, CommitVC),
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = append_to_log(TxType, Key, Value, CommitVC, OpLog, DefaultSize),
        ok = update_last_red(TxType, Key, Vsn, AllReplicas, CommitVC, LastRed, DefaultSize),
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
-spec update_last_red(TxType :: transaction_type(),
                      Key :: key(),
                      Vsn :: grb_time:ts(),
                      AtReplicas :: [replica_id()],
                      CommitVC :: vclock(),
                      LastRed :: last_red(),
                      LogSize :: non_neg_integer()) -> ok.

%% dialyzer doesn't like the calls to red_clocks_match/1 / append_clocks_match/4
-dialyzer({nowarn_function, update_last_red/7}).
-ifndef('RED_BLUE_CONFLICT').
update_last_red(blue, _, _, _, _, _, _) ->
    ok;
update_last_red(red, Key, Vsn, AtReplicas, CommitVC, LastRed, LogSize) ->
    update_last_red_log(Key, Vsn, AtReplicas, CommitVC, LastRed, LogSize).
-else.
update_last_red(_TxType, Key, Vsn, AtReplicas, CommitVC, LastRed, LogSize) ->
    update_last_red_log(Key, Vsn, AtReplicas, CommitVC, LastRed, LogSize).
-endif.

-spec update_last_red_log(Key :: key(),
                          Vsn :: grb_time:ts(),
                          AtReplicas :: [replica_id()],
                          CommitVC :: vclock(),
                          LastRed :: last_red(),
                          LogSize :: non_neg_integer()) -> ok.
update_last_red_log(Key, Vsn, AtReplicas, CommitVC, LastRed, LogSize) ->
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

grb_main_vnode_compute_new_known_time_test() ->
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
    ?assertEqual({ok, #{}}, get_prepared_writeset(?PREPARED_TABLE, tx_1)),
    ?assertEqual(2, compute_new_known_time(?PREPARED_TABLE)),

    %% tx_3 was removed earlier, but it has a higher ts than tx_2, so tx_2 is still the lowest
    ?assertEqual({ok, #{}}, get_prepared_writeset(?PREPARED_TABLE, tx_3)),
    ?assertEqual(2, compute_new_known_time(?PREPARED_TABLE)),

    %% now, tx_4 is the next in the queue, at ts 10-1
    ?assertEqual({ok, #{}}, get_prepared_writeset(?PREPARED_TABLE, tx_2)),
    ?assertEqual(9, compute_new_known_time(?PREPARED_TABLE)),

    %% same with tx_5
    ?assertEqual({ok, #{}}, get_prepared_writeset(?PREPARED_TABLE, tx_4)),
    ?assertEqual(49, compute_new_known_time(?PREPARED_TABLE)),

    ?assertEqual({ok, #{}}, get_prepared_writeset(?PREPARED_TABLE, tx_5)),
    %% Now that the queue is empty, the time is the current clock
    Ts = grb_time:timestamp(),
    Lowest = compute_new_known_time(?PREPARED_TABLE),
    ?assert(Ts < Lowest),

    ets:delete(?PREPARED_TABLE),
    ok.

-endif.
