-module(grb_paxos_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% init api
-export([all_fetch_lastvc_table/0,
         init_leader_state/0,
         init_follower_state/0,
         put_conflicts_all/1]).

%% heartbeat api
-export([prepare_heartbeat/2,
         accept_heartbeat/6,
         decide_heartbeat/4]).

%% tx API
-export([prepare/7,
         prepare_local/7,
         accept/10,
         decide/5,
         decide_local/5,
         learn_abort/5,
         deliver/5]).

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

-define(master, grb_paxos_vnode_master).
-define(deliver_event, deliver_event).
-define(prune_event, prune_event).
-define(send_aborts_event, send_aborts_event).

-define(QUEUE_KEY(__P), {?MODULE, __P, message_queue_len}).
-define(LOG_QUEUE_LEN(__P), grb_measurements:log_queue_length(?QUEUE_KEY(__P))).

-define(TO_COMMIT_TS_KEY(__P), {?MODULE, __P, seen_to_commit_time}).
-define(TO_DELIVERY_TS_KEY(__P), {?MODULE, __P, seen_to_delivery_time}).
-define(TO_ABORT_TS_KEY(__P), {?MODULE, __P, seen_to_abort_time}).

-define(NEXT_READY_DURATION(__P), {?MODULE, __P, get_next_ready_ts}).
-define(DELIVER_DURATION(__P), {?MODULE, __P, deliver_updates_ts}).

-define(ACCEPT_FLIGHT_TS(__P), {?MODULE, __P, accept_in_flight}).

-define(DELIVER_CALLED_N(__P), {?MODULE, __P, deliver_updates_called}).

-define(leader, leader).
-define(follower, follower).
-type role() :: ?leader | ?follower.

-ifdef(ENABLE_METRICS).
-define(INIT_TIMING_TABLE(__S),
    begin
        __Tref = ets:new(timing_data, [ordered_set]),
        ok = persistent_term:put({?MODULE, __S#state.partition, timing_data}, __Tref),
        __S#state{timing_table=__Tref}
    end).

-define(MARK_SEEN_TX_TS(__Id, __Now, __S),
    begin
        ets:insert(__S#state.timing_table, {{__Id, first_seen}, __Now}),
        ok
    end).

-define(ADD_COMMIT_TS(__Id, __S),
    begin true = ets:insert(__S#state.timing_table, {{__Id, commit}, grb_time:timestamp()}), __S end).

-define(REPORT_ABORT_TS(__Id, __S),
    try
        __Now = grb_time:timestamp(),
        __PrepTime = ets:lookup_element(__S#state.timing_table, {__Id, first_seen}, 2),

        ok = grb_measurements:log_stat(?TO_ABORT_TS_KEY(__S#state.partition),
                                       grb_time:diff_native(__Now, __PrepTime)),

        _ = ets:select_delete(__S#state.timing_table,
                              [{ {{__Id, '_'}, '_'}, [], [true]} ]),

        __S
    catch _:_ ->
        __S
    end).

-define(REPORT_LEADER_TS(__Id, __Now, __Partition),
    try
        __Table = persistent_term:get({?MODULE, __Partition, timing_data}),
        __PrepTime = ets:lookup_element(__Table, {__Id, first_seen}, 2),
        __DecTime = ets:lookup_element(__Table, {__Id, commit}, 2),

        ok = grb_measurements:log_stat(?TO_COMMIT_TS_KEY(__Partition),
                                        grb_time:diff_native(__DecTime, __PrepTime)),

        ok = grb_measurements:log_stat(?TO_DELIVERY_TS_KEY(__Partition),
                                        grb_time:diff_native(__Now, __PrepTime)),

        _ = ets:select_delete(persistent_term:get({?MODULE, __Partition, timing_data}),
                              [{ {{__Id, '_'}, '_'}, [], [true]} ]),

        ok
    catch _:_ ->
        ok
    end).

-define(REPORT_FOLLOWER_TS(__Id, __Now, __Partition),
    try
        __Table = persistent_term:get({?MODULE, __Partition, timing_data}),
        __PrepTime = ets:lookup_element(__Table, {__Id, first_seen}, 2),

        ok = grb_measurements:log_stat(?TO_DELIVERY_TS_KEY(__Partition),
                                        grb_time:diff_native(__Now, __PrepTime)),

        _ = ets:select_delete(persistent_term:get({?MODULE, __Partition, timing_data}),
                              [{ {{__Id, '_'}, '_'}, [], [true]} ]),

        ok
    catch _:_ ->
        ok
    end).

-export([deliver_updates/5]).

-define(DELIVER_UPDATES(__P, __N, __B, __F, __S),
    begin
        {__Took, __Res} = timer:tc(?MODULE, deliver_updates, [__P, __N, __B, __F, __S]),
        grb_measurements:log_stat(?DELIVER_DURATION(__P), __Took),
        __Res
    end).
-else.

-define(INIT_TIMING_TABLE(__S), __S).
-define(MARK_SEEN_TX_TS(__Id, __Now, __S), begin _ = __Id, _ = __Now, __S end).

-define(ADD_COMMIT_TS(__Id, __S), begin _ = __Id, __S end).
-define(REPORT_ABORT_TS(__Id, __S), begin _ = __Id, __S end).

-define(REPORT_LEADER_TS(__Id, __Now, __S), begin _ = __Id, _ = __Now, _= __S, ok end).
-define(REPORT_FOLLOWER_TS(__Id, __Now, __S), begin _ = __Id, _ = __Now, _ = __S, ok end).

-define(DELIVER_UPDATES(__P, __N, __B, __F, __S), deliver_updates(__P, __N, __B, __F, __S)).
-endif.

-type pending_message() :: {accept, red_coord_location(), ballot(), term(), tx_label(), readset(), writeset(), vclock()}
                         | {accept_hb, replica_id(), ballot(), red_heartbeat_id(), grb_time:ts()}
                         | {deliver_transactions, ballot(), grb_time:ts(), [ red_heartbeat_id() | {term(), tx_label()} ]}.

-ifdef(ENABLE_METRICS).
-record(state, {
    partition :: partition_id(),
    replica_id = undefined :: replica_id() | undefined,
    heartbeat_process = undefined :: pid() | undefined,
    heartbeat_schedule_ms :: non_neg_integer(),
    heartbeat_schedule_timer = undefined :: reference() | undefined,
    last_delivered = 0 :: grb_time:ts(),
    deliver_timer = undefined :: reference() | undefined,
    deliver_interval :: non_neg_integer(),
    prune_timer = undefined :: reference() | undefined,
    prune_interval :: non_neg_integer(),
    send_aborts_timer = undefined :: reference() | undefined,
    send_aborts_interval_ms :: non_neg_integer(),
    op_log_last_vc_replica :: grb_oplog_vnode:last_vc() | undefined,
    synod_role = undefined :: role() | undefined,
    synod_state = undefined :: grb_paxos_state:t() | undefined,
    conflict_relations :: conflict_relations(),
    abort_buffer_io = [] :: iodata(),
    fifo_sequence_number = 0 :: non_neg_integer(),
    pending_fifo_messages = #{} :: #{ non_neg_integer() => pending_message() },
    pending_abort_messages = #{} :: #{ {term(), ballot()} => {atom(), grb_time:ts()} },
    pending_prepares = #{} :: #{ term() => {tx_label(), readset(), writeset(), vclock(), red_coord_location()} },
    timing_table :: cache_id()
}).
-else.
-record(state, {
    partition :: partition_id(),
    replica_id = undefined :: replica_id() | undefined,

    %% only at leader
    heartbeat_process = undefined :: pid() | undefined,
    heartbeat_schedule_ms :: non_neg_integer(),
    heartbeat_schedule_timer = undefined :: reference() | undefined,

    last_delivered = 0 :: grb_time:ts(),

    %% How often to check for ready transactions.
    %% Only happens at the leader.
    deliver_timer = undefined :: reference() | undefined,
    deliver_interval :: non_neg_integer(),

    %% How often to prune already-delivered transactions.
    prune_timer = undefined :: reference() | undefined,
    prune_interval :: non_neg_integer(),

    %% How often does the leader send aborts?
    send_aborts_timer = undefined :: reference() | undefined,
    send_aborts_interval_ms :: non_neg_integer(),

    %% read replica of the last commit vc cache by grb_oplog_vnode
    op_log_last_vc_replica :: grb_oplog_vnode:last_vc() | undefined,

    %% paxos state and role
    synod_role = undefined :: role() | undefined,
    synod_state = undefined :: grb_paxos_state:t() | undefined,

    %% conflict information, who conflicts with whom
    conflict_relations :: conflict_relations(),

    %% A buffer of delayed abort messages, already encoded for sending.
    %%
    %% The leader can wait for a while before sending abort messages to followers.
    %% In the normal case, followers don't need to learn about aborted transactions,
    %% since they don't execute any delivery preconditions. This allows us to save
    %% an exchaned message during commit if we know the transactions is aborted.
    %%
    %% During recovery, it's important that the new leader knows about aborted
    %% transactions, otherwise it won't be able to deliver new transactions.
    %%
    %% A solution to this problem is to retry transactions that have been sitting
    %% in prepared for too long.
    %% fixme(borja): this requires that we have the entire writeset, otherwise we won't be able to retry
    abort_buffer_io = [] :: iodata(),

    %% The next sequence number to use when sending / receiving ordered messages.
    fifo_sequence_number = 0 :: non_neg_integer(),

    %% Store any pending messages from the leader. We must process certain messages
    %% in order, so if the incoming sequence number doesn't match the expected one,
    %% we will buffer it here until all the dependencies are satsified.
    pending_fifo_messages = #{} :: #{ non_neg_integer() => pending_message() },

    %% Store any pending abort messages here. Since these messages are batched, it doesn't
    %% make sense to assign them sequence numbers, as they are delivered later. If we receive
    %% an abort for a transaction before we receive an accept, buffer it here.
    pending_abort_messages = #{} :: #{ {term(), ballot()} => { {abort, atom()}, grb_time:ts()} },

    %% Store any pending prepares here. This is only used at the leader if the incoming prepare
    %% timestamp is higher than the current time. This usually shouldn't happen unless clock skew
    %% is too noticeable.
    pending_prepares = #{} :: #{ term() => {tx_label(), readset(), writeset(), vclock(), red_coord_location()} }
}).
-endif.

-spec all_fetch_lastvc_table() -> ok.
-ifdef(BLUE_KNOWN_VC).
all_fetch_lastvc_table() ->
    ok.
-else.
all_fetch_lastvc_table() ->
    Res = grb_dc_utils:bcast_vnode_sync(?master, fetch_lastvc_table, 1000),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res).
-endif.

-spec put_conflicts_all(conflict_relations()) -> ok | error.
put_conflicts_all(Conflicts) ->
    Results = [try
        riak_core_vnode_master:sync_command(N, {learn_conflicts, Conflicts}, ?master, 1000)
    catch
        _:_ -> false
    end || N <- grb_dc_utils:get_index_nodes() ],
    case lists:all(fun(Result) -> Result =:= ok end, Results) of
        true ->
            ok;
        false ->
            error
    end.

-spec init_leader_state() -> ok.
init_leader_state() ->
    Res = grb_dc_utils:bcast_vnode_sync(?master, init_leader),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res).

-spec init_follower_state() -> ok.
init_follower_state() ->
    Res = grb_dc_utils:bcast_vnode_sync(?master, init_follower),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res).

-spec prepare_heartbeat(partition_id(), term()) -> ok.
prepare_heartbeat(Partition, Id) ->
    grb_dc_utils:vnode_command(Partition, {prepare_hb, Id}, ?master).

-spec accept_heartbeat(partition_id(), replica_id(), non_neg_integer(), ballot(), red_heartbeat_id(), grb_time:ts()) -> ok.
accept_heartbeat(Partition, SourceReplica, Sequence, Ballot, Id, Ts) ->
    grb_dc_utils:vnode_command(Partition,
                               {sequence_msg, Sequence, {accept_hb, SourceReplica, Ballot, Id, Ts}},
                               ?master).

-spec decide_heartbeat(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
decide_heartbeat(Partition, Ballot, Id, Ts) ->
    grb_dc_utils:vnode_command(Partition,
                               {decide_hb, Ballot, Id, Ts},
                               ?master).

-spec prepare(IndexNode :: index_node(),
              TxId :: term(),
              Label :: tx_label(),
              Readset :: readset(),
              WriteSet :: writeset(),
              SnapshotVC :: vclock(),
              Coord :: red_coord_location()) -> ok.

prepare(Idx={Partition, Node}, TxId, Label, ReadSet, Writeset, SnapshotVC, Coord) ->
    if
        Node =:= node() ->
            prepare_local(Partition, TxId, Label, ReadSet, Writeset, SnapshotVC, Coord);
        true ->
            riak_core_vnode_master:command(Idx,
                                           {prepare, TxId, Label, ReadSet, Writeset, SnapshotVC},
                                           Coord,
                                           ?master)
    end.

%% Same as above, for cases where we're sure we're in the same physical machine.
prepare_local(Partition, TxId, Label, RS, WS, VC, Coord) ->
    grb_dc_utils:vnode_command(Partition, {prepare, TxId, Label, RS, WS, VC},
                               Coord, ?master).

-ifndef(ENABLE_METRICS).
-spec accept(Partition :: partition_id(),
             Sequence :: non_neg_integer(),
             Ballot :: ballot(),
             TxId :: term(),
             Label :: tx_label(),
             RS :: readset(),
             WS :: writeset(),
             Vote :: red_vote(),
             PrepareVC :: vclock(),
             Coord :: red_coord_location()) -> ok.

accept(Partition, Sequence, Ballot, TxId, Label, RS, WS, Vote, PrepareVC, Coord) ->
    grb_dc_utils:vnode_command(Partition,
                               {sequence_msg, Sequence,
                                   {accept, Coord, Ballot, TxId, Label, RS, WS, Vote, PrepareVC}},
                               ?master).
-else.
-spec accept(Partition :: partition_id(),
             Sequence :: non_neg_integer(),
             Ballot :: ballot(),
             TxId :: term(),
             Label :: tx_label(),
             RS :: readset(),
             WS :: writeset(),
             Vote :: red_vote(),
             PrepareVC :: vclock(),
             Coord :: {grb_time:ts(), red_coord_location()}) -> ok.

accept(Partition, Sequence, Ballot, TxId, Label, RS, WS, Vote, PrepareVC, {SentTs, Coord}) ->
    Elapsed = grb_time:diff_native(grb_time:timestamp(), SentTs),
    grb_measurements:log_stat(?ACCEPT_FLIGHT_TS(Partition), Elapsed),
    grb_dc_utils:vnode_command(Partition,
                               {sequence_msg, Sequence,
                                   {accept, Coord, Ballot, TxId, Label, RS, WS, Vote, PrepareVC}},
                               ?master).
-endif.

-spec decide(index_node(), ballot(), term(), red_vote(), grb_vclock:ts()) -> ok.
decide(Idx={Partition, Node}, Ballot, TxId, Decision, CommitTs) ->
    if
        Node =:= node() ->
            decide_local(Partition, Ballot, TxId, Decision, CommitTs);
        true ->
            riak_core_vnode_master:command(Idx,
                                           {decision, Ballot, TxId, Decision, CommitTs},
                                           ?master)
    end.

decide_local(Partition, Ballot, TxId, Decision, CommitTs) ->
    grb_dc_utils:vnode_command(Partition,
                               {decision, Ballot, TxId, Decision, CommitTs},
                               ?master).

-spec learn_abort(partition_id(), ballot(), term(), term(), grb_vclock:ts()) -> ok.
learn_abort(Partition, Ballot, TxId, Reason, CommitTs) ->
    grb_dc_utils:vnode_command(Partition,
                               {learn_abort, Ballot, TxId, Reason, CommitTs},
                               ?master).

-spec deliver(partition_id(), non_neg_integer(), ballot(), grb_time:ts(), [ { term(), tx_label() } | red_heartbeat_id() ]) -> ok.
deliver(Partition, Sequence, Ballot, Timestamp, TransactionIds) ->
    grb_dc_utils:vnode_command(Partition,
                               {sequence_msg, Sequence,
                                   {deliver_transactions, Ballot, Timestamp, TransactionIds}},
                               ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    ok = grb_dc_utils:register_vnode_pid(?master, Partition, self()),

    {ok, DeliverInterval} = application:get_env(grb, red_delivery_interval),
    PruningInterval = application:get_env(grb, red_prune_interval, 0),

    %% conflict information can be overwritten by calling grb:put_conflicts/1
    Conflicts = application:get_env(grb, red_conflicts_config, #{}),

    %% only at the leader, but we don't care.
    %% Peg heartbeat schedule to 1ms, we don't want the user to be able to set something smaller.
    HeartbeatScheduleMs = max(application:get_env(grb, red_heartbeat_schedule_ms, 1), 1),
    {ok, SendAbortIntervalMs} = application:get_env(grb, red_abort_interval_ms),

    %% don't care about setting bad values, we will overwrite it
    State = #state{partition=Partition,
                   heartbeat_schedule_ms=HeartbeatScheduleMs,
                   deliver_interval=DeliverInterval,
                   prune_interval=PruningInterval,
                   send_aborts_interval_ms=SendAbortIntervalMs,
                   op_log_last_vc_replica=undefined,
                   synod_state=undefined,
                   conflict_relations=Conflicts},

    {ok, ?INIT_TIMING_TABLE(State)}.

terminate(_Reason, #state{synod_state=undefined}) ->
    ok;

terminate(_Reason, #state{synod_state=SynodState}) ->
    ok = grb_paxos_state:delete(SynodState),
    ok.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    {reply, true, State};

handle_command({learn_conflicts, Conflicts}, _Sender, State) ->
    {reply, ok, State#state{conflict_relations=Conflicts}};

handle_command(fetch_lastvc_table, _Sender, S0=#state{partition=Partition}) ->
    {Result, S} = try
        Table = grb_oplog_vnode:last_vc_table(Partition),
        {ok, S0#state{op_log_last_vc_replica=Table}}
    catch _:_  ->
        {error, S0}
    end,
    {reply, Result, S};

handle_command(init_leader, _Sender, S=#state{partition=Partition, synod_role=undefined, synod_state=undefined}) ->

    ok = grb_measurements:create_stat(?QUEUE_KEY(Partition)),
    ok = grb_measurements:create_stat(?TO_COMMIT_TS_KEY(Partition)),
    ok = grb_measurements:create_stat(?TO_ABORT_TS_KEY(Partition)),
    ok = grb_measurements:create_stat(?TO_DELIVERY_TS_KEY(Partition)),

    ok = grb_measurements:create_stat(?NEXT_READY_DURATION(Partition)),
    ok = grb_measurements:create_stat(?DELIVER_DURATION(Partition)),

    ok = grb_measurements:create_stat(?DELIVER_CALLED_N(Partition)),

    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Pid} = grb_red_heartbeat:new(ReplicaId, Partition),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     heartbeat_process=Pid,
                                     synod_role=?leader,
                                     synod_state=grb_paxos_state:new()})};

handle_command(init_follower, _Sender, S=#state{synod_role=undefined, synod_state=undefined}) ->

    ok = grb_measurements:create_stat(?QUEUE_KEY(S#state.partition)),
    ok = grb_measurements:create_stat(?TO_ABORT_TS_KEY(S#state.partition)),
    ok = grb_measurements:create_stat(?TO_DELIVERY_TS_KEY(S#state.partition)),
    ok = grb_measurements:create_stat(?ACCEPT_FLIGHT_TS(S#state.partition)),

    ReplicaId = grb_dc_manager:replica_id(),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     synod_role=?follower,
                                     synod_state=grb_paxos_state:new()})};

%%%===================================================================
%%% leader protocol messages
%%%===================================================================

handle_command({prepare_hb, Id}, _Sender, S0=#state{synod_role=?leader,
                                                    partition=Partition,
                                                    synod_state=LeaderState0,
                                                    fifo_sequence_number=SequenceNumber}) ->

    %% Cancel and resubmit any pending heartbeats for this partition.
    S = reschedule_heartbeat(S0),

    {Result, LeaderState} = grb_paxos_state:prepare_hb(Id, LeaderState0),
    case Result of
        {ok, Ballot, Timestamp} ->
            ?LOG_DEBUG("~p: HEARTBEAT_PREPARE(~b, ~p, ~b)", [Partition, Ballot, Id, Timestamp]),
            grb_red_heartbeat:handle_accept_ack(Partition, Ballot, Id, Timestamp),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_heartbeat(ReplicaId, Partition,
                                                             SequenceNumber, Ballot, Id, Timestamp)
            end, grb_dc_connection_manager:connected_replicas());

        {already_decided, _Decision, _Timestamp} ->
            ?LOG_ERROR("~p heartbeat already decided, reused identifier ~p", [Partition, Id]),
            %% todo(borja, red): This shouldn't happen, but should let red_timer know
            erlang:error(heartbeat_already_decided)
    end,
    {noreply, S#state{synod_state=LeaderState, fifo_sequence_number=SequenceNumber+1}};

handle_command({prepare, TxId, Label, RS, WS, SnapshotVC}, Coordinator,
               S0=#state{synod_role=?leader, partition=Partition, pending_prepares=PendingPrepares}) ->

    ?LOG_QUEUE_LEN(Partition),
    Now = grb_time:timestamp(),
    ?MARK_SEEN_TX_TS(TxId, Now, S0),
    PrepareTs = grb_vclock:get_time(?RED_REPLICA, SnapshotVC),

    S = if
        Now =< PrepareTs ->
            %% If we're below the snapshot, wait until our local clock catches up.
            ok = grb_measurements:log_counter({?MODULE, Partition, prepare_not_ready}),
            erlang:send_after(grb_time:diff_ms(PrepareTs, Now), self(), {retry_prepare, TxId}),
            %% Store all data for later to avoid copying again on message send.
            S0#state{pending_prepares=PendingPrepares#{TxId => {Label, RS, WS, SnapshotVC, Coordinator}}};

        true ->
            prepare_internal(TxId, Label, RS, WS, SnapshotVC, Coordinator, S0)
    end,
    {noreply, S};

handle_command({decide_hb, Ballot, Id, Ts}, _Sender, S0=#state{synod_role=?leader,
                                                               partition=Partition}) ->
    ?LOG_DEBUG("~p: HEARTBEAT_DECIDE(~b, ~p, ~b)", [Partition, Ballot, Id, Ts]),
    S = case decide_hb_internal(Ballot, Id, Ts, S0) of
        {not_ready, Ms} ->
            erlang:send_after(Ms, self(), {retry_decide_hb, Ballot, Id, Ts}),
            S0;
        {ok, S1} ->
            S1
    end,
    {noreply, S};

handle_command({decision, Ballot, TxId, Decision, CommitTs}, _Sender, S0=#state{synod_role=?leader,
                                                                                partition=Partition}) ->
    ?LOG_DEBUG("~p DECIDE(~b, ~p, ~p)", [Partition, Ballot, TxId, Decision]),
    S = case decide_internal(Ballot, TxId, Decision, CommitTs, S0) of
        {not_ready, Ms} ->
            erlang:send_after(Ms, self(), {retry_decision, Ballot, TxId, Decision, CommitTs}),
            S0;
        {ok, S1} ->
            maybe_buffer_abort(Ballot, TxId, Decision, CommitTs, S1)
    end,
    {noreply, S};

%%%===================================================================
%%% follower protocol messages
%%%===================================================================

handle_command({sequence_msg, SequenceNumber, Payload}, _Sender,
               S0=#state{synod_role=?follower, fifo_sequence_number=NextSequenceNumber, pending_fifo_messages=Pending}) ->
    S = if
        NextSequenceNumber =:= SequenceNumber ->
            %% process message, advance Seq, re-process pending
            S1 = execute_follower_command(Payload, S0),
            reprocess_pending_messages(S1#state{fifo_sequence_number=NextSequenceNumber + 1});
        true ->
            S0#state{pending_fifo_messages=Pending#{SequenceNumber => Payload}}
    end,
    {noreply, S};

handle_command({learn_abort, Ballot, TxId, Reason, CommitTs}, _Sender,
               S0=#state{synod_role=?follower, partition=Partition}) ->

    ok = grb_oplog_vnode:clean_transaction_ops(Partition, TxId),
    AbortDecision = {abort, Reason},
    S = case decide_internal(Ballot, TxId, AbortDecision, CommitTs, S0) of
        {ok, S1} ->
            S1;
        not_prepared ->
            %% Stash for later
            Pending = S0#state.pending_abort_messages,
            S0#state{pending_abort_messages=Pending#{ {TxId, Ballot} => {AbortDecision, CommitTs} }}
    end,
    {noreply, ?REPORT_ABORT_TS(TxId, S)};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

-spec reprocess_pending_messages(#state{}) -> #state{}.
reprocess_pending_messages(S0=#state{fifo_sequence_number=N,
                                     pending_fifo_messages=Pending0}) ->
    case maps:take(N, Pending0) of
        error ->
            S0;
        {Msg, Pending} ->
            S1 = execute_follower_command(Msg, S0),
            reprocess_pending_messages(S1#state{pending_fifo_messages=Pending, fifo_sequence_number=N+1})
    end.

-spec reprocess_pending_aborts(term(), ballot(), #state{}) -> #state{}.
reprocess_pending_aborts(TxId, Ballot, S0=#state{pending_abort_messages=PendingAborts0}) ->
    case maps:take({TxId, Ballot}, PendingAborts0) of
        error ->
            S0;
        {{Decision, CommitTs}, PendingAborts} ->
            {ok, S} = decide_internal(Ballot, TxId, Decision, CommitTs, S0),
            S#state{pending_abort_messages=PendingAborts}
    end.

-spec execute_follower_command(pending_message(), #state{}) -> #state{}.
execute_follower_command({accept, Coordinator, Ballot, TxId, Label, RS, WS, Vote, PrepareVC},
                         State=#state{replica_id=LocalId, partition=Partition, synod_state=FollowerState0}) ->

    ?MARK_SEEN_TX_TS(TxId, grb_time:timestamp(), State),
    ?LOG_QUEUE_LEN(Partition),
    ?LOG_DEBUG("~p: ACCEPT(~b, ~p, ~p), reply to coordinator ~p", [Partition, Ballot, TxId, Vote, Coordinator]),
    PrepareTs = grb_vclock:get_time(?RED_REPLICA, PrepareVC),
    {ok, FollowerState} = grb_paxos_state:accept(Ballot, TxId, Label, RS, WS, Vote, PrepareVC, FollowerState0),
    ok = reply_accept_ack(Coordinator, LocalId, Partition, Ballot, TxId, Vote, PrepareTs),
    reprocess_pending_aborts(TxId, Ballot, State#state{synod_state=FollowerState});

execute_follower_command({accept_hb, SourceReplica, Ballot, Id, Ts},
                         S0=#state{partition=Partition, synod_state=FollowerState0}) ->

    ?LOG_DEBUG("~p: HEARTBEAT_ACCEPT(~b, ~p, ~b)", [Partition, Ballot, Id, Ts]),
    {ok, FollowerState} = grb_paxos_state:accept_hb(Ballot, Id, Ts, FollowerState0),
    ok = grb_dc_connection_manager:send_red_heartbeat_ack(SourceReplica, Partition, Ballot, Id, Ts),
    S0#state{synod_state=FollowerState};

execute_follower_command({deliver_transactions, Ballot, Timestamp, TransactionIds},
                         S0=#state{partition=Partition, last_delivered=LastDelivered}) ->

    Now = grb_time:timestamp(),
    ValidBallot = grb_paxos_state:deliver_is_valid_ballot(Ballot, S0#state.synod_state),
    if
        Timestamp > LastDelivered andalso ValidBallot ->
            %% We're at follower, so we will always be ready to receive a deliver event
            %% We already checked for a valid ballot above, so that can't fail.
            %% Due to FIFO, we will always receive a DELIVER after an ACCEPT from the same leader,
            %% so we don't have to worry about that either.
            S1 = lists:foldl(
                fun
                    ({?red_heartbeat_marker, _}=Id, Acc) ->
                        %% heartbeats always commit
                        ?LOG_DEBUG("~p: HEARTBEAT_DECIDE(~b, ~p, ~b)", [Partition, Ballot, Id, Timestamp]),
                        {ok, SAcc} = decide_hb_internal(Ballot, Id, Timestamp, Acc),
                        SAcc;

                    ({TxId, Label}, Acc) ->
                        ?REPORT_FOLLOWER_TS(TxId, Now, Partition),

                        %% We only receive committed transactions. Aborted transactions were received during decision.
                        ?LOG_DEBUG("~p DECIDE(~b, ~p, ~p)", [Partition, Ballot, TxId, ok]),
                        {ok, SAcc} = decide_internal(Ballot, TxId, ok, Timestamp, Acc),

                        %% Since it's committed, we can deliver it immediately
                        {WS, CommitVC} = grb_paxos_state:get_decided_data(TxId, SAcc#state.synod_state),
                        ?LOG_DEBUG("~p DELIVER(~p, ~p, ~p)", [Partition, Timestamp, Label, WS]),
                        ok = grb_oplog_vnode:handle_red_transaction(Partition, TxId, Label, WS, CommitVC),
                        SAcc
                end,
                S0,
                TransactionIds
            ),

            %% We won't receive more transactions with this (or less) timestamp, so we can perform a heartbeat
            ok = grb_oplog_vnode:handle_red_heartbeat(Partition, Timestamp),
            S1#state{last_delivered=Timestamp};

        true ->
            %% fixme(borja, red): What to do here if bad ballot?
            ?LOG_WARNING("DELIVER(~p, ~p) is not valid", [Ballot, Timestamp]),
            S0
    end.

handle_info({retry_prepare, TxId}, S0=#state{pending_prepares=PendingPrepares0}) ->
    S = case maps:take(TxId, PendingPrepares0) of
        error ->
            %% Shouldn't happen, but who knows (someone could send us a rogue message)
            ?LOG_ERROR("RETRY_PREPARE(~p) not found", [TxId]),
            S0;

        {{Label, RS, WS, SnapshotVC, Coordinator}, PendingPrepares} ->
            prepare_internal(TxId, Label, RS, WS, SnapshotVC, Coordinator, S0=#state{pending_prepares=PendingPrepares})
    end,
    {ok, S};

handle_info({retry_decide_hb, Ballot, Id, Ts}, S0) ->
    S = case decide_hb_internal(Ballot, Id, Ts, S0) of
        {not_ready, Ms} ->
            %% This shouldn't happen, we already made sure that we'd be ready when we received this message
            ?LOG_ERROR("DECIDE_HB(~p, ~p) retry", [Ballot, Id]),
            erlang:send_after(Ms, self(), {retry_decide_hb, Ballot, Id, Ts}),
            S0;
        {ok, S1} ->
            S1
    end,
    {ok, S};

handle_info({retry_decision, Ballot, TxId, Decision, CommitTs}, S0=#state{synod_role=?leader,
                                                                          partition=Partition}) ->
    S = case decide_internal(Ballot, TxId, Decision, CommitTs, S0) of
        {not_ready, Ms} ->
            %% This shouldn't happen, we already made sure that we'd be ready when we received this message
            ?LOG_ERROR("DECIDE(~p, ~p, ~p) retry", [Partition, Ballot, TxId]),
            erlang:send_after(Ms, self(), {retry_decision, Ballot, TxId, Decision, CommitTs}),
            S0;
        {ok, S1} ->
            maybe_buffer_abort(Ballot, TxId, Decision, CommitTs, S1)
    end,
    {ok, S};

handle_info(?deliver_event, S=#state{synod_role=?leader,
                                     partition=Partition,
                                     synod_state=SynodState,
                                     last_delivered=LastDelivered0,
                                     deliver_timer=Timer,
                                     deliver_interval=Interval,
                                     fifo_sequence_number=StartingSeq}) ->
    ?CANCEL_TIMER_FAST(Timer),
    CurBallot = grb_paxos_state:current_ballot(SynodState),
    {LastDelivered, NewSeq} = ?DELIVER_UPDATES(Partition, StartingSeq, CurBallot, LastDelivered0, SynodState),
    if
        LastDelivered > LastDelivered0 ->
            ?LOG_DEBUG("~p DELIVER_HB(~b)", [Partition, LastDelivered]),
            ok = grb_oplog_vnode:handle_red_heartbeat(Partition, LastDelivered);
        true ->
            ok
    end,
    {ok, S#state{fifo_sequence_number=NewSeq,
                 last_delivered=LastDelivered,
                 deliver_timer=erlang:send_after(Interval, self(), ?deliver_event)}};

handle_info(?prune_event, S=#state{last_delivered=LastDelivered,
                                   synod_state=SynodState,
                                   prune_timer=Timer,
                                   prune_interval=Interval}) ->

    ?CANCEL_TIMER_FAST(Timer),
    ?LOG_DEBUG("~p PRUNE_BEFORE(~b)", [S#state.partition, LastDelivered]),
    %% todo(borja, red): Should compute MinLastDelivered
    %% To know the safe cut-off point, we should exchange LastDelivered with all replicas and find
    %% the minimum. Either replicas send a message to the leader, which aggregates the min and returns,
    %% or we build some tree.
    {ok, S#state{synod_state=grb_paxos_state:prune_decided_before(LastDelivered, SynodState),
                 prune_timer=erlang:send_after(Interval, self(), ?prune_event)}};

handle_info(?send_aborts_event, S=#state{synod_role=?leader,
                                         partition=Partition,
                                         abort_buffer_io=AbortBuffer,
                                         send_aborts_timer=Timer,
                                         send_aborts_interval_ms=Interval}) ->
    ?CANCEL_TIMER_FAST(Timer),
    ok = send_abort_buffer(Partition, AbortBuffer),
    {ok, S#state{abort_buffer_io=[],
                 send_aborts_timer=erlang:send_after(Interval, self(), ?send_aborts_event)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec start_timers(#state{}) -> #state{}.
start_timers(S=#state{synod_role=?leader, deliver_interval=DeliverInt,
                      prune_interval=PruneInt, send_aborts_interval_ms=AbortInt}) ->

    reschedule_heartbeat(S#state{prune_timer=grb_dc_utils:maybe_send_after(PruneInt, ?prune_event),
                                 deliver_timer=grb_dc_utils:maybe_send_after(DeliverInt, ?deliver_event),
                                 send_aborts_timer=grb_dc_utils:maybe_send_after(AbortInt, ?send_aborts_event)});

start_timers(S=#state{synod_role=?follower, prune_interval=PruneInt}) ->
    S#state{prune_timer=grb_dc_utils:maybe_send_after(PruneInt, ?prune_event)}.

-spec prepare_internal(term(), tx_label(), readset(), writeset(), vclock(), red_coord_location(), #state{}) -> #state{}.
prepare_internal(TxId, Label, RS, WS, SnapshotVC, Coordinator,
                 S0=#state{replica_id=LocalId, partition=Partition,
                           synod_state=LeaderState0, conflict_relations=Conflicts,
                           op_log_last_vc_replica=LastRed, fifo_sequence_number=SequenceNumber}) ->

    %% Cancel and resubmit any pending heartbeats for this partition.
    S1 = reschedule_heartbeat(S0),
    {Result, LeaderState} = grb_paxos_state:prepare(TxId, Label, RS, WS, SnapshotVC, LastRed, Conflicts, LeaderState0),
    ?LOG_DEBUG("~p: ~p prepared as ~p, reply to coordinator ~p", [Partition, TxId, Result, Coordinator]),
    case Result of
        {already_decided, Decision, CommitVC} ->
            %% skip replicas, this is enough to reply to the client
            reply_already_decided(Coordinator, LocalId, Partition, TxId, Decision, CommitVC),
            S1#state{synod_state=LeaderState};

        {Vote, Ballot, PrepareVC} ->
            PrepareTs = grb_vclock:get_time(?RED_REPLICA, PrepareVC),
            ok = reply_accept_ack(Coordinator, LocalId, Partition, Ballot, TxId, Vote, PrepareTs),
            ok = send_accepts(Partition, Coordinator, SequenceNumber, Ballot, TxId, Label, Vote, RS, WS, PrepareVC),
            S1#state{synod_state=LeaderState, fifo_sequence_number=SequenceNumber + 1}
    end.

-spec reply_accept_ack(red_coord_location(), replica_id(), partition_id(), ballot(), term(), red_vote(), grb_time:ts()) -> ok.
-ifndef(ENABLE_METRICS).
reply_accept_ack({coord, Replica, Node}, MyReplica, Partition, Ballot, TxId, Vote, PrepareTS) ->
    if
        Replica =:= MyReplica ->
            grb_red_coordinator:accept_ack(Node, MyReplica, Partition, Ballot, TxId, Vote, PrepareTS);
        true ->
            grb_dc_connection_manager:send_red_accept_ack(Replica, Node, Partition, Ballot, TxId, Vote, PrepareTS)
    end.
-else.
reply_accept_ack({coord, Replica, Node}, MyReplica, Partition, Ballot, TxId, Vote, PrepareTS) ->
    SendTS = grb_time:timestamp(),
    if
        Replica =:= MyReplica ->
            grb_red_coordinator:accept_ack({SendTS, Node}, MyReplica, Partition, Ballot, TxId, Vote, PrepareTS);
        true ->
            Msg = grb_dc_messages:frame(grb_dc_messages:red_accept_ack({SendTS, Node}, Ballot, Vote, TxId, PrepareTS)),
            grb_dc_connection_manager:send_raw_framed(Replica, Partition, Msg)
    end.
-endif.

-spec reply_already_decided(red_coord_location(), replica_id(), partition_id(), term(), red_vote(), vclock()) -> ok.
reply_already_decided({coord, Replica, Node}, MyReplica, Partition, TxId, Decision, CommitVC) ->
    if
        Replica =:= MyReplica ->
            grb_red_coordinator:already_decided(Node, TxId, Decision, CommitVC);
        true ->
            grb_dc_connection_manager:send_red_already_decided(Replica, Node, Partition, TxId, Decision, CommitVC)
    end.

-spec send_accepts(Partition :: partition_id(),
                   Coordinator :: red_coord_location(),
                   SequenceNumber :: non_neg_integer(),
                   Ballot :: ballot(),
                   TxId :: term(),
                   Label :: tx_label(),
                   Decision :: red_vote(),
                   RS :: readset(),
                   WS :: writeset(),
                   PrepareVC :: vclock()) -> ok.

-ifndef(ENABLE_METRICS).
send_accepts(Partition, Coordinator, SequenceNumber, Ballot, TxId, Label, Decision, RS, WS, PrepareVC) ->
    AcceptMsgIO = grb_dc_messages:frame(grb_dc_messages:red_accept(SequenceNumber, Coordinator, Ballot, Decision, TxId, Label, RS, WS, PrepareVC)),
    lists:foreach(
        fun(R) -> grb_dc_connection_manager:send_raw_framed(R, Partition, AcceptMsgIO) end,
        grb_dc_connection_manager:connected_replicas()
    ).
-else.
send_accepts(Partition, Coordinator, SequenceNumber, Ballot, TxId, Label, Decision, RS, WS, PrepareVC) ->
    SendTS = grb_time:timestamp(),
    AcceptMsgIO = grb_dc_messages:frame(grb_dc_messages:red_accept(SequenceNumber, {SendTS, Coordinator}, Ballot, Decision, TxId, Label, RS, WS, PrepareVC)),
    lists:foreach(
        fun(R) -> grb_dc_connection_manager:send_raw_framed(R, Partition, AcceptMsgIO) end,
        grb_dc_connection_manager:connected_replicas()
    ).
-endif.

-spec decide_hb_internal(ballot(), term(), grb_time:ts(), #state{}) -> {ok, #state{}} | {not_ready, non_neg_integer()}.
%% this is here due to grb_paxos_state:decision_hb/4, that dialyzer doesn't like
%% because it passes an integer as a clock
-dialyzer({nowarn_function, decide_hb_internal/4}).
decide_hb_internal(Ballot, Id, Ts, S=#state{synod_role=Role,
                                            synod_state=SynodState0}) ->
    Now = grb_time:timestamp(),
    if
        (Role =:= ?leader) and (Now < Ts) ->
            {not_ready, grb_time:diff_ms(Now, Ts)};

        true ->
            case grb_paxos_state:decision_hb(Ballot, Id, Ts, SynodState0) of
                {ok, SynodState} ->
                    {ok, S#state{synod_state=SynodState}};

                bad_ballot ->
                    ok = grb_measurements:log_counter({?MODULE, bad_ballot}),
                    %% fixme(borja, red): should this initiate a leader recovery, or ignore?
                    ?LOG_ERROR("~p: bad heartbeat ballot ~b", [S#state.partition, Ballot]),
                    {ok, S};

                not_prepared ->
                    ok = grb_measurements:log_counter({?MODULE, out_of_order_decision}),
                    %% fixme(borja, red): something very wrong happened due to FIFO, leader might have changed
                    ?LOG_ERROR("~p: out-of-order decision (~b) for a not prepared transaction ~p", [S#state.partition, Ballot, Id]),
                    {ok, S}
            end
    end.

-spec decide_internal(ballot(), term(), red_vote(), grb_time:ts(), #state{}) -> {ok, #state{}} | not_prepared | {not_ready, non_neg_integer()}.
decide_internal(Ballot, TxId, Decision, CommitTs, S=#state{synod_role=Role,
                                                           synod_state=SynodState0}) ->
    Now = grb_time:timestamp(),
    if
        (Role =:= ?leader) and (Now < CommitTs) ->
            ok = grb_measurements:log_counter({?MODULE, S#state.partition, decision_not_ready}),
            {not_ready, grb_time:diff_ms(Now, CommitTs)};

        true ->
            case grb_paxos_state:decision(Ballot, TxId, Decision, CommitTs, SynodState0) of
                {ok, SynodState} ->
                    {ok, S#state{synod_state=SynodState}};

                not_prepared ->
                    ok = grb_measurements:log_counter({?MODULE, out_of_order_decision}),
                    not_prepared;

                bad_ballot ->
                    ok = grb_measurements:log_counter({?MODULE, bad_ballot}),
                    %% fixme(borja, red): should this initiate a leader recovery, or ignore?
                    ?LOG_ERROR("~p: bad ballot ~b for ~p", [S#state.partition, Ballot, TxId]),
                    {ok, S}
            end
    end.

-spec maybe_buffer_abort(ballot(), term(), red_vote(), grb_time:ts(), #state{}) -> #state{}.
maybe_buffer_abort(_Ballot, TxId, ok, _CommitTs, State) ->
    %% If this is a commit, we can wait until delivery
    ?ADD_COMMIT_TS(TxId, State);
maybe_buffer_abort(Ballot, TxId, {abort, Reason}, CommitTs, State=#state{partition=Partition,
                                                                         abort_buffer_io=AbortBuffer,
                                                                         send_aborts_interval_ms=Ms}) ->
    ok = grb_oplog_vnode:clean_transaction_ops(Partition, TxId),
    FramedAbortMsg = grb_dc_messages:frame(grb_dc_messages:red_learn_abort(Ballot, TxId, Reason, CommitTs)),
    ?REPORT_ABORT_TS(TxId, if
        Ms > 0 ->
            %% Abort delay is active.
            State#state{abort_buffer_io=[AbortBuffer, FramedAbortMsg]};
        true ->
            %% Abort delay is disabled, send immediately.
            ok = send_abort_buffer(Partition, FramedAbortMsg),
            State
    end).

-spec send_abort_buffer(partition_id(), iodata()) -> ok.
send_abort_buffer(_Partition, []) ->
    ok;
send_abort_buffer(Partition, IOAborts) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_raw_framed(ReplicaId, Partition, IOAborts)
    end, grb_dc_connection_manager:connected_replicas()).

%% @doc Deliver all available updates with commit timestamp higher than `From`.
%%
%%      Returns the commit timestamp of the last transaction or heartbeat to be delivered.
%%
-spec deliver_updates(partition_id(), non_neg_integer(), ballot(), grb_time:ts(), grb_paxos_state:t()) -> {grb_time:ts(), non_neg_integer()}.
-ifndef(ENABLE_METRICS).
deliver_updates(Partition, SequenceNumber, Ballot, From, SynodState) ->
    {NewFrom, NewSeq, IOList} = deliver_updates_1(Partition, SequenceNumber, Ballot, From, SynodState, []),
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_raw_framed(ReplicaId, Partition, IOList)
    end, grb_dc_connection_manager:connected_replicas()),
    {NewFrom, NewSeq}.

deliver_updates_1(Partition, SequenceNumber, Ballot, From, SynodState, IOAcc) ->
    case grb_paxos_state:get_next_ready(From, SynodState) of
        false ->
            {From, SequenceNumber, IOAcc};

        {NextFrom, Entries} ->
            %% Collect only the identifiers for the transactions, we don't care
            %% about the writeset, followers already have it.
            %%
            %% No need to reverse the accumulator, since they all have the same
            %% commit timestamp, they can be delivered in any order.
            Identifiers = lists:foldl(
                fun
                    ( {?red_heartbeat_marker, _}=Heartbeat, Acc) ->
                        [ Heartbeat | Acc ];

                    ({TxId, Label, WriteSet, CommitVC}, Acc) ->
                        if
                            is_map(WriteSet) andalso map_size(WriteSet) > 0 ->
                                ?LOG_DEBUG("~p DELIVER(~p, ~p, ~p)", [Partition, NextFrom, Label, WriteSet]),
                                ok = grb_oplog_vnode:handle_red_transaction(Partition, TxId, Label, WriteSet, CommitVC);
                            true ->
                                ok
                        end,
                        [ { TxId, Label } | Acc ]
                end,
                [],
                Entries
            ),

            Msg = grb_dc_messages:frame(grb_dc_messages:red_deliver(Partition, SequenceNumber, Ballot, NextFrom, Identifiers)),
            deliver_updates_1(Partition, SequenceNumber + 1, Ballot, NextFrom, SynodState, [IOAcc, Msg])
    end.
-else.
deliver_updates(Partition, SequenceNumber, Ballot, From, SynodState) ->
    {NewFrom, NewSeq, IOList} = deliver_updates(Partition, SequenceNumber, Ballot, From, SynodState, []),
    grb_measurements:log_stat(?DELIVER_CALLED_N(Partition), (NewSeq - SequenceNumber)),
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_raw_framed(ReplicaId, Partition, IOList)
    end, grb_dc_connection_manager:connected_replicas()),
    {NewFrom, NewSeq}.

deliver_updates(Partition, SequenceNumber, Ballot, From, SynodState, IOAcc) ->
    Now = grb_time:timestamp(),
    {Took, Res} = timer:tc(grb_paxos_state, get_next_ready, [From, SynodState]),
    grb_measurements:log_stat(?NEXT_READY_DURATION(Partition), Took),
    case Res of
        false ->
            {From, SequenceNumber, IOAcc};

        {NextFrom, Entries} ->
            Identifiers = lists:foldl(
                fun
                    ( {?red_heartbeat_marker, _}=Heartbeat, Acc) ->
                        [ Heartbeat | Acc ];

                    ({TxId, Label, WriteSet, CommitVC}, Acc) ->
                        if
                            is_map(WriteSet) andalso map_size(WriteSet) > 0 ->
                                ?REPORT_LEADER_TS(TxId, Now, Partition),
                                ok = grb_oplog_vnode:handle_red_transaction(Partition, TxId, Label, WriteSet, CommitVC);
                            true ->
                                ok
                        end,
                        [ { TxId, Label } | Acc ]
                end,
                [],
                Entries
            ),

            Msg = grb_dc_messages:frame(grb_dc_messages:red_deliver(Partition, SequenceNumber, Ballot, NextFrom, Identifiers)),
            deliver_updates(Partition, SequenceNumber + 1, Ballot, NextFrom, SynodState, [IOAcc, Msg])
    end.
-endif.

%% Strong heartbeats are expensive, since they are equivalent
%% to a read-only strong transaction that always commits. If
%% we're sending a heartbeat every X ms, that's (1000 / X)
%% transactions per second and per partition being performed.
%%
%% To avoid paying the price, we look if the process is currently
%% preparing any transactions. If the system is not doing anything,
%% we will schedule a heartbeat. If, on the other hand, there are
%% transactions being prepared, then we know the client is submitting
%% transactions, and thus the system will keep advancing.
%%
%% What we will do is schedule an inital timer when the process starts,
%% and keep doing that whenever we receive a new prepare. If we receive
%% a heartbeat instead, we will simply schedule a new one in the future.
-spec reschedule_heartbeat(#state{}) -> #state{}.
-ifndef(DISABLE_STRONG_HEARTBEAT).
reschedule_heartbeat(S=#state{heartbeat_process=HBPid,
                              heartbeat_schedule_ms=DelayMs,
                              heartbeat_schedule_timer=Timer}) ->
    if
        is_reference(Timer) -> ?CANCEL_TIMER_FAST(Timer);
        true -> ok
    end,
    S#state{heartbeat_schedule_timer=grb_red_heartbeat:schedule_heartbeat(HBPid, DelayMs)}.
-else.
reschedule_heartbeat(S) -> S.
-endif.

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

delete(State) ->
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.
