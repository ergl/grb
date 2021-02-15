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
         accept_heartbeat/5,
         decide_heartbeat/4]).

%% tx API
-export([prepare/7,
         accept/9,
         decide/5,
         learn_abort/5,
         deliver/4]).

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

-define(INIT_LEADER_METRICS,
    ok = grb_measurements:create_stat({?MODULE, leader_message_queue})).
-define(INIT_FOLLOWER_METRICS,
    ok = grb_measurements:create_stat({?MODULE, follower_message_queue})).
-define(LOG_LEADER_QUEUE,
    ok = grb_measurements:log_queue_length({?MODULE, leader_message_queue})).
-define(LOG_FOLLOWER_QUEUE,
    ok = grb_measurements:log_queue_length({?MODULE, follower_message_queue})).

-define(leader, leader).
-define(follower, follower).
-type role() :: ?leader | ?follower.

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

    %% a buffer of outstanding decision messages
    %% If we receive a DECIDE message from the coordinator before we receive
    %% an ACCEPT from the leader, we should buffer the DECIDE here, and apply it
    %% right after we receive an ACCEPT with a matching ballot and transaction id
    decision_buffer = #{} :: #{{term(), ballot()} => {red_vote(), vclock()} | grb_time:ts()},

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
    abort_buffer_io = [] :: iodata()
}).

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
    riak_core_vnode_master:command({Partition, node()},
                                   {prepare_hb, Id},
                                   ?master).

-spec accept_heartbeat(partition_id(), replica_id(), term(), ballot(), grb_time:ts()) -> ok.
accept_heartbeat(Partition, SourceReplica, Ballot, Id, Ts) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {accept_hb, SourceReplica, Ballot, Id, Ts},
                                   ?master).

-spec decide_heartbeat(index_node(), ballot(), term(), grb_time:ts()) -> ok.
decide_heartbeat(IndexNode, Ballot, Id, Ts) ->
    riak_core_vnode_master:command(IndexNode,
                                   {decide_hb, Ballot, Id, Ts},
                                   ?master).

-spec prepare(IndexNode :: index_node(),
              TxId :: term(),
              Label :: tx_label(),
              Readset :: readset(),
              WriteSet :: writeset(),
              SnapshotVC :: vclock(),
              Coord :: red_coord_location()) -> ok.

prepare(IndexNode, TxId, Label, ReadSet, Writeset, SnapshotVC, Coord) ->
    riak_core_vnode_master:command(IndexNode,
                                   {prepare, TxId, Label, ReadSet, Writeset, SnapshotVC},
                                   Coord,
                                   ?master).

-spec accept(Partition :: partition_id(),
             Ballot :: ballot(),
             TxId :: term(),
             Label :: tx_label(),
             RS :: readset(),
             WS :: writeset(),
             Vote :: red_vote(),
             PrepareVC :: vclock(),
             Coord :: red_coord_location()) -> ok.

accept(Partition, Ballot, TxId, Label, RS, WS, Vote, PrepareVC, Coord) ->
    %% For blue transactions, any pending operations are removed during blue commit, since
    %% it is performed at the replica / partitions where the client originally performed the
    %% operations. For red commit, however, the client can start the red commit at any
    %% partition, so we aren't able to prune them. With this, we prune the operations
    %% for red transactions when we accept them
    ok = grb_oplog_vnode:clean_transaction_ops(Partition, TxId),
    riak_core_vnode_master:command({Partition, node()},
                                   {accept, Ballot, TxId, Label, RS, WS, Vote, PrepareVC},
                                   Coord,
                                   ?master).

-spec decide(index_node(), ballot(), term(), red_vote(), vclock()) -> ok.
decide(IndexNode, Ballot, TxId, Decision, CommitVC) ->
    riak_core_vnode_master:command(IndexNode,
                                   {decision, Ballot, TxId, Decision, CommitVC},
                                   ?master).

-spec learn_abort(partition_id(), ballot(), term(), term(), vclock()) -> ok.
learn_abort(Partition, Ballot, TxId, Reason, CommitVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {learn_abort, Ballot, TxId, Reason, CommitVC},
                                   ?master).

-spec deliver(partition_id(), ballot(), grb_time:ts(), [ {term(), term(), #{}, vclock()} | {term(), term()} ]) -> ok.
deliver(Partition, Ballot, Timestamp, Transactions) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {deliver_transactions, Ballot, Timestamp, Transactions},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, DeliverInterval} = application:get_env(grb, red_delivery_interval),
    PruningInterval = application:get_env(grb, red_prune_interval, 0),

    %% conflict information can be overwritten by calling grb:put_conflicts/1
    Conflicts = application:get_env(grb, red_conflicts_config, #{}),

    %% only at the leader, but we don't care
    {ok, HeartbeatScheduleMs} = application:get_env(grb, red_heartbeat_schedule_ms),
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
    {ok, State}.

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
    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Pid} = grb_red_heartbeat:new(ReplicaId, Partition),
    ?INIT_LEADER_METRICS,
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     heartbeat_process=Pid,
                                     synod_role=?leader,
                                     synod_state=grb_paxos_state:new()})};

handle_command(init_follower, _Sender, S=#state{synod_role=undefined, synod_state=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    ?INIT_FOLLOWER_METRICS,
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     synod_role=?follower,
                                     synod_state=grb_paxos_state:new()})};

%%%===================================================================
%%% leader protocol messages
%%%===================================================================

handle_command({prepare_hb, Id}, _Sender, S=#state{synod_role=?leader,
                                                   partition=Partition,
                                                   synod_state=LeaderState0}) ->

    %% Cancel the timer (it's the incoming heartbeat)
    ok = cancel_schedule_heartbeat_timer(S#state.heartbeat_schedule_timer),

    {Result, LeaderState} = grb_paxos_state:prepare_hb(Id, LeaderState0),
    case Result of
        {ok, Ballot, Timestamp} ->
            ?LOG_DEBUG("~p: HEARTBEAT_PREPARE(~b, ~p, ~b)", [Partition, Ballot, Id, Timestamp]),
            grb_red_heartbeat:handle_accept_ack(Partition, Ballot, Id, Timestamp),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_heartbeat(ReplicaId, Partition,
                                                             Ballot, Id, Timestamp)
            end, grb_dc_connection_manager:connected_replicas());

        {already_decided, _Decision, _Timestamp} ->
            ?LOG_ERROR("~p heartbeat already decided, reused identifier ~p", [Partition, Id]),
            %% todo(borja, red): This shouldn't happen, but should let red_timer know
            erlang:error(heartbeat_already_decided)
    end,
    {noreply, S#state{synod_state=LeaderState,
                      %% clean the timer so that we know that we need to schedule a new one in the future
                      heartbeat_schedule_timer=undefined}};

handle_command({prepare, TxId, Label, RS, WS, SnapshotVC},
               Coordinator, S=#state{synod_role=?leader,
                                     replica_id=LocalId,
                                     partition=Partition,
                                     synod_state=LeaderState0,
                                     conflict_relations=Conflicts,
                                     op_log_last_vc_replica=LastRed}) ->

    %% Cancel any pending heartbeats, since we're preparing transactions.
    %% This means clients are submitting transactions, so we don't need the overhead.
    ok = cancel_schedule_heartbeat_timer(S#state.heartbeat_schedule_timer),

    ?LOG_LEADER_QUEUE,

    %% For blue transactions, any pending operations are removed during blue commit, since
    %% it is performed at the replica / partitions where the client originally performed the
    %% operations. For red commit, however, the client can start the red commit at any
    %% partition, so we aren't able to prune them. With this, we prune the operations
    %% for red transactions when we prepare them
    ok = grb_oplog_vnode:clean_transaction_ops(Partition, TxId),

    {Result, LeaderState} = grb_paxos_state:prepare(TxId, Label, RS, WS, SnapshotVC, LastRed, Conflicts, LeaderState0),
    ?LOG_DEBUG("~p: ~p prepared as ~p, reply to coordinator ~p", [Partition, TxId, Result, Coordinator]),
    case Result of
        {already_decided, Decision, CommitVC} ->
            %% skip replicas, this is enough to reply to the client
            reply_already_decided(Coordinator, LocalId, Partition, TxId, Decision, CommitVC);
        {Vote, Ballot, PrepareVC} ->
            reply_accept_ack(Coordinator, LocalId, Partition, Ballot, TxId, Vote, PrepareVC),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_accept(ReplicaId, Coordinator, Partition,
                                                          Ballot, Vote, TxId, Label, RS, WS, PrepareVC)
            end, grb_dc_connection_manager:connected_replicas())
    end,
    {noreply, S#state{synod_state=LeaderState,
                      heartbeat_schedule_timer=undefined}};

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

handle_command({decision, Ballot, TxId, Decision, CommitVC}, _Sender, S0=#state{synod_role=?leader,
                                                                                partition=Partition}) ->
    ?LOG_DEBUG("~p DECIDE(~b, ~p, ~p)", [Partition, Ballot, TxId, Decision]),
    S = case decide_internal(Ballot, TxId, Decision, CommitVC, S0) of
        {not_ready, Ms} ->
            erlang:send_after(Ms, self(), {retry_decision, Ballot, TxId, Decision, CommitVC}),
            S0;
        {ok, S1} ->
            maybe_buffer_abort(Ballot, TxId, Decision, CommitVC, S1)
    end,
    {noreply, S};

%%%===================================================================
%%% follower protocol messages
%%%===================================================================

handle_command({accept, Ballot, TxId, Label, RS, WS, Vote, PrepareVC},
                Coordinator, S0=#state{replica_id=LocalId,
                                       partition=Partition,
                                       synod_state=FollowerState0,
                                       decision_buffer=DecisionBuffer0}) ->

    ?LOG_FOLLOWER_QUEUE,

    ?LOG_DEBUG("~p: ACCEPT(~b, ~p, ~p), reply to coordinator ~p", [Partition, Ballot, TxId, Vote, Coordinator]),
    {ok, FollowerState} = grb_paxos_state:accept(Ballot, TxId, Label, RS, WS, Vote, PrepareVC, FollowerState0),
    S1 = S0#state{synod_state=FollowerState},
    S = case maps:take({TxId, Ballot}, DecisionBuffer0) of
        error ->
            reply_accept_ack(Coordinator, LocalId, Partition, Ballot, TxId, Vote, PrepareVC),
            S1;

        {{Decision, CommitVC}, DecisionBuffer} ->
            %% if the coordinator already sent us a decision, there's no need to cast an ACCEPT_ACK message
            %% because the coordinator does no longer care
            ?LOG_DEBUG("~p: buffered DECIDE(~b, ~p, ~p)", [Partition, Ballot, TxId, Decision]),
            {ok, S2} = decide_internal(Ballot, TxId, Decision, CommitVC, S1#state{decision_buffer=DecisionBuffer}),
            S2
    end,
    {noreply, S};

handle_command({accept_hb, SourceReplica, Ballot, Id, Ts}, _Sender, S0=#state{partition=Partition,
                                                                              synod_state=FollowerState0,
                                                                              decision_buffer=DecisionBuffer0}) ->

    ?LOG_DEBUG("~p: HEARTBEAT_ACCEPT(~b, ~p, ~b)", [Partition, Ballot, Id, Ts]),
    {ok, FollowerState} = grb_paxos_state:accept_hb(Ballot, Id, Ts, FollowerState0),
    S1 = S0#state{synod_state=FollowerState},
    S = case maps:take({Id, Ballot}, DecisionBuffer0) of
        error ->
            ok = grb_dc_connection_manager:send_red_heartbeat_ack(SourceReplica, Partition, Ballot, Id, Ts),
            S1;

        {FinalTs, DecisionBuffer} ->
            %% if the coordinator already sent us a decision, there's no need to cast an ACCEPT_ACK message
            %% because the coordinator does no longer care
            ?LOG_DEBUG("~p: buffered DECIDE_HB(~b, ~p)", [Partition, Ballot, Id]),
            {ok, S2} = decide_hb_internal(Ballot, Id, FinalTs, S1#state{decision_buffer=DecisionBuffer}),
            S2
    end,
    {noreply, S};

handle_command({learn_abort, Ballot, TxId, Reason, CommitVC}, _Sender, S0=#state{partition=Partition}) ->
    ?LOG_DEBUG("~p LEARN_ABORT(~b, ~p, ~p)", [Partition, Ballot, TxId]),
    {ok, S} = decide_internal(Ballot, TxId, {abort, Reason}, CommitVC, S0),
    {noreply, S};

handle_command({deliver_transactions, Ballot, Timestamp, Transactions}, _Sender, S0=#state{synod_role=?follower,
                                                                                           partition=Partition,
                                                                                           last_delivered=LastDelivered}) ->
    ValidBallot = grb_paxos_state:deliver_is_valid_ballot(Ballot, S0#state.synod_state),
    S = if
        Timestamp > LastDelivered andalso ValidBallot ->
            %% We're at follower, so we will always be ready to receive a deliver event
            %% We already checked for a valid ballot above, so that can't fail.
            %% Due to FIFO, we will always receive a DELIVER after an ACCEPT from the same leader,
            %% so we don't have to worry about that either.
            S1 = lists:foldl(
                fun
                    ({heartbeat, _}=Id, Acc) ->
                        %% heartbeats always commit
                        ?LOG_DEBUG("~p: HEARTBEAT_DECIDE(~b, ~p, ~b)", [Partition, Ballot, Id, Timestamp]),
                        {ok, SAcc} = decide_hb_internal(Ballot, Id, Timestamp, Acc),
                        SAcc;

                    ({TxId, Label, WS, CommitVC}, Acc) ->
                        %% We only receive committed transactions. Aborted transactions were received during decision.
                        ?LOG_DEBUG("~p DECIDE(~b, ~p, ~p)", [Partition, Ballot, TxId, ok]),
                        {ok, SAcc} = decide_internal(Ballot, TxId, ok, CommitVC, Acc),

                        %% Since it's committed, we can deliver it immediately
                        ?LOG_DEBUG("~p DELIVER(~p, ~p, ~p)", [Partition, Timestamp, Label, WS]),
                        ok = grb_oplog_vnode:handle_red_transaction(Partition, Label, WS, CommitVC),
                        SAcc
                end,
                S0,
                Transactions
            ),
            %% We won't receive more transactions with this (or less) timestamp, so we can perform a heartbeat
            ok = grb_propagation_vnode:handle_red_heartbeat(Partition, Timestamp),
            S1#state{last_delivered=Timestamp};

        true ->
            %% fixme(borja, red): What to do here if bad ballot?
            ?LOG_WARNING("DELIVER(~p, ~p) is not valid", [Ballot, Timestamp]),
            S0
    end,
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

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

handle_info({retry_decision, Ballot, TxId, Decision, CommitVC}, S0=#state{synod_role=?leader}) ->
    S = case decide_internal(Ballot, TxId, Decision, CommitVC, S0) of
        {not_ready, Ms} ->
            %% This shouldn't happen, we already made sure that we'd be ready when we received this message
            ?LOG_ERROR("DECIDE(~p, ~p) retry", [Ballot, TxId]),
            erlang:send_after(Ms, self(), {retry_decision, Ballot, TxId, Decision, CommitVC}),
            S0;
        {ok, S1} ->
            maybe_buffer_abort(Ballot, TxId, Decision, CommitVC, S1)
    end,
    {ok, S};

handle_info(?deliver_event, S=#state{synod_role=?leader,
                                     partition=Partition,
                                     synod_state=SynodState,
                                     last_delivered=LastDelivered0,
                                     deliver_timer=Timer,
                                     deliver_interval=Interval}) ->
    ?CANCEL_TIMER_FAST(Timer),
    CurBallot = grb_paxos_state:current_ballot(SynodState),
    {LastDelivered, DeliveredAnyTx} = deliver_updates(Partition, CurBallot, LastDelivered0, SynodState),
    if
        LastDelivered > LastDelivered0 ->
            ?LOG_DEBUG("~p DELIVER_HB(~b)", [Partition, LastDelivered]),
            ok = grb_propagation_vnode:handle_red_heartbeat(Partition, LastDelivered);
        true ->
            ok
    end,
    {ok, maybe_schedule_heartbeat(DeliveredAnyTx,
                                  S#state{last_delivered=LastDelivered,
                                          deliver_timer=erlang:send_after(Interval, self(), ?deliver_event)})};

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

    S#state{prune_timer=grb_dc_utils:maybe_send_after(PruneInt, ?prune_event),
            deliver_timer=grb_dc_utils:maybe_send_after(DeliverInt, ?deliver_event),
            send_aborts_timer=grb_dc_utils:maybe_send_after(AbortInt, ?send_aborts_event)};

start_timers(S=#state{synod_role=?follower, prune_interval=PruneInt}) ->
    S#state{prune_timer=grb_dc_utils:maybe_send_after(PruneInt, ?prune_event)}.

-spec reply_accept_ack(red_coord_location(), replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
reply_accept_ack({coord, Replica, Node}, MyReplica, Partition, Ballot, TxId, Vote, PrepareVC) ->
    MyNode = node(),
    case {Replica, Node} of
        {MyReplica, MyNode} ->
            grb_red_coordinator:accept_ack(Partition, Ballot, TxId, Vote, PrepareVC);
        {MyReplica, OtherNode} ->
            erpc:cast(OtherNode, grb_red_coordinator, accept_ack, [Partition, Ballot, TxId, Vote, PrepareVC]);
        {OtherReplica, _} ->
            grb_dc_connection_manager:send_red_accept_ack(OtherReplica, Node, Partition, Ballot, TxId, Vote, PrepareVC)
    end.

-spec reply_already_decided(red_coord_location(), replica_id(), partition_id(), term(), red_vote(), vclock()) -> ok.
reply_already_decided({coord, Replica, Node}, MyReplica, Partition, TxId, Decision, CommitVC) ->
    MyNode = node(),
    case {Replica, Node} of
        {MyReplica, MyNode} ->
            grb_red_coordinator:already_decided(TxId, Decision, CommitVC);
        {MyReplica, OtherNode} ->
            erpc:cast(OtherNode, grb_red_coordinator, already_decided, [TxId, Decision, CommitVC]);
        {OtherReplica, _} ->
            grb_dc_connection_manager:send_red_already_decided(OtherReplica, Node, Partition, TxId, Decision, CommitVC)
    end.

-spec decide_hb_internal(ballot(), term(), grb_time:ts(), #state{}) -> {ok, #state{}} | {not_ready, non_neg_integer()}.
%% this is here due to grb_paxos_state:decision_hb/4, that dialyzer doesn't like
%% because it passes an integer as a clock
-dialyzer({nowarn_function, decide_hb_internal/4}).
decide_hb_internal(Ballot, Id, Ts, S=#state{synod_role=Role,
                                            synod_state=SynodState0,
                                            decision_buffer=Buffer}) ->
    Now = grb_time:timestamp(),
    if
        (Role =:= ?leader) and (Now < Ts) ->
            {not_ready, grb_time:diff_ms(Now, Ts)};

        true ->
            case grb_paxos_state:decision_hb(Ballot, Id, Ts, SynodState0) of
                {ok, SynodState} ->
                    {ok, S#state{synod_state=SynodState}};

                bad_ballot ->
                    %% fixme(borja, red): should this initiate a leader recovery, or ignore?
                    ?LOG_ERROR("~p: bad heartbeat ballot ~b", [S#state.partition, Ballot]),
                    {ok, S};

                not_prepared ->
                    ?LOG_DEBUG("~p: DECIDE_HEARTBEAT(~b, ~p) := not_prepared, buffering", [S#state.partition, Ballot, Id]),
                    %% buffer the decision and reserve our commit spot
                    %% until we receive a matching ACCEPT from the leader
                    ok = grb_paxos_state:reserve_decision(Id, ok, Ts, SynodState0),
                    {ok, S#state{decision_buffer=Buffer#{{Id, Ballot} => Ts}}}
            end
    end.

-spec decide_internal(ballot(), term(), red_vote(), vclock(), #state{}) -> {ok, #state{}} | {not_ready, non_neg_integer()}.
decide_internal(Ballot, TxId, Decision, CommitVC, S=#state{synod_role=Role,
                                                           synod_state=SynodState0,
                                                           decision_buffer=Buffer}) ->
    Now = grb_time:timestamp(),
    CommitTs = grb_vclock:get_time(?RED_REPLICA, CommitVC),
    if
        (Role =:= ?leader) and (Now < CommitTs) ->
            {not_ready, grb_time:diff_ms(Now, CommitTs)};

        true ->
            case grb_paxos_state:decision(Ballot, TxId, Decision, CommitVC, SynodState0) of
                {ok, SynodState} ->
                    {ok, S#state{synod_state=SynodState}};

                bad_ballot ->
                    %% fixme(borja, red): should this initiate a leader recovery, or ignore?
                    ?LOG_ERROR("~p: bad ballot ~b for ~p", [S#state.partition, Ballot, TxId]),
                    {ok, S};

                not_prepared ->
                    ok = grb_measurements:log_counter({?MODULE, out_of_order_decision}),
                    ?LOG_DEBUG("~p: DECIDE(~b, ~p) := not_prepared, buffering", [S#state.partition, Ballot, TxId]),
                    %% buffer the decision and reserve our commit spot
                    %% until we receive a matching ACCEPT from the leader
                    ok = grb_paxos_state:reserve_decision(TxId, Decision, CommitVC, SynodState0),
                    {ok, S#state{decision_buffer=Buffer#{{TxId, Ballot} => {Decision, CommitVC}}}}
            end
    end.

-spec maybe_buffer_abort(ballot(), term(), red_vote(), vclock(), #state{}) -> #state{}.
maybe_buffer_abort(_Ballot, _TxId, ok, _CommitVC, State) ->
    %% If this is a commit, we can wait until delivery
    State;
maybe_buffer_abort(Ballot, TxId, {abort, Reason}, CommitVC, State=#state{partition=Partition,
                                                                         abort_buffer_io=AbortBuffer,
                                                                         send_aborts_interval_ms=Ms}) ->

    FramedAbortMsg = grb_dc_messages:frame(grb_dc_messages:red_learn_abort(Ballot, TxId, Reason, CommitVC)),
    if
        Ms > 0 ->
            %% Abort delay is active.
            State#state{abort_buffer_io=[AbortBuffer, FramedAbortMsg]};
        true ->
            %% Abort delay is disabled, send immediately.
            send_abort_buffer(Partition, FramedAbortMsg)
    end.

-spec send_abort_buffer(partition_id(), iodata()) -> ok.
send_abort_buffer(_Partition, []) ->
    ok;
send_abort_buffer(Partition, IOAborts) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_raw_framed(ReplicaId, Partition, IOAborts)
    end, grb_dc_connection_manager:connected_replicas()).

%% @doc Deliver all available updates with commit timestamp higher than `From`.
%%
%%      Returns the commit timestamp of the last transaction or heartbeat to be delivered, along
%%      with a boolean indicating if this function delivered any transaction.
%%
%%      Callers can use the information about delivered transactions to schedule a heartbeat, if
%%      needed.
%%
-spec deliver_updates(partition_id(), ballot(), grb_time:ts(), grb_paxos_state:t()) -> {grb_time:ts(), boolean()}.
deliver_updates(Partition, Ballot, From, SynodState) ->
    deliver_updates(Partition, Ballot, From, SynodState, false).

-spec deliver_updates(partition_id(), ballot(), grb_time:ts(), grb_paxos_state:t(), boolean()) -> {grb_time:ts(), boolean()}.
deliver_updates(Partition, Ballot, From, SynodState, DeliveredTx0) ->
    case grb_paxos_state:get_next_ready(From, SynodState) of
        false ->
            {From, DeliveredTx0};

        {NextFrom, Entries} ->

            %% Let followers know that these transactions are ready to be delivered.
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_deliver(ReplicaId, Partition, Ballot, NextFrom, Entries)
            end, grb_dc_connection_manager:connected_replicas()),

            DeliveredTx = lists:foldl(
                fun
                    ({_TxId, Label, WriteSet, CommitVC}, _)
                        when is_map(WriteSet) andalso map_size(WriteSet) =/= 0->
                            ?LOG_DEBUG("~p DELIVER(~p, ~p, ~p)", [Partition, NextFrom, Label, WriteSet]),
                            ok = grb_oplog_vnode:handle_red_transaction(Partition, Label, WriteSet, CommitVC),
                            true;

                    (_, Acc) ->
                        Acc
                end,
                DeliveredTx0,
                Entries
            ),

            deliver_updates(Partition, Ballot, NextFrom, SynodState, DeliveredTx)
    end.

%% Strong heartbeats are expensive, since they are equivalent
%% to a read-only strong transaction that always commits. If
%% we're sending a heartbeat every X ms, that's (1000 / X)
%% transactions per second and per partition being performed.
%%
%% To avoid paying the price, we look if the process is currently
%% certifying or delivering transactions. If the system is not
%% doing anything, we schedule a heartbeat. If, on the other hand,
%% the process is preparing or delivering transactions, we know clients
%% are submitting transactions, and thus we know the system will advance
%% even if we don't send heartbeats.
%%
%% Since delivery is checked every 1ms, in an idle system the final
%% result is similar, performing a heartbeat every (1 + X) ms, until the
%% system starts processing client transactions. Heartbeats stop being
%% scheduled until the system finds itself not preparing / delivering
%% transactions for an amount of time.
-spec maybe_schedule_heartbeat(boolean(), #state{}) -> #state{}.
maybe_schedule_heartbeat(DeliveredAnyTx, S=#state{heartbeat_schedule_timer=Timer,
                                                  heartbeat_schedule_ms=DelayMs}) ->

    if
        (DelayMs > 0) andalso (not DeliveredAnyTx) andalso (Timer =:= undefined) ->
            %% Schedule a strong heartbeat `DelayMs` in the future if we're currently
            %% not delivering any transactions - this means the system is idle and no
            %% client is submitting strong transactions.

            %% If there's an already scheduled timer, we don't need to schedule a new
            %% one.
            TRef = grb_red_heartbeat:schedule_heartbeat(S#state.heartbeat_process, DelayMs),
            S#state{heartbeat_schedule_timer=TRef};
        true ->
           S
    end.

-spec cancel_schedule_heartbeat_timer(reference() | undefined) -> ok.
cancel_schedule_heartbeat_timer(Timer) when is_reference(Timer) ->
    ok = ?CANCEL_TIMER_FAST(Timer);
cancel_schedule_heartbeat_timer(_) ->
    ok.

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
