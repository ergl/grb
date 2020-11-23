-module(grb_paxos_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% init api
-export([all_fetch_lastvc_table/0,
         init_leader_state/0,
         init_follower_state/0]).

%% heartbeat api
-export([prepare_heartbeat/2,
         accept_heartbeat/5,
         broadcast_hb_decision/4,
         decide_heartbeat/4]).

%% tx API
-export([prepare/6,
         accept/8,
         broadcast_decision/5,
         decide/5]).

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
-define(deliver, deliver_event).
-define(prune, prune_event).

-define(leader_queue_length_stat, {?MODULE, leader_message_queue}).
-define(follower_queue_length_stat, {?MODULE, follower_message_queue}).

-record(state, {
    partition :: partition_id(),
    replica_id = undefined :: replica_id() | undefined,
    %% only at leader
    heartbeat_process = undefined :: pid() | undefined,

    last_delivered = 0 :: grb_time:ts(),

    deliver_timer = undefined :: reference() | undefined,
    deliver_interval :: non_neg_integer(),
    %% How often do we re-check the clock at the leader to insert a decision
    %% We insert directly at the followers
    decision_retry_interval :: non_neg_integer(),

    prune_timer = undefined :: reference() | undefined,
    prune_interval :: non_neg_integer(),

    %% read replica of the last commit vc cache by grb_oplog_vnode
    op_log_last_vc_replica :: grb_oplog_vnode:last_vc() | undefined,
    synod_state = undefined :: grb_paxos_state:t() | undefined,

    %% a buffer of outstanding decision messages
    %% If we receive a DECIDE message from the coordinator before we receive
    %% an ACCEPT from the leader, we should buffer the DECIDE here, and apply it
    %% right after we receive an ACCEPT with a matching ballot and transaction id
    decision_buffer = #{} :: #{{term(), ballot()} => {red_vote(), vclock()} | grb_time:ts()}
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

-spec broadcast_hb_decision(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
broadcast_hb_decision(Partition, Ballot, Id, Ts) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_red_decide_heartbeat(ReplicaId, Partition, Ballot, Id, Ts)
    end, grb_dc_connection_manager:connected_replicas()),
    decide_heartbeat(Partition, Ballot, Id, Ts).

-spec decide_heartbeat(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
decide_heartbeat(Partition, Ballot, Id, Ts) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {decide_hb, Ballot, Id, Ts},
                                   ?master).

-spec prepare(IndexNode :: index_node(),
              TxId :: term(),
              Readset :: readset(),
              WriteSet :: writeset(),
              SnapshotVC :: vclock(),
              Coord :: red_coord_location()) -> ok.

prepare(IndexNode, TxId, ReadSet, Writeset, SnapshotVC, Coord) ->
    riak_core_vnode_master:command(IndexNode,
                                   {prepare, TxId, ReadSet, Writeset, SnapshotVC},
                                   Coord,
                                   ?master).

-spec accept(Partition :: partition_id(),
             Ballot :: ballot(),
             TxId :: term(),
             RS :: readset(),
             WS :: writeset(),
             Vote :: red_vote(),
             PrepareVC :: vclock(),
             Coord :: red_coord_location()) -> ok.

accept(Partition, Ballot, TxId, RS, WS, Vote, PrepareVC, Coord) ->
    %% For blue transactions, any pending operations are removed during blue commit, since
    %% it is performed at the replica / partitions where the client originally performed the
    %% operations. For red commit, however, the client can start the red commit at any
    %% partition, so we aren't able to prune them. With this, we prune the operations
    %% for red transactions when we accept them
    ok = grb_oplog_vnode:clean_transaction_ops(Partition, TxId),
    riak_core_vnode_master:command({Partition, node()},
                                   {accept, Ballot, TxId, RS, WS, Vote, PrepareVC},
                                   Coord,
                                   ?master).

-spec broadcast_decision(partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
broadcast_decision(Partition, Ballot, TxId, Decision, CommitVC) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_red_decision(ReplicaId, Partition, Ballot, TxId, Decision, CommitVC)
    end, grb_dc_connection_manager:connected_replicas()),
    decide(Partition, Ballot, TxId, Decision, CommitVC).

-spec decide(partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
decide(Partition, Ballot, TxId, Decision, CommitVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {decision, Ballot, TxId, Decision, CommitVC},
                                   ?master).


%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, RetryInterval} = application:get_env(grb, red_leader_check_clock_interval),
    {ok, DeliverInterval} = application:get_env(grb, red_delivery_interval),
    PruningInterval = application:get_env(grb, red_prune_interval, 0),

    %% don't care about setting bad values, we will overwrite it
    State = #state{partition=Partition,
                   deliver_interval=DeliverInterval,
                   decision_retry_interval=RetryInterval,
                   prune_interval=PruningInterval,
                   op_log_last_vc_replica=undefined,
                   synod_state=undefined},
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

handle_command(fetch_lastvc_table, _Sender, S0=#state{partition=Partition}) ->
    {Result, S} = try
        Table = grb_oplog_vnode:last_vc_table(Partition),
        {ok, S0#state{op_log_last_vc_replica=Table}}
    catch _:_  ->
        {error, S0}
    end,
    {reply, Result, S};

handle_command(init_leader, _Sender, S=#state{partition=Partition, synod_state=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Pid} = grb_red_timer:start_timer(ReplicaId, Partition),
    ok = grb_measurements:create_stat(?leader_queue_length_stat),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     heartbeat_process=Pid,
                                     synod_state=grb_paxos_state:leader()})};

handle_command(init_follower, _Sender, S=#state{synod_state=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    ok = grb_measurements:create_stat(?follower_queue_length_stat),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     synod_state=grb_paxos_state:follower()})};

%%%===================================================================
%%% leader protocol messages
%%%===================================================================

handle_command({prepare_hb, Id}, _Sender, S=#state{partition=Partition,
                                                   synod_state=LeaderState0}) ->

    {Result, LeaderState} = grb_paxos_state:prepare_hb(Id, LeaderState0),
    case Result of
        {ok, Ballot, Timestamp} ->
            ?LOG_DEBUG("~p: HEARTBEAT_PREPARE(~b, ~p, ~b)", [Partition, Ballot, Id, Timestamp]),
            grb_red_timer:handle_accept_ack(Partition, Ballot, Id, Timestamp),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_heartbeat(ReplicaId, Partition,
                                                             Ballot, Id, Timestamp)
            end, grb_dc_connection_manager:connected_replicas());

        {already_decided, _Decision, _Timestamp} ->
            ?LOG_ERROR("~p heartbeat already decided, reused identifier ~p", [Partition, Id]),
            %% todo(borja, red): This shouldn't happen, but should let red_timer know
            erlang:error(heartbeat_already_decided)
    end,
    {noreply, S#state{synod_state=LeaderState}};

handle_command({prepare, TxId, RS, WS, SnapshotVC},
               Coordinator, S=#state{replica_id=LocalId,
                                     partition=Partition,
                                     synod_state=LeaderState0,
                                     op_log_last_vc_replica=LastRed}) ->

    ok = grb_measurements:log_queue_length(?leader_queue_length_stat),

    %% For blue transactions, any pending operations are removed during blue commit, since
    %% it is performed at the replica / partitions where the client originally performed the
    %% operations. For red commit, however, the client can start the red commit at any
    %% partition, so we aren't able to prune them. With this, we prune the operations
    %% for red transactions when we prepare them
    ok = grb_oplog_vnode:clean_transaction_ops(Partition, TxId),

    {Result, LeaderState} = grb_paxos_state:prepare(TxId, RS, WS, SnapshotVC, LastRed, LeaderState0),
    ?LOG_DEBUG("~p: ~p prepared as ~p, reply to coordinator ~p", [Partition, TxId, Result, Coordinator]),
    case Result of
        {already_decided, Decision, CommitVC} ->
            %% skip replicas, this is enough to reply to the client
            reply_already_decided(Coordinator, LocalId, Partition, TxId, Decision, CommitVC);
        {Vote, Ballot, PrepareVC} ->
            reply_accept_ack(Coordinator, LocalId, Partition, Ballot, TxId, Vote, PrepareVC),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_accept(ReplicaId, Coordinator, Partition,
                                                          Ballot, Vote, TxId, RS, WS, PrepareVC)
            end, grb_dc_connection_manager:connected_replicas())
    end,
    {noreply, S#state{synod_state=LeaderState}};

%%%===================================================================
%%% follower protocol messages
%%%===================================================================

handle_command({accept, Ballot, TxId, RS, WS, Vote, PrepareVC}, Coordinator, S0=#state{replica_id=LocalId,
                                                                                       partition=Partition,
                                                                                       synod_state=FollowerState0,
                                                                                       decision_buffer=DecisionBuffer0}) ->

    ok = grb_measurements:log_queue_length(?follower_queue_length_stat),

    ?LOG_DEBUG("~p: ACCEPT(~b, ~p, ~p), reply to coordinator ~p", [Partition, Ballot, TxId, Vote, Coordinator]),
    {ok, FollowerState} = grb_paxos_state:accept(Ballot, TxId, RS, WS, Vote, PrepareVC, FollowerState0),
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

%%%===================================================================
%%% leader / follower protocol messages
%%%===================================================================

handle_command({decide_hb, Ballot, Id, Ts}, _Sender, S0=#state{partition=P}) ->

    ?LOG_DEBUG("~p: HEARTBEAT_DECIDE(~b, ~p, ~b)", [P, Ballot, Id, Ts]),
    {ok, S} = decide_hb_internal(Ballot, Id, Ts, S0),
    {noreply, S};

handle_command({decision, Ballot, TxId, Decision, CommitVC}, _Sender, S0=#state{partition=P}) ->

    ?LOG_DEBUG("~p DECIDE(~b, ~p, ~p)", [P, Ballot, TxId, Decision]),
    {ok, S} = decide_internal(Ballot, TxId, Decision, CommitVC, S0),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

handle_info({retry_decide_hb, Ballot, Id, Ts}, S0) ->
    decide_hb_internal(Ballot, Id, Ts, S0);

handle_info({retry_decision, Ballot, TxId, Decision, CommitVC}, S0) ->
    decide_internal(Ballot, TxId, Decision, CommitVC, S0);

handle_info(?deliver, S=#state{partition=Partition,
                               last_delivered=LastDelivered,
                               synod_state=SynodState,
                               deliver_timer=Timer,
                               deliver_interval=Interval}) ->
    erlang:cancel_timer(Timer),
    {ok, S#state{last_delivered=deliver_updates(Partition, LastDelivered, SynodState),
                 deliver_timer=erlang:send_after(Interval, self(), ?deliver)}};

handle_info(?prune, S=#state{last_delivered=LastDelivered,
                             synod_state=SynodState,
                             prune_timer=Timer,
                             prune_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    ?LOG_DEBUG("~p PRUNE_BEFORE(~b)", [S#state.partition, LastDelivered]),
    %% todo(borja, red): Should compute MinLastDelivered
    %% To know the safe cut-off point, we should exchange LastDelivered with all replicas and find
    %% the minimum. Either replicas send a message to the leader, which aggregates the min and returns,
    %% or we build some tree.
    {ok, S#state{synod_state=grb_paxos_state:prune_decided_before(LastDelivered, SynodState),
                 prune_timer=erlang:send_after(Interval, self(), ?prune)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec start_timers(#state{}) -> #state{}.
start_timers(S=#state{deliver_interval=DeliverInt, prune_interval=PruneInt}) ->
    S#state{prune_timer=grb_dc_utils:maybe_send_after(PruneInt, ?prune),
            deliver_timer=grb_dc_utils:maybe_send_after(DeliverInt, ?deliver)}.

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

-spec decide_hb_internal(ballot(), term(), grb_time:ts(), #state{}) -> {ok, #state{}} | error.
%% this is here due to grb_paxos_state:decision_hb/4, that dialyzer doesn't like
%% because it passes an integer as a clock
-dialyzer({nowarn_function, decide_hb_internal/4}).
decide_hb_internal(Ballot, Id, Ts, S=#state{synod_state=SynodState0,
                                            decision_buffer=Buffer,
                                            decision_retry_interval=Time}) ->

    case grb_paxos_state:decision_hb(Ballot, Id, Ts, SynodState0) of
        {ok, SynodState} ->
            {ok, S#state{synod_state=SynodState}};

        not_ready ->
            erlang:send_after(Time, self(), {retry_decide_hb, Ballot, Id, Ts}),
            {ok, S};

        bad_ballot ->
            %% todo(borja, red): should this initiate a leader recovery, or ignore?
            ?LOG_ERROR("~p: bad heartbeat ballot ~b", [S#state.partition, Ballot]),
            {ok, S};

        not_prepared ->
            ?LOG_DEBUG("~p: DECIDE_HEARTBEAT(~b, ~p) := not_prepared, buffering", [S#state.partition, Ballot, Id]),
            %% buffer the decision and reserve our commit spot
            %% until we receive a matching ACCEPT from the leader
            ok = grb_paxos_state:reserve_decision(Id, ok, Ts, SynodState0),
            {ok, S#state{decision_buffer=Buffer#{{Id, Ballot} => Ts}}}
    end.

-spec decide_internal(ballot(), term(), red_vote(), vclock(), #state{}) -> {ok, #state{}} | error.
decide_internal(Ballot, TxId, Decision, CommitVC, S=#state{synod_state=SynodState0,
                                                           decision_buffer=Buffer,
                                                           decision_retry_interval=Time}) ->

    case grb_paxos_state:decision(Ballot, TxId, Decision, CommitVC, SynodState0) of
        {ok, SynodState} ->
            {ok, S#state{synod_state=SynodState}};

        not_ready ->
            erlang:send_after(Time, self(), {retry_decision, Ballot, TxId, Decision, CommitVC}),
            {ok, S};

        bad_ballot ->
            %% todo(borja, red): should this initiate a leader recovery, or ignore?
            ?LOG_ERROR("~p: bad ballot ~b for ~p", [S#state.partition, Ballot, TxId]),
            {ok, S};

        not_prepared ->
            ok = grb_measurements:log_counter({?MODULE, out_of_order_decision}),
            ?LOG_DEBUG("~p: DECIDE(~b, ~p) := not_prepared, buffering", [S#state.partition, Ballot, TxId]),
            %% buffer the decision and reserve our commit spot
            %% until we receive a matching ACCEPT from the leader
            ok = grb_paxos_state:reserve_decision(TxId, Decision, CommitVC, SynodState0),
            {ok, S#state{decision_buffer=Buffer#{{TxId, Ballot} => {Decision, CommitVC}}}}
    end.

-spec deliver_updates(partition_id(), grb_time:ts(), grb_paxos_state:t()) -> grb_time:ts().
deliver_updates(Partition, From, SynodState) ->
    case grb_paxos_state:get_next_ready(From, SynodState) of
        false ->
            From;
        {NextFrom, Entries} ->
            lists:foreach(fun
                ({WriteSet, CommitVC}) when is_map(WriteSet) andalso map_size(WriteSet) =/= 0->
                    ?LOG_DEBUG("~p DELIVER(~p, ~p)", [Partition, NextFrom, WriteSet]),
                    ok = grb_oplog_vnode:handle_red_transaction(Partition, WriteSet, CommitVC);
                (_) ->
                    ok
            end, Entries),
            ?LOG_DEBUG("~p DELIVER_HB(~b)", [Partition, NextFrom]),
            ok = grb_propagation_vnode:handle_red_heartbeat(Partition, NextFrom),
            deliver_updates(Partition, NextFrom, SynodState)
    end.

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
