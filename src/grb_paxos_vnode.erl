-module(grb_paxos_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% init api
-export([init_leader_state/0,
         init_follower_state/0]).

%% heartbeat api
-export([prepare_heartbeat/2,
         accept_heartbeat/5,
         broadcast_hb_decision/5,
         decide_heartbeat/4]).

%% tx API
-export([prepare/6,
         accept/8,
         broadcast_decision/6,
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

-record(state, {
    partition :: partition_id(),
    replica_id = undefined :: replica_id() | undefined,

    last_delivered = 0 :: grb_time:ts(),

    deliver_timer = undefined :: reference() | undefined,
    deliver_interval :: non_neg_integer(),
    decision_retry_interval :: non_neg_integer(),

    prune_timer = undefined :: reference() | undefined,
    prune_interval :: non_neg_integer(),

    %% read replica of the last version cache by grb_main_vnode
    op_log_red_replica :: atom(),
    synod_state = undefined :: grb_paxos_state:t() | undefined,
    heartbeat_process = undefined :: pid() | undefined
}).

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

-spec broadcast_hb_decision(partition_id(), replica_id(), ballot(), term(), grb_time:ts()) -> ok.
broadcast_hb_decision(Partition, SourceReplica, Ballot, Id, Ts) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_red_decide_heartbeat(ReplicaId, SourceReplica,
                                                            Partition, Ballot, Id, Ts)
    end, grb_dc_connection_manager:connected_replicas()),
    decide_heartbeat(Partition, Ballot, Id, Ts).

-spec decide_heartbeat(partition_id(), ballot(), term(), grb_time:ts()) -> ok.
decide_heartbeat(Partition, Ballot, Id, Ts) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {decide_hb, Ballot, Id, Ts},
                                   ?master).

-spec prepare(IndexNode :: index_node(),
              TxId :: term(),
              Readset :: #{key() => grb_time:ts()},
              WriteSet :: #{key() => val()},
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
             RS :: #{},
             WS :: #{},
             Vote :: red_vote(),
             PrepareVC :: vclock(),
             Coord :: red_coord_location()) -> ok.

accept(Partition, Ballot, TxId, RS, WS, Vote, PrepareVC, Coord) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {accept, Ballot, TxId, RS, WS, Vote, PrepareVC},
                                   Coord,
                                   ?master).

-spec broadcast_decision(replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
broadcast_decision(LocalId, Partition, Ballot, TxId, Decision, CommitVC) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_red_decision(ReplicaId, LocalId, Partition, Ballot, TxId, Decision, CommitVC)
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
    {ok, RetryInterval} = application:get_env(grb, red_heartbeat_interval),
    {ok, DeliverInterval} = application:get_env(grb, red_delivery_interval),
    PruningInterval = application:get_env(grb, red_prune_interval, 0),
    %% don't care about setting bad values, we will overwrite it
    State = #state{partition=Partition,
                   deliver_interval=DeliverInterval,
                   decision_retry_interval=RetryInterval,
                   prune_interval=PruningInterval,
                   op_log_red_replica=grb_dc_utils:cache_name(Partition, ?OP_LOG_LAST_RED),
                   synod_state=undefined},
    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    {reply, true, State};

handle_command(init_leader, _Sender, S=#state{partition=Partition,
                                              heartbeat_process=undefined,
                                              synod_state=undefined}) ->

    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Pid} = grb_red_timer:start(ReplicaId, Partition),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     heartbeat_process=Pid,
                                     synod_state=grb_paxos_state:leader()})};

handle_command(init_follower, _Sender, S=#state{synod_state=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     synod_state=grb_paxos_state:follower()})};

%%%===================================================================
%%% leader protocol messages
%%%===================================================================

handle_command({prepare_hb, Id}, _Sender, S=#state{replica_id=LocalId,
                                                   partition=Partition,
                                                   synod_state=LeaderState}) ->

    Result = grb_paxos_state:prepare_hb(Id, LeaderState),
    case Result of
        {ok, Ballot, Timestamp} ->
            ?LOG_DEBUG("~p: HEARTBEAT_PREPARE(~b, ~p, ~b)", [Partition, Ballot, Id, Timestamp]),
            grb_red_timer:handle_accept_ack(Partition, Ballot, Id, Timestamp),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_heartbeat(ReplicaId, LocalId, Partition,
                                                             Ballot, Id, Timestamp)
            end, grb_dc_connection_manager:connected_replicas());

        {already_decided, _Decision, _Timestamp} ->
            ?LOG_ERROR("~p heartbeat already decided, reused identifier ~p", [Partition, Id]),
            %% todo(borja, red): This shouldn't happen, but should let red_timer know
            erlang:error(heartbeat_already_decided)
    end,
    {noreply, S};

handle_command({prepare, TxId, RS, WS, SnapshotVC},
               Coordinator, S=#state{replica_id=LocalId,
                                     partition=Partition,
                                     synod_state=LeaderState,
                                     op_log_red_replica=LastRed}) ->

    Result = grb_paxos_state:prepare(TxId, RS, WS, SnapshotVC, LastRed, LeaderState),
    ?LOG_DEBUG("~p: ~p prepared as ~p, reply to coordinator ~p", [Partition, TxId, Result, Coordinator]),
    case Result of
        {already_decided, Decision, CommitVC} ->
            %% skip replicas, this is enough to reply to the client
            reply_already_decided(Coordinator, LocalId, Partition, TxId, Decision, CommitVC);
        {Vote, Ballot, PrepareVC}=Prepare ->
            reply_accept_ack(Coordinator, LocalId, Partition, Ballot, TxId, Vote, PrepareVC),
            lists:foreach(fun(ReplicaId) ->
                grb_dc_connection_manager:send_red_accept(ReplicaId, Coordinator, Partition,
                                                          TxId, RS, WS, Prepare)
            end, grb_dc_connection_manager:connected_replicas())
    end,
    {noreply, S};

%%%===================================================================
%%% follower protocol messages
%%%===================================================================

handle_command({accept, Ballot, TxId, RS, WS, Vote, PrepareVC}, Coordinator, S) ->
    ?LOG_DEBUG("~p ACCEPT(~p, ~b), reply to coordinator ~p", [TxId, S#state.partition, Ballot, Coordinator]),
    ok = grb_paxos_state:accept(Ballot, TxId, RS, WS, Vote, PrepareVC, S#state.synod_state),
    reply_accept_ack(Coordinator, S#state.replica_id, S#state.partition, Ballot, TxId, Vote, PrepareVC),
    {noreply, S};

handle_command({accept_hb, SourceReplica, Ballot, Id, Ts},
                _Sender, S=#state{replica_id=LocalId, partition=Partition, synod_state=FollowerState}) ->

    ?LOG_DEBUG("~p: HEARTBEAT_ACCEPT(~b, ~p, ~b)", [Partition, Ballot, Id, Ts]),
    ok = grb_paxos_state:accept_hb(Ballot, Id, Ts, FollowerState),
    ok = grb_dc_connection_manager:send_red_heartbeat_ack(SourceReplica, LocalId,
                                                          Partition, Ballot, Id, Ts),
    {noreply, S};

%%%===================================================================
%%% leader / follower protocol messages
%%%===================================================================

handle_command({decide_hb, Ballot, Id, Ts}, _Sender, S=#state{partition=P,
                                                             decision_retry_interval=Int,
                                                             synod_state=SynodState}) ->

    ?LOG_DEBUG("~p: HEARTBEAT_DECIDE(~b, ~p, ~b)", [P, Ballot, Id, Ts]),
    ok = decide_hb_internal(P, Ballot, Id, Ts, SynodState, Int),
    {noreply, S};

handle_command({decision, Ballot, TxId, Decision, CommitVC}, _Sender, S=#state{partition=Partition,
                                                                               decision_retry_interval=Int,
                                                                               synod_state=SynodState}) ->

    ?LOG_DEBUG("~p DECIDE(~b, ~p)", [TxId, Ballot, Decision]),
    ok = decide_internal(Partition, Ballot, TxId, Decision, CommitVC, SynodState, Int),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("~p unhandled_command ~p", [?MODULE, Message]),
    {noreply, State}.

handle_info({retry_decide_hb, Ballot, Id, Ts}, S=#state{partition=P,
                                                       decision_retry_interval=Int,
                                                       synod_state=SynodState}) ->

    ok = decide_hb_internal(P, Ballot, Id, Ts, SynodState, Int),
    {ok, S};

handle_info({retry_decision, Ballot, TxId, Decision, CommitVC},
            S=#state{partition=Partition, decision_retry_interval=Int, synod_state=SynodState}) ->

    ok = decide_internal(Partition, Ballot, TxId, Decision, CommitVC, SynodState, Int),
    {ok, S};

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
    ok = grb_paxos_state:prune_decided_before(LastDelivered, SynodState),
    {ok, S#state{prune_timer=erlang:send_after(Interval, self(), ?prune)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("~p unhandled_info ~p", [?MODULE, Msg]),
    {ok, State}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec start_timers(#state{}) -> #state{}.
start_timers(S=#state{deliver_interval=Deliver, prune_interval=0}) ->
    S#state{
        prune_timer=undefined,
        deliver_timer=erlang:send_after(Deliver, self(), ?deliver)
    };
start_timers(S=#state{deliver_interval=Deliver, prune_interval=Prune}) ->
    S#state{
        prune_timer=erlang:send_after(Prune, self(), ?prune),
        deliver_timer=erlang:send_after(Deliver, self(), ?deliver)
    }.

-spec reply_accept_ack(red_coord_location(), replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
reply_accept_ack({coord, Replica, Node}, MyReplica, Partition, Ballot, TxId, Vote, PrepareVC) ->
    MyNode = node(),
    case {Replica, Node} of
        {MyReplica, MyNode} ->
            grb_red_coordinator:accept_ack(Partition, Ballot, TxId, Vote, PrepareVC);
        {MyReplica, OtherNode} ->
            erpc:call(OtherNode, grb_red_coordinator, accept_ack, [Partition, Ballot, TxId, Vote, PrepareVC]);
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
            erpc:call(OtherNode, grb_red_coordinator, already_decided, [TxId, Decision, CommitVC]);
        {OtherReplica, _} ->
            grb_dc_connection_manager:send_red_decided(OtherReplica, Node, Partition, TxId, Decision, CommitVC)
    end.

-spec decide_hb_internal(partition_id(), ballot(), term(), grb_time:ts(), grb_paxos_state:t(), non_neg_integer()) -> ok | error.
%% this is here due to grb_paxos_state:decision_hb/4, that dialyzer doesn't like
%% (it thinks it will never return not_ready because that is returned right after
%% an ets:select with a record)
-dialyzer({no_match, decide_hb_internal/6}).
decide_hb_internal(P, Ballot, Id, Ts, SynodState, Time) ->
    case grb_paxos_state:decision_hb(Ballot, Id, Ts, SynodState) of
        ok -> ok;
        not_ready ->
            erlang:send_after(Time, self(), {retry_decide_hb, Ballot, Id, Ts}),
            ok;
        bad_ballot ->
            ?LOG_ERROR("Bad heartbeat ballot ~b", [Ballot]),
            ok;
        not_prepared ->
            ?LOG_ERROR("~p ~p DECIDE_HEARTBEAT(~b) := not_prepared", [P, self(), Ballot]),
            %% todo(borja, red): This might return not_prepared at followers
            %% todo(borja, red): Buffer DECISION until we receive ACCEPT_ACK from leader
            %% if the coordinator receives a quorum of ACCEPT_ACK before this follower
            %% receives an ACCEPT, it might be that we receive a DECISION before
            %% the decided transaction has been processes. What to do?
            %% if we set the quorum size to all replicas, this won't happen
            error
    end.

-spec decide_internal(partition_id(), ballot(), term(), red_vote(), vclock(), grb_paxos_state:t(), non_neg_integer()) -> ok | error.
decide_internal(Partition, Ballot, TxId, Decision, CommitVC, State, Time) ->
    case grb_paxos_state:decision(Ballot, TxId, Decision, CommitVC, State) of
        ok -> ok;
        not_ready ->
            erlang:send_after(Time, self(), {retry_decision, Ballot, TxId, Decision, CommitVC}),
            ok;
        bad_ballot ->
            ?LOG_ERROR("~p: bad ballot ~b for ~p", [Partition, Ballot, TxId]),
            ok;
        not_prepared ->
            ?LOG_ERROR("~p: DECIDE(~b, ~p) := not_prepared", [Partition, Ballot, TxId]),
            %% todo(borja, red): This might return not_prepared at followers
            %% todo(borja, red): Buffer DECISION until we receive ACCEPT_ACK from leader
            %% if the coordinator receives a quorum of ACCEPT_ACK before this follower
            %% receives an ACCEPT, it might be that we receive a DECISION before
            %% the decided transaction has been processes. What to do?
            %% if we set the quorum size to all replicas, this won't happen
            error
    end.

-spec deliver_updates(partition_id(), grb_time:ts(), grb_paxos_state:t()) -> grb_time:ts().
%% dialyzer doesn't like grb_paxos_state:get_next_ready/2 due to ets:select and records
-dialyzer({no_return, deliver_updates/3}).
deliver_updates(Partition, From, SynodState) ->
    case grb_paxos_state:get_next_ready(From, SynodState) of
        false ->
            From;
        {NextFrom, Entries} ->
            lists:foreach(fun
                (heartbeat) ->
                    ?LOG_DEBUG("~p DELIVER_HB(~b)", [Partition, NextFrom]),
                    ok = grb_propagation_vnode:handle_red_heartbeat(Partition, NextFrom);
                ({WriteSet, CommitVC}) ->
                    ?LOG_DEBUG("~p DELIVER(~p, ~p)", [Partition, NextFrom, WriteSet]),
                    ok = grb_main_vnode:handle_red_transaction(Partition, WriteSet, NextFrom, CommitVC)
            end, Entries),
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

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.
