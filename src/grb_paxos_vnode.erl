-module(grb_paxos_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% api
-export([init_leader_state/0,
         init_follower_state/0,
         prepare_heartbeat/1,
         accept_heartbeat/4,
         broadcast_hb_decision/3,
         decide_heartbeat/2]).

-export([local_prepare/5,
         remote_prepare/6]).

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

-record(state, {
    partition :: partition_id(),
    replica_id = undefined :: replica_id() | undefined,

    last_delivered = 0 :: grb_time:ts(),

    deliver_timer = undefined :: reference() | undefined,
    deliver_interval :: non_neg_integer(),
    decision_retry_interval :: non_neg_integer(),

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

-spec prepare_heartbeat(partition_id()) -> {ok, ballot()}.
prepare_heartbeat(Partition) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        prepare_hb,
                                        ?master).

-spec accept_heartbeat(partition_id(), replica_id(), ballot(), grb_time:ts()) -> ok.
accept_heartbeat(Partition, SourceReplica, Ballot, Ts) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {accept_hb, SourceReplica, Ballot, Ts},
                                   ?master).

-spec broadcast_hb_decision(partition_id(), replica_id(), ballot()) -> ok.
broadcast_hb_decision(Partition, SourceReplica, Ballot) ->
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_red_decide_heartbeat(ReplicaId, SourceReplica, Partition, Ballot)
    end, grb_dc_connection_manager:connected_replicas()),
    decide_heartbeat(Partition, Ballot).

-spec decide_heartbeat(partition_id(), ballot()) -> ok.
decide_heartbeat(Partition, Ballot) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {decide_hb, Ballot},
                                   ?master).


-spec local_prepare(index_node(), term(), #{}, #{}, vclock()) -> ok.
local_prepare(IndexNode, TxId, Readset, Writeset, SnapshotVC) ->
    %% todo(borja, red)
    riak_core_vnode_master:command(IndexNode,
                                   {local_prepare, TxId, Readset, Writeset, SnapshotVC},
                                   ?master).

-spec remote_prepare(partition_id(), replica_id(), term(), #{}, #{}, vclock()) -> ok.
remote_prepare(Partition, SourceReplica, TxId, Readset, Writeset, SnapshotVC) ->
    %% todo(borja, red)
    riak_core_vnode_master:command({Partition, node()},
                                   {remote_prepare, SourceReplica, TxId, Readset, Writeset, SnapshotVC},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, RetryInterval} = application:get_env(grb, red_heartbeat_interval),
    {ok, DeliverInterval} = application:get_env(grb, red_delivery_interval),
    State = #state{partition=Partition,
                   deliver_interval=DeliverInterval,
                   decision_retry_interval=RetryInterval,
                   %% don't care, we will overwrite it
                   synod_state=undefined},
    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    {reply, true, State};

handle_command(init_leader, _Sender, S=#state{partition=P, heartbeat_process=undefined,
                                              deliver_interval=Int, synod_state=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Pid} = grb_red_timer:start(ReplicaId, P),
    {reply, ok, S#state{replica_id=ReplicaId,
                        heartbeat_process=Pid,
                        synod_state=grb_paxos_state:leader(),
                        deliver_timer=erlang:send_after(Int, self(), ?deliver)}};

handle_command(init_follower, _Sender, S=#state{deliver_interval=Int, synod_state=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    {reply, ok, S#state{replica_id=ReplicaId,
                        synod_state=grb_paxos_state:follower(),
                        deliver_timer=erlang:send_after(Int, self(), ?deliver)}};

handle_command(prepare_hb, _Sender, S=#state{replica_id=LocalId,
                                             partition=Partition,
                                             synod_state=LeaderState}) ->

    {Ballot, Ts} = grb_paxos_state:prepare_hb(LeaderState),
    lists:foreach(fun(ReplicaId) ->
        grb_dc_connection_manager:send_red_heartbeat(ReplicaId, LocalId, Partition, Ballot, Ts)
    end, grb_dc_connection_manager:connected_replicas()),
    {reply, {ok, Ballot}, S};

handle_command({accept_hb, SourceReplica, Ballot, Ts}, _Sender, S=#state{replica_id=LocalId,
                                                                         partition=Partition,
                                                                         synod_state=FollowerState}) ->
    ok = grb_paxos_state:accept_hb(Ballot, Ts, FollowerState),
    ok = grb_dc_connection_manager:send_red_heartbeat_ack(SourceReplica, LocalId, Partition, Ballot),
    {noreply, S};

handle_command({decide_hb, Ballot}, _Sender, S=#state{decision_retry_interval=Int, synod_state=SynodState}) ->
    ok = decide_hb_internal(Ballot, SynodState, Int),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_info({retry_decide_hb, Ballot}, S=#state{decision_retry_interval=Int, synod_state=SynodState}) ->
    ok = decide_hb_internal(Ballot, SynodState, Int),
    {ok, S};

handle_info(?deliver, S=#state{partition=Partition,
                               last_delivered=LastDelivered,
                               synod_state=SynodState,
                               deliver_timer=Timer,
                               deliver_interval=Interval}) ->
    erlang:cancel_timer(Timer),
    {ok, S#state{last_delivered=deliver_updates(Partition, LastDelivered, SynodState),
                 deliver_timer=erlang:send_after(Interval, self(), ?deliver)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("unhandled_info ~p", [Msg]),
    {ok, State}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec decide_hb_internal(ballot(), grb_paxos_state:t(), non_neg_integer()) -> ok.
decide_hb_internal(Ballot, SynodState, Time) ->
    %% todo(borja, red): This might return not_prepared at followers
    %% if the coordinator receives a quorum of ACCEPT_ACK before this follower
    %% receives an ACCEPT, it might be that we receive a DECISION before
    %% the decided transaction has been processes. What to do?
    %% if we set the quorum size to all replicas, this won't happen
    case grb_paxos_state:decision_hb(Ballot, SynodState) of
        not_ready ->
            erlang:send_after(Time, self(), {retry_decide_hb, Ballot});
        ok ->
            ok
    end.

-spec deliver_updates(partition_id(), grb_time:ts(), grb_paxos_state:t()) -> grb_time:ts().
deliver_updates(Partition, From, SynodState) ->
    case grb_paxos_state:get_next_ready(From, SynodState) of
        false ->
            From;

        {heartbeat, Ts} ->
            ok = grb_propagation_vnode:handle_red_heartbeat(Partition, Ts),
            deliver_updates(Partition, Ts, SynodState);

        {NextFrom, WriteSet, CommitVC} ->
            ok = grb_main_vnode:handle_red_transaction(Partition, WriteSet, NextFrom, CommitVC),
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
