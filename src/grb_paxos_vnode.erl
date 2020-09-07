-module(grb_paxos_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% api
-export([init_leader_state/0,
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
-define(DECISION_RETRY, 5).

%% todo(borja, red): timer to process decidedRed and deliver updates + indexes
-record(state, {
    partition :: partition_id(),
    replica_id = undefined :: replica_id() | undefined,

    synod_state :: grb_paxos_state:t(),
    heartbeat_process = undefined :: pid() | undefined
}).

-spec init_leader_state() -> ok.
init_leader_state() ->
    Res = grb_dc_utils:bcast_vnode_sync(?master, init_leader),
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
    State = #state{partition=Partition,
                   %% don't care, we will overwrite it
                   synod_state=grb_paxos_state:follower()},
    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    {reply, true, State};

handle_command(init_leader, _Sender, S=#state{partition=P, heartbeat_process=undefined}) ->
    ReplicaId = grb_dc_manager:replica_id(),
    {ok, Pid} = grb_red_timer:start(ReplicaId, P),
    {reply, ok, S#state{replica_id=ReplicaId,
                        heartbeat_process=Pid,
                        synod_state=grb_paxos_state:leader()}};

handle_command(init_leader, _Sender, S=#state{heartbeat_process=_Pid}) ->
    {reply, ok, S};

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

handle_command({decide_hb, Ballot}, _Sender, S=#state{synod_state=SynodState}) ->
    ok = decide_hb_internal(Ballot, SynodState),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_info({retry_decide_hb, Ballot}, S=#state{synod_state=SynodState}) ->
    ok = decide_hb_internal(Ballot, SynodState),
    {ok, S};

handle_info(Msg, State) ->
    ?LOG_WARNING("unhandled_info ~p", [Msg]),
    {ok, State}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec decide_hb_internal(ballot(), grb_paxos_state:t()) -> ok.
decide_hb_internal(Ballot, SynodState) ->
    case grb_paxos_state:decision_hb_pre(Ballot, SynodState) of
        ok ->
            grb_paxos_state:decision_hb(SynodState);
        not_ready ->
            erlang:send_after(?DECISION_RETRY, self(), {retry_decide_hb, Ballot})
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
