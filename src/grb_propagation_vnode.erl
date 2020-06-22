-module(grb_propagation_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Public API
-export([propagate_transactions/2,
         append_blue_commit/5]).

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
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-define(master, grb_propagation_vnode_master).

-record(state, {
    partition :: partition_id(),
    %% fixme(borja): Only used for naive replication, use globalKnownMatrix when uniform
    last_sent = 0 :: grb_time:ts(),
    logs = #{} :: #{replica_id() => grb_blue_commit_log:t()}
}).

%%%===================================================================
%%% public api
%%%===================================================================

-spec propagate_transactions(partition_id(), grb_time:ts()) -> ok.
propagate_transactions(Partition, KnownTime) ->
    riak_core_vnode_master:command({Partition, node()}, {propagate_tx, KnownTime}, ?master).

-spec append_blue_commit(replica_id(), partition_id(), term(), #{}, vclock()) -> ok.
append_blue_commit(ReplicaId, Partition, TxId, WS, CommitVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {append_blue, ReplicaId, TxId, WS, CommitVC},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    State = #state{partition=Partition},
    {ok, State}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command({append_blue, ReplicaId, TxId, WS, CommitVC}, _Sender, S=#state{logs=Logs}) ->
    ReplicaLog = maps:get(ReplicaId, Logs, grb_blue_commit_log:new(ReplicaId)),
    {noreply, S#state{logs = Logs#{ReplicaId => grb_blue_commit_log:insert(TxId, WS, CommitVC, ReplicaLog)}}};

handle_command({propagate_tx, KnownTime}, _Sender, State) ->
    ok = propagate_internal(KnownTime, State),
    %% fixme(borja): Change once we add uniform replication
    %% last_send should change to globalKnownMatrix
    {noreply, State#state{last_sent=KnownTime}};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

%%%===================================================================
%%% internal functions
%%%===================================================================

%% fixme(borja): Change once we add uniform replication
-spec propagate_internal(grb_time:ts(), #state{}) -> ok.
propagate_internal(LocalKnownTime, #state{partition=P, last_sent=LastSent, logs=Logs}) ->
    LocalId = grb_dc_utils:replica_id(),
    LocalLog = maps:get(LocalId, Logs, grb_blue_commit_log:new(LocalId)),
    ToSend = grb_blue_commit_log:get_bigger(LastSent, LocalLog),
    case ToSend of
        [] ->
            grb_dc_connection_manager:broadcast_heartbeat(LocalId, P, LocalKnownTime);
        Entries ->
            %% Entries are already ordered according to local commit time at this replica
            lists:foreach(fun(Entry) ->
                grb_dc_connection_manager:broadcast_tx(LocalId, P, Entry)
            end, Entries)
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
