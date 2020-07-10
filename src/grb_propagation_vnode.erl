-module(grb_propagation_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API
-export([known_vc/1,
         stable_vc/1,
         update_stable_vc/2,
         uniform_vc/1,
         update_uniform_vc/2,
         handle_blue_heartbeat/3,
         handle_clock_update/4,
         append_blue_commit/6]).

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

-define(master, grb_propagation_vnode_master).
-define(propagate_req, propagate_event).

-type global_known_matrix() :: #{{replica_id(), replica_id()} => grb_time:ts()}.
-type blue_commit_logs() :: #{replica_id() => grb_blue_commit_log:t()}.

-record(state, {
    partition :: partition_id(),
    local_replica :: replica_id(),

    logs = #{} :: blue_commit_logs(),
    global_known_matrix = #{} :: global_known_matrix(),

    propagate_interval :: non_neg_integer(),
    propagate_timer = undefined :: reference() | undefined,

    %% It doesn't make sense to append it if we're not connected to other clusters
    should_append_commit = true :: boolean(),
    clock_cache :: cache(atom(), vclock())
}).

%%%===================================================================
%%% public api
%%%===================================================================

-spec uniform_vc(partition_id()) -> vclock().
uniform_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), uniform_vc, 2).

-spec stable_vc(partition_id()) -> vclock().
stable_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), stable_vc, 2).

-spec update_stable_vc(partition_id(), vclock()) -> ok.
update_stable_vc(Partition, SVC) ->
    riak_core_vnode_master:command({Partition, node()}, {update_stable_vc, SVC}, ?master).

-spec update_uniform_vc(partition_id(), vclock()) -> ok.
update_uniform_vc(Partition, SVC) ->
    riak_core_vnode_master:command({Partition, node()}, {update_uniform_vc, SVC}, ?master).

-spec known_vc(partition_id()) -> vclock().
known_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?PARTITION_CLOCK_TABLE), known_vc, 2).

-spec handle_blue_heartbeat(partition_id(), replica_id(), grb_time:ts()) -> ok.
handle_blue_heartbeat(Partition, ReplicaId, Ts) ->
    riak_core_vnode_master:command({Partition, node()}, {blue_hb, ReplicaId, Ts}, ?master).

-spec handle_clock_update(partition_id(), replica_id(), vclock(), vclock()) -> ok.
handle_clock_update(Partition, FromReplicaId, KnownVC, StableVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {remote_clock_update, FromReplicaId, KnownVC, StableVC},
                                   ?master).

-spec append_blue_commit(replica_id(), partition_id(), grb_time:ts(), term(), #{}, vclock()) -> ok.
append_blue_commit(ReplicaId, Partition, KnownTime, TxId, WS, CommitVC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {append_blue, ReplicaId, KnownTime, TxId, WS, CommitVC},
                                   ?master).

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, PropagateInterval} = application:get_env(grb, propagate_interval),
    ClockTable = new_cache(Partition, ?PARTITION_CLOCK_TABLE),
    true = ets:insert(ClockTable, [{uniform_vc, grb_vclock:new()},
                                   {stable_vc, grb_vclock:new()},
                                   {known_vc, grb_vclock:new()}]),

    {ok, #state{partition=Partition,
                local_replica=grb_dc_utils:replica_id(), % ok to do this, we'll overwrite it after join
                propagate_interval=PropagateInterval,
                clock_cache=ClockTable}}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(enable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=true}};

handle_command(disable_blue_append, _Sender, S) ->
    {reply, ok, S#state{should_append_commit=false}};

handle_command(learn_dc_id, _Sender, S) ->
    %% called after joining ring, this is now the correct id
    {reply, ok, S#state{local_replica=grb_dc_utils:replica_id()}};

handle_command(start_propagate_timer, _From, S = #state{propagate_interval=Int, propagate_timer=undefined}) ->
    TRef = erlang:send_after(Int, self(), ?propagate_req),
    {reply, ok, S#state{propagate_timer=TRef}};

handle_command(start_propagate_timer, _From, S = #state{propagate_timer=_TRef}) ->
    {reply, ok, S};

handle_command(stop_propagate_timer, _From, S = #state{propagate_timer=undefined}) ->
    {reply, ok, S};

handle_command(stop_propagate_timer, _From, S = #state{propagate_timer=TRef}) ->
    erlang:cancel_timer(TRef),
    {reply, ok, S#state{propagate_timer=undefined}};

handle_command({update_stable_vc, SVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, stable_vc, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, SVC),
    true = ets:update_element(ClockTable, stable_vc, {2, NewSVC}),
    {noreply, S};

handle_command({update_uniform_vc, SVC}, _Sender, S=#state{clock_cache=ClockTable}) ->
    OldSVC = ets:lookup_element(ClockTable, uniform_vc, 2),
    %% Safe to update everywhere, caller has already ensured to not update the current replica
    NewSVC = grb_vclock:max(OldSVC, SVC),
    true = ets:update_element(ClockTable, uniform_vc, {2, NewSVC}),
    {noreply, S};

handle_command({blue_hb, FromReplica, Ts}, _Sender, S=#state{clock_cache=ClockTable}) ->
    ok = update_known_vc(FromReplica, Ts, ClockTable),
    {noreply, S};

handle_command({remote_clock_update, FromReplicaId, KnownVC, StableVC}, _Sender, S) ->
    ok = update_clocks_internal(FromReplicaId, KnownVC, StableVC),
    {noreply, S};

handle_command({append_blue, ReplicaId, KnownTime, _TxId, _WS, _CommitVC}, _Sender, S=#state{clock_cache=ClockTable,
                                                                                             should_append_commit=false}) ->
    ok = update_known_vc(ReplicaId, KnownTime, ClockTable),
    {noreply, S};

handle_command({append_blue, ReplicaId, KnownTime, TxId, WS, CommitVC}, _Sender, S=#state{logs=Logs,
                                                                                          clock_cache=ClockTable,
                                                                                          should_append_commit=true})->
    ReplicaLog = maps:get(ReplicaId, Logs, grb_blue_commit_log:new(ReplicaId)),
    ok = update_known_vc(ReplicaId, KnownTime, ClockTable),
    {noreply, S#state{logs = Logs#{ReplicaId => grb_blue_commit_log:insert(TxId, WS, CommitVC, ReplicaLog)}}};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_info(?propagate_req, State=#state{partition=P,
                                         local_replica=LocalId,
                                         clock_cache=ClockTable,
                                         propagate_timer=Timer,
                                         propagate_interval=Interval}) ->

    erlang:cancel_timer(Timer),
    KnownTime = grb_main_vnode:get_known_time(P),
    KnownVC = get_updated_known_vc(LocalId, KnownTime, ClockTable),
    StableVC = ets:lookup_element(ClockTable, stable_vc, 2),
    ok = compute_uniform_vc(LocalId, StableVC),
    NewMatrix = propagate_internal(KnownVC, StableVC, State),
    {ok, State#state{global_known_matrix=NewMatrix,
                     propagate_timer=erlang:send_after(Interval, self(), ?propagate_req)}};

handle_info(Msg, State) ->
    ?LOG_WARNING("unhandled_info ~p", [Msg]),
    {ok, State}.

%%%===================================================================
%%% internal functions
%%%===================================================================

-spec propagate_internal(vclock(), vclock(), #state{}) -> global_known_matrix().
propagate_internal(KnownVC, StableVC, #state{local_replica=LocalId,
                                             partition=Partition,
                                             logs=Logs,
                                             global_known_matrix=Matrix}) ->

    RemoteReplicas = grb_dc_connection_manager:connected_replicas(),
    AllReplicas = [LocalId | RemoteReplicas],
    lists:foldl(fun(TargetReplica, GlobalMatrix) ->
        %% piggy-back on this loop to send our clocks
        %% todo(borja, speed): piggy-back on a blue heartbeat inside propagate_to when ReplayReplica = LocalId?
        ok = grb_dc_connection_manager:send_clocks(TargetReplica, LocalId, Partition, KnownVC, StableVC),
        propagate_to(TargetReplica, AllReplicas, Partition, Logs, KnownVC, GlobalMatrix)
    end, Matrix, RemoteReplicas).

%% @doc Propagate transactions / heartbeats to the target replica.
%%
%%      This will iterate over all the other connected replicas and fetch the
%%      necessary transactions/replicas to relay to the target replica.
%%
%%      In effect, we re-send transactions from other replicas to the target,
%%      to ensure that even if a replica goes down, if we received an update
%%      from it, other replicas will see it.
-spec propagate_to(Target :: replica_id(),
                   AllReplicas :: [replica_id()],
                   LocalPartition :: partition_id(),
                   Logs :: blue_commit_logs(),
                   KnownVC :: vclock(),
                   Matrix :: global_known_matrix()) -> global_known_matrix().

propagate_to(_TargetReplica, [], _Partition, _Logs, _KnownVC, MatrixAcc) ->
    MatrixAcc;
propagate_to(TargetReplica, [TargetReplica | Rest], Partition, Logs, KnownVC, MatrixAcc) ->
    %% skip ourselves
    propagate_to(TargetReplica, Rest, Partition, Logs, KnownVC, MatrixAcc);
propagate_to(TargetReplica, [RelayReplica | Rest], Partition, Logs, KnownVC, MatrixAcc) ->
    %% tx <- { <_, _, VC> \in log[relay] | VC[relay] > globalMatrix[target, relay] }
    %% if tx =/= \emptyset
    %%     for all t \in tx (in t.VC[relay] order)
    %%         send REPLICATE(relay, t) to target
    %% else
    %%     send HEARTBEAT(relay, knownVC[relay]) to target
    %% globalMatrix[target, relay] <- knownVC[relay]
    RelayKnownTime = grb_vclock:get_time(RelayReplica, KnownVC),
    Log = maps:get(RelayReplica, Logs, grb_blue_commit_log:new(RelayReplica)),
    LastSent = maps:get({TargetReplica, RelayReplica}, MatrixAcc, 0),
    ToSend = grb_blue_commit_log:get_bigger(LastSent, Log),
    case ToSend of
        [] ->
            grb_dc_connection_manager:send_heartbeat(TargetReplica, RelayReplica, Partition, RelayKnownTime);
        Txs ->
            %% Entries are already ordered to commit time at the replica of the log
            lists:foreach(fun(Tx) ->
                grb_dc_connection_manager:send_tx(TargetReplica, RelayReplica, Partition, Tx)
            end, Txs)
    end,
    NewMatrix = MatrixAcc#{{TargetReplica, RelayReplica} => RelayKnownTime},
    propagate_to(TargetReplica, Rest, Partition, Logs, KnownVC, NewMatrix).

%% @doc Set knownVC[ReplicaId] <-max- Time
-spec update_known_vc(replica_id(), grb_time:ts(), cache(atom(), vclock())) -> ok.
update_known_vc(ReplicaId, Time, ClockTable) ->
    Old = ets:lookup_element(ClockTable, known_vc, 2),
    New = grb_vclock:set_max_time(ReplicaId, Time, Old),
    true = ets:update_element(ClockTable, known_vc, {2, New}),
    ok.

%% @doc Same as update_known_vc/3, but return resulting knownVC
-spec get_updated_known_vc(replica_id(), grb_time:ts(), cache(atom(), vclock())) -> vclock().
get_updated_known_vc(ReplicaId, Time, ClockTable) ->
    Old = ets:lookup_element(ClockTable, known_vc, 2),
    New = grb_vclock:set_max_time(ReplicaId, Time, Old),
    true = ets:update_element(ClockTable, known_vc, {2, New}),
    New.

%% todo(borja, uniformity)
update_clocks_internal(SourceReplica, _KnownVC, StableVC) ->
    ok = compute_uniform_vc(SourceReplica, StableVC),
    ok.

%% todo(borja, uniformity)
compute_uniform_vc(_SourceReplica, _StableVC) ->
    ok.

%%%===================================================================
%%% Util Functions
%%%===================================================================

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
    BiName = atom_to_binary(Name, latin1),
    BinPart = integer_to_binary(Partition),
    TableName = <<BiName/binary, <<"-">>/binary, BinPart/binary, <<"@">>/binary, BinNode/binary>>,
    safe_bin_to_atom(TableName).

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
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

terminate(_Reason, #state{clock_cache=ClockCache}) ->
    try ets:delete(ClockCache) catch _:_ -> ok end,
    ok.

delete(State=#state{clock_cache=ClockCache}) ->
    try ets:delete(ClockCache) catch _:_ -> ok end,
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.
