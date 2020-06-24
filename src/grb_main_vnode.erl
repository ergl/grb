-module(grb_main_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Public API
-export([cache_name/2,
         prepare_blue/4,
         handle_replicate/5]).

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

-define(master, grb_main_vnode_master).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n = ?READ_CONCURRENCY :: non_neg_integer(),

    prepared_blue :: #{any() => {#{}, vclock()}},

    propagate_interval :: non_neg_integer(),
    propagate_timer = undefined :: timer:tref() | undefined,

    %% todo(borja, crdt): change type of op_log when adding crdts
    op_log_size :: non_neg_integer(),
    op_log :: cache(key(), cache(key(), grb_version_log:t()))
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec prepare_blue(partition_id(), term(), #{}, vclock()) -> grb_time:ts().
prepare_blue(Partition, TxId, WriteSet, SnapshotVC) ->
    %% todo(borja, uniformity): Have to update uniform_vc, not stable_vc
    StableVC0 = grb_propagation_vnode:stable_vc(Partition),
    StableVC1 = grb_vclock:max_except(grb_dc_utils:replica_id(), StableVC0, SnapshotVC),
    ok = grb_propagation_vnode:update_stable_vc(Partition, StableVC1),
    Ts = grb_time:timestamp(),
    ok = riak_core_vnode_master:command({Partition, node()},
                                        {prepare_blue, TxId, WriteSet, Ts},
                                        ?master),
    Ts.

-spec handle_replicate(partition_id(), replica_id(), term(), #{}, vclock()) -> ok.
handle_replicate(Partition, SourceReplica, TxId, WS, VC) ->
    riak_core_vnode_master:command({Partition, node()},
                                   {replicate_tx, SourceReplica, TxId, WS, VC},
                                   ?master).


%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Interval = application:get_env(grb, propagate_interval, 5),
    KeyLogSize = application:get_env(grb, version_log_size, 50),
    State = #state{
        partition = Partition,
        prepared_blue = #{},
        propagate_interval = Interval,
        op_log_size = KeyLogSize,
        op_log = new_cache(Partition, ?OP_LOG_TABLE)
    },

    {ok, State}.

handle_command(ping, _Sender, State) ->
    {reply, {pong, node(), State#state.partition}, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.op_log]),
    {reply, Ready, State};

handle_command(start_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = case grb_partition_replica:replica_ready(P, N) of
        true -> true;
        false ->
            ok = grb_partition_replica:start_replicas(P, N),
            grb_partition_replica:replica_ready(P, N)
    end,
    {reply, Result, S};

handle_command(stop_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    ok = grb_partition_replica:stop_replicas(P, N),
    {reply, ok, S};

handle_command(replicas_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = grb_partition_replica:replica_ready(P, N),
    {reply, Result, S};

handle_command(start_propagate_timer, _From, S = #state{partition=P, propagate_interval=Int, propagate_timer=undefined}) ->
    Args = [{P, node()}, propagate_event, ?master],
    {ok, TRef} = timer:apply_interval(Int, riak_core_vnode_master, command, Args),
    {reply, ok, S#state{propagate_timer=TRef}};

handle_command(start_propagate_timer, _From, S = #state{propagate_timer=_TRef}) ->
    {reply, ok, S};

handle_command(propagate_event, _From, State=#state{partition=P, prepared_blue=PreparedBlue}) ->
    ok = propagate_internal(P, PreparedBlue),
    {noreply, State};

handle_command(stop_propagate_timer, _From, S = #state{propagate_timer=undefined}) ->
    {reply, ok, S};

handle_command(stop_propagate_timer, _From, S = #state{propagate_timer=TRef}) ->
    {ok, cancel} = timer:cancel(TRef),
    {reply, ok, S#state{propagate_timer=undefined}};

handle_command({prepare_blue, TxId, WS, Ts}, _From, S=#state{prepared_blue=PB}) ->
    ?LOG_DEBUG("prepare_blue ~p wtih time ~p", [TxId, Ts]),
    {noreply, S#state{prepared_blue=PB#{TxId => {WS, Ts}}}};

handle_command({decide_blue, TxId, VC}, _From, State) ->
    NewState = decide_blue_internal(TxId, VC, State),
    {noreply, NewState};

handle_command({replicate_tx, SourceReplica, TxId, WS, VC}, _From, S=#state{partition=P,
                                                                            op_log=OpLog,
                                                                            op_log_size=LogSize}) ->
    CommitTime = grb_vclock:get_time(SourceReplica, VC),
    ok = update_partition_state(TxId, WS, VC, OpLog, LogSize),
    %% todo(borja, uniformity): Add to committedBlue[SourceReplica]
    ok = grb_propagation_vnode:handle_blue_heartbeat(P, SourceReplica, CommitTime),
    {noreply, S};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

-spec decide_blue_internal(term(), vclock(), #state{}) -> #state{}.
decide_blue_internal(TxId, VC, S=#state{partition=SelfPartition,
                                        op_log=OpLog,
                                        op_log_size=LogSize,
                                        prepared_blue=PreparedBlue}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    {{WS, _}, PreparedBlue1} = maps:take(TxId, PreparedBlue),
    ReplicaId = grb_dc_utils:replica_id(),
    ok = update_partition_state(TxId, WS, VC, OpLog, LogSize),
    ok = grb_propagation_vnode:append_blue_commit(ReplicaId, SelfPartition, TxId, WS, VC),
    S#state{prepared_blue=PreparedBlue1}.

-spec update_partition_state(TxId :: term(),
                             WS :: #{},
                             CommitVC :: vclock(),
                             OpLog :: cache(key(), grb_version_log:t()),
                             DefaultSize :: non_neg_integer()) -> ok.

update_partition_state(_TxId, WS, CommitVC, OpLog, DefaultSize) ->
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = case ets:lookup(OpLog, Key) of
            [{Key, PrevLog}] -> PrevLog;
            [] -> grb_version_log:new(DefaultSize)
        end,
        NewLog = grb_version_log:append({blue, Value, CommitVC}, Log),
        [{Key, NewLog} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),
    ok.

-spec propagate_internal(partition_id(), #{any() => {#{}, vclock()}}) -> ok.
propagate_internal(Partition, PreparedBlue) ->
    Ts = case map_size(PreparedBlue) of
        0 -> grb_time:timestamp();
        _ ->
            MinTS = maps:fold(fun
                (_, {_, Ts}, ignore) -> Ts;
                (_, {_, Ts}, Acc) -> erlang:min(Ts, Acc)
            end, ignore, PreparedBlue),
            ?LOG_DEBUG("knownVC[d] = min_prep (~p - 1)", [MinTS]),
            MinTS - 1
    end,
    grb_propagation_vnode:propagate_transactions(Partition, Ts).

%%%===================================================================
%%% Util Functions
%%%===================================================================

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

-spec new_cache(partition_id(), atom()) -> cache_id().
new_cache(Partition, Name) ->
    new_cache(Partition, Name, [set, protected, named_table, {read_concurrency, true}]).

new_cache(Partition, Name, Options) ->
    CacheName = cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            ?LOG_INFO("Unsable to create cache ~p at ~p, retrying", [Name, Partition]),
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

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.
