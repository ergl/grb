-module(grb_vnode).
-behaviour(riak_core_vnode).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Public API
-export([cache_name/2,
         start_replicas/0,
         stop_replicas/0,
         prepare_blue/4]).

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

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n = ?READ_CONCURRENCY :: non_neg_integer(),

    prepared_blue :: #{any() => {#{}, vclock()}},

    %% todo(borja): Maybe move this to other vnode when impl. replication
    propagate_interval :: non_neg_integer(),
    propagate_timer = undefined :: timer:tref() | undefined,

    %% todo(borja): for now
    op_log :: cache(key(), val())
}).

%%%===================================================================
%%% API
%%%===================================================================

start_replicas() ->
    R = grb_dc_utils:bcast_vnode_local_sync(grb_vnode_master, start_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, R).

stop_replicas() ->
    R = grb_dc_utils:bcast_vnode_local_sync(grb_vnode_master, stop_replicas),
    ok = lists:foreach(fun({_, ok}) -> ok end, R).

%% todo(borja): Update uniform_vc once replication is done
-spec prepare_blue(partition_id(), _, _, vclock()) -> grb_time:ts().
prepare_blue(Partition, TxId, WriteSet, _VC) ->
    Ts = grb_time:timestamp(),
    ok = riak_core_vnode_master:command({Partition, node()},
                                        {prepare_blue, TxId, WriteSet, Ts},
                                        grb_vnode_master),
    Ts.

%%%===================================================================
%%% api riak_core callbacks
%%%===================================================================

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    Interval = application:get_env(grb, propagate_interval, 5),
    State = #state{
        partition = Partition,
        prepared_blue = #{},
        propagate_interval = Interval,
        op_log = new_cache(Partition, ?OP_LOG_TABLE)
    },

    {ok, State}.

%% Sample command: respond to a ping
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
    Args = [{P, node()}, propagate_event, grb_vnode_master],
    {ok, TRef} = timer:apply_interval(Int, riak_core_vnode_master, command, Args),
    {reply, ok, S#state{propagate_timer=TRef}};

handle_command(start_propagate_timer, _From, S = #state{propagate_timer=_TRef}) ->
    {reply, ok, S};

handle_command(propagate_event, _From, State) ->
    ok = propagate_internal(State#state.prepared_blue),
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

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

-spec decide_blue_internal(term(), vclock(), #state{}) -> #state{}.
decide_blue_internal(TxId, VC, S=#state{op_log=OpLog,
                                        prepared_blue=PreparedBlue}) ->

    ?LOG_DEBUG("~p(~p, ~p)", [?FUNCTION_NAME, TxId, VC]),

    {{WS, _}, PreparedBlue1} = maps:take(TxId, PreparedBlue),
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = case ets:lookup(OpLog, Key) of
            [{Key, PrevLog}] -> PrevLog;
            [] -> grb_version_log:new()
        end,
        NewLog = grb_version_log:append({blue, Value, VC}, Log),
        [{Key, NewLog} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),

    S#state{prepared_blue=PreparedBlue1}.

%% todo(borja): revisit with replication
-spec propagate_internal(#{any() => {#{}, vclock()}}) -> ok.
propagate_internal(PreparedBlue) when map_size(PreparedBlue) =:= 0 ->
    Ts = grb_time:timestamp(),
    grb_replica_state:set_known_vc(Ts);

propagate_internal(PreparedBlue) ->
    MinTS = maps:fold(fun
        (_, {_, Ts}, ignore) -> Ts;
        (_, {_, Ts}, Acc) -> erlang:min(Ts, Acc)
    end, ignore, PreparedBlue),
    ?LOG_DEBUG("knownVC[d] = min_prep (~p - 1)", [MinTS]),
    grb_replica_state:set_known_vc(MinTS - 1).

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
            lager:info("Unsable to create cache ~p at ~p, retrying", [Name, Partition]),
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
