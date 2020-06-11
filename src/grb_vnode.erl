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
    State = #state{
        partition = Partition,
        prepared_blue = #{},
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
    ok = grb_partition_replica:start_replicas(P, N),
    Result = grb_partition_replica:replica_ready(P, N),
    {reply, Result, S};

handle_command(stop_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    ok = grb_partition_replica:stop_replicas(P, N),
    {reply, ok, S};

handle_command(replicas_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = grb_partition_replica:replica_ready(P, N),
    {reply, Result, S};

handle_command({prepare_blue, TxId, WS, Ts}, _From, S=#state{prepared_blue=PB}) ->
    {noreply, S#state{prepared_blue=PB#{TxId => {WS, Ts}}}};

handle_command({decide_blue, TxId, VC}, _From, State) ->
    ok = decide_blue_internal(TxId, VC, State),
    {noreply, State};

handle_command(Message, _Sender, State) ->
    ?LOG_WARNING("unhandled_command ~p", [Message]),
    {noreply, State}.

decide_blue_internal(TxId, VC, #state{op_log=OpLog,
                                      prepared_blue=PreparedBlue}) ->

    {{WS, _}, PreparedBlue1} = maps:take(TxId, PreparedBlue),
    %% todo(borja): Append version
    Objects = maps:fold(fun(Key, Value, Acc) ->
        Log = case ets:lookup(OpLog, Key) of
            [{Key, PrevLog}] -> PrevLog;
            [] -> []
        end,
        NewLog = [{VC, Value} | Log],
        [{Key, NewLog} | Acc]
    end, [], WS),
    true = ets:insert(OpLog, Objects),

    %% todo(borja): Move this to replication once we're done
    case maps:size(PreparedBlue1) of
        0 -> grb_replica_state:set_known_vc(grb_time:timestamp());
        _ ->
            %% Get the min of the elements in the map
            MinTimestamp = maps:fold(fun(_, {_, Ts}, Acc) ->
                case Acc of
                    ignore -> Ts;
                    _ -> erlang:min(Ts, Acc)
                end
            end, ignore, PreparedBlue1),
            grb_replica_state:set_known_vc(MinTimestamp - 1)
    end.


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
