-module(grb_writer_coordinator).
-behavior(gen_server).
-behavior(grb_vnode_worker).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/2]).

%% supervision tree
-export([start_link/2]).

%% protocol api
-export([decide_blue/3]).

%% vnode_worker callbacks
-export([persist_worker_num/2,
         start_worker/2,
         is_ready/2,
         terminate_worker/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(NUM_COORDS_KEY, num_coords).

-record(tx_entry, {
    type :: transaction_type(),
    ws :: writeset(),
    commit_vc :: vclock(),
    to_ack :: non_neg_integer(),
    acked :: non_neg_integer(),
    prepared_ts :: grb_time:ts()
}).

-record(state, {
    partition :: partition_id(),
    replica_id :: replica_id(),
    known_barrier_wait_ms :: non_neg_integer(),
    pending_decides = #{} :: #{term() => vclock()},
    accumulators = #{} :: #{term() => #tx_entry{}}
}).

-type state() :: #state{}.

-spec start_link(Partition :: partition_id(),
                 Id :: non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Partition, Id) ->
    gen_server:start_link(?MODULE, [Partition, Id], []).

-spec persist_worker_num(partition_id(), non_neg_integer()) -> ok.
persist_worker_num(Partition, N) ->
    persistent_term:put({?MODULE, Partition, ?NUM_COORDS_KEY}, N).

start_worker(Partition, Id) ->
    grb_writer_coordinator_sup:start_coordinator(Partition, Id).

is_ready(Partition, Id) ->
    gen_server:call(coord_pid(Partition, Id), ready).

terminate_worker(Partition, Id) ->
    gen_server:call(coord_pid(Partition, Id), shutdown).

-spec decide_blue(partition_id(), term(), vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    gen_server:cast(random_coord(Partition), {decide_blue, TxId, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    ok = persist_coord_pid(Partition, Id, self()),
    {ok, WaitMs} = application:get_env(grb, partition_ready_wait_ms),
    {ok, #state{partition=Partition,
                known_barrier_wait_ms=WaitMs,
                replica_id=grb_dc_manager:replica_id()}}.

handle_call(ready, _From, State) ->
    {reply, true, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [Request]),
    {noreply, State}.

handle_cast({decide_blue, TxId, VC}, S0=#state{replica_id=ReplicaId,
                                               pending_decides=Pending,
                                               known_barrier_wait_ms=WaitMs}) ->

    S = case grb_oplog_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId}),
            S0#state{pending_decides=Pending#{TxId => VC}};
        ready ->
            send_write_requests(blue, TxId, VC, S0)
    end,
    {noreply, S};

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [Request]),
    {noreply, State}.

-spec send_write_requests(transaction_type(), term(), vclock(), state()) -> state().
send_write_requests(TxType, TxId, VC, S=#state{partition=Partition, accumulators=WriteAcc}) ->
    Promise = grb_promise:new(self(), TxId),
    {ok, WS, PreparedAt} = grb_oplog_vnode:get_prepared_writeset(Partition, TxId),
    ToAck = lists:foldl(fun({Key, Value}, Acc) ->
        ok = grb_oplog_writer:async_append(Promise, Partition, TxType, Key, Value, VC),
        Acc + 1
    end, 0, maps:to_list(WS)),
    TxEntry = #tx_entry{type=TxType, ws=WS, commit_vc=VC, prepared_ts=PreparedAt, to_ack=ToAck, acked=0},
    S#state{accumulators=WriteAcc#{TxId => TxEntry}}.

handle_info({retry_decide, TxId}, S0=#state{replica_id=ReplicaId,
                                            pending_decides=Pending,
                                            known_barrier_wait_ms=WaitMs}) ->
    #{TxId := VC} = Pending,
    S = case grb_oplog_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId}),
            S0;
        ready ->
            S1 = send_write_requests(blue, TxId, VC, S0),
            S1#state{pending_decides=maps:remove(TxId, Pending)}
    end,
    {noreply, S};

handle_info({'$grb_promise_resolve', ok, TxId}, S0=#state{partition=Partition,
                                                          accumulators=WriteAcc}) ->
    S = case maps:get(TxId, WriteAcc, undefined) of
        undefined ->
            ?LOG_WARNING("missed write ack"),
            S0;
        TxState ->
            S0#state{accumulators=handle_write_ack(Partition, TxId, TxState, WriteAcc)}
    end,
    {noreply, S};

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled msg ~p", [Info]),
    {noreply, State}.

-spec handle_write_ack(partition_id(), term(), #tx_entry{}, #{term() := #tx_entry{}}) -> #{term() := #tx_entry{}}.
handle_write_ack(_Partition, TxId, TxData, WriteAcc) ->
    #tx_entry{to_ack=ToAck, acked=Acked, prepared_ts=_PreparedTs} = TxData,
    case (Acked + 1) of
        ToAck ->
            %% Fixme(borja): Finish this, append to blue commit log (convert to ETS)
            %% grb_oplog_vnode:remove_from_prepared(Partition, TxId, PreparedTs),
            maps:remove(TxId, WriteAcc);
        N ->
            WriteAcc#{TxId := TxData#tx_entry{acked=N}}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Naming
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec coord_pid(partition_id(), non_neg_integer()) -> pid().
coord_pid(Partition, Id) ->
    persistent_term:get({?MODULE, Partition, Id}).

-spec persist_coord_pid(partition_id(), non_neg_integer(), pid()) -> ok.
persist_coord_pid(Partition, Id, Pid) ->
    persistent_term:put({?MODULE, Partition, Id}, Pid).

-spec random_coord(partition_id()) -> pid().
random_coord(Partition) ->
    coord_pid(Partition, rand:uniform(num_coords(Partition))).

-spec num_coords(partition_id()) -> non_neg_integer().
num_coords(Partition) ->
    persistent_term:get({?MODULE, Partition, ?NUM_COORDS_KEY}, ?OPLOG_COORDS_NUM).
