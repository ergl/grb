-module(grb_writer_coordinator).
-behavior(gen_server).
-behavior(grb_vnode_worker).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/2]).

%% supervision tree
-export([start_link/2]).

-export([enable_append/2,
         disable_append/2]).

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
    tx_type :: transaction_type(),
    ws :: writeset(),
    commit_vc :: vclock(),
    to_ack :: non_neg_integer(),
    acked :: non_neg_integer()
}).

-record(local_blue_tx, {
    tx_id :: term(),
    prepared_ts :: grb_time:ts()
}).

-type tx_marker() :: #local_blue_tx{}.
-type tx_acc() :: #{tx_marker() := #tx_entry{}}.

-record(state, {
    partition :: partition_id(),
    replica_id :: replica_id(),
    known_barrier_wait_ms :: non_neg_integer(),
    pending_decides :: #{term() => vclock()},
    should_append_commits :: boolean(),
    accumulators :: tx_acc()
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

-spec enable_append(partition_id(), non_neg_integer()) -> ok.
enable_append(Partition, Count) ->
    enable_append_(Partition, Count).

enable_append_(_, 0) ->
    ok;
enable_append_(P, N) ->
    ok = gen_server:call(coord_pid(P, N), enable_append),
    enable_append_(P, N - 1).

-spec disable_append(partition_id(), non_neg_integer()) -> ok.
disable_append(Partition, Count) ->
    disable_append_(Partition, Count).

disable_append_(_, 0) ->
    ok;
disable_append_(P, N) ->
    ok = gen_server:call(coord_pid(P, N), disable_append),
    disable_append_(P, N - 1).

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
                replica_id=grb_dc_manager:replica_id(),
                known_barrier_wait_ms=WaitMs,
                pending_decides=#{},
                should_append_commits=true,
                accumulators=#{}}}.

handle_call(ready, _From, State) ->
    {reply, true, State};

handle_call(enable_append, _From, State) ->
    {reply, ok, State#state{should_append_commits=true}};

handle_call(disable_append, _From, State) ->
    {reply, ok, State#state{should_append_commits=false}};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [Request]),
    {noreply, State}.

handle_cast({decide_blue, TxId, VC}, S0=#state{partition=Partition,
                                               replica_id=ReplicaId,
                                               pending_decides=Pending,
                                               known_barrier_wait_ms=WaitMs}) ->

    S = case grb_oplog_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId}),
            S0#state{pending_decides=Pending#{TxId => VC}};
        ready ->
            {ok, WS, PreparedAt} = grb_oplog_vnode:get_transaction_writeset(Partition, TxId),
            send_write_requests(#local_blue_tx{tx_id=TxId, prepared_ts=PreparedAt}, blue, WS, VC, S0)
    end,
    {noreply, S};

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [Request]),
    {noreply, State}.

-spec send_write_requests(tx_marker(), transaction_type(), writeset(), vclock(), state()) -> state().
send_write_requests(TxMarker, TxType, WS, VC, S=#state{partition=Partition, accumulators=WriteAcc}) ->
    Promise = grb_promise:new(self(), TxMarker),
    ToAck = lists:foldl(fun({Key, Value}, Acc) ->
        ok = grb_oplog_writer:async_append(Promise, Partition, TxType, Key, Value, VC),
        Acc + 1
    end, 0, maps:to_list(WS)),
    TxEntry = #tx_entry{tx_type=TxType, ws=WS, commit_vc=VC, to_ack=ToAck, acked=0},
    S#state{accumulators=WriteAcc#{TxMarker => TxEntry}}.

handle_info({retry_decide, TxId}, S0=#state{partition=Partition,
                                            replica_id=ReplicaId,
                                            pending_decides=Pending,
                                            known_barrier_wait_ms=WaitMs}) ->
    #{TxId := VC} = Pending,
    S = case grb_oplog_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId}),
            S0;
        ready ->
            {ok, WS, PreparedAt} = grb_oplog_vnode:get_transaction_writeset(Partition, TxId),
            S1 = send_write_requests(#local_blue_tx{tx_id=TxId, prepared_ts=PreparedAt}, blue, WS, VC, S0),
            S1#state{pending_decides=maps:remove(TxId, Pending)}
    end,
    {noreply, S};

handle_info({'$grb_promise_resolve', ok, TxMarker}, S0=#state{partition=Partition,
                                                              accumulators=WriteAcc}) ->
    S = case maps:get(TxMarker, WriteAcc, undefined) of
        undefined ->
            ?LOG_WARNING("missed write ack"),
            S0;
        TxState ->
            handle_write_ack(Partition, TxMarker, TxState, S0)
    end,
    {noreply, S};

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled msg ~p", [Info]),
    {noreply, State}.

-spec handle_write_ack(partition_id(), term(), #tx_entry{}, state()) -> state().
handle_write_ack(Partition, Marker=#local_blue_tx{tx_id=TxId, prepared_ts=PreparedTs}, TxData, S) ->
    #state{partition=Partition, accumulators=WriteAcc, should_append_commits=ShouldAppend} = S,
    #tx_entry{to_ack=ToAck, acked=Acked, ws=WS, commit_vc=VC} = TxData,
    case (Acked + 1) of
        ToAck ->
            ok = grb_oplog_vnode:remove_prepared(Partition, TxId, PreparedTs),
            KnownTime = grb_oplog_vnode:compute_known_time(Partition),
            case ShouldAppend of
                true ->
                    grb_propagation_vnode:append_blue_commit(Partition, KnownTime, WS, VC);
                false ->
                    grb_propagation_vnode:handle_self_blue_heartbeat(Partition, KnownTime)
            end,
            maps:remove(TxId, WriteAcc);
        N ->
            S#state{accumulators=WriteAcc#{Marker => TxData#tx_entry{acked=N}}}
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
