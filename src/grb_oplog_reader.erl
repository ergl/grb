%% -------------------------------------------------------------------
%% This module allows multiple readers on the ETS tables of a grb_oplog_vnode
%% -------------------------------------------------------------------
-module(grb_oplog_reader).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/2]).

%% supervision tree
-export([start_link/2]).

%% protocol api
-export([empty_wait/4,
         async_key_snapshot/6,
         multikey_snapshot/5,
         multikey_snapshot_bypass/5,
         async_key_operation/7,
         decide_blue/3]).

%% replica management API
-export([start_readers/2,
         stop_readers/1,
         readers_ready/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(NUM_READERS_KEY, num_replicas).

-record(pending_reads, {
    promise :: grb_promise:t(),
    to_ack :: pos_integer(),
    accumulator :: #{key() := snapshot()}
}).
-type pending_reads() :: #pending_reads{}.

-record(waiting_reads, {
    promise :: grb_promise:t(),
    pending_snapshot_vc :: vclock(),
    pending_key_payload :: {reads, [{key(), crdt()}]} | {updates, [{key(), operation()}]}
}).
-type waiting_reads() :: #waiting_reads{}.

-record(state, {
    %% Partition that this server is replicating
    partition :: partition_id(),
    replica_id :: replica_id(),

    known_barrier_wait_ms :: non_neg_integer(),
    %% Note(borja): Only the transaction is used as identifier, which could cause issues if
    %% the same transaction issues two multi-key reads to the same partition.
    %% However, this shouldn't be a problem, because issuing two concurrent multi-key
    %% requests on the same partition should be extremely rare.
    pending_reads = #{} :: #{term() => pending_reads() | waiting_reads()},
    simple_waiting_reads = #{} ::
        #{ {grb_promise:t(), term()} => { vclock(), key(), crdt(), operation() }
                                      | { vclock(), key(), crdt() }
                                      | { vclock() } }
}).

-type state() :: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Reader management API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start a reader responsible for serving reads to this partion
%%
%%      To allow concurrency, multiple readers are started. The `Id`
%%      parameter helps to distinguish them.
%%
%%      Since they replicate ETS tables stored in vnodes, they have
%%      to be started in the same physical node.
%%
%%
%%      This function is called from the supervisor dynamically
%%
-spec start_link(Partition :: partition_id(),
                 Id :: non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.

start_link(Partition, Id) ->
    gen_server:start_link(?MODULE, [Partition, Id], []).

%% @doc Start `Count` readers for the given partition
-spec start_readers(partition_id(), non_neg_integer()) -> ok.
start_readers(Partition, Count) ->
    ok = persist_num_readers(Partition, Count),
    start_readers_internal(Partition, Count).

%% @doc Check if all readers at this node and partition are ready
-spec readers_ready(partition_id(), non_neg_integer()) -> boolean().
readers_ready(Partition, N) ->
    reader_ready_internal(Partition, N).

%% @doc Stop readers for the given partition
-spec stop_readers(partition_id()) -> ok.
stop_readers(Partition) ->
    stop_readers_internal(Partition, num_readers(Partition)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec empty_wait(grb_promise:t(), partition_id(), term(), vclock()) -> ok.
empty_wait(Promise, Partition, TxId, VC) ->
    gen_server:cast(random_reader(Partition), {empty_wait, Promise, TxId, VC}).

-spec async_key_snapshot(grb_promise:t(), partition_id(), term(), key(), crdt(), vclock()) -> ok.
async_key_snapshot(Promise, Partition, TxId, Key, Type, VC) ->
    gen_server:cast(random_reader(Partition), {key_snapshot, Promise, TxId, Key, Type, VC}).

-spec async_key_operation(grb_promise:t(), partition_id(), term(), key(), crdt(), operation(), vclock()) -> ok.
async_key_operation(Promise, Partition, TxId, Key, Type, ReadOp, VC) ->
    gen_server:cast(random_reader(Partition), {key_version, Promise, TxId, Key, Type, ReadOp, VC}).

-spec multikey_snapshot(Promise :: grb_promise:t(),
                        Partition :: partition_id(),
                        TxId :: term(),
                        VC :: vclock(),
                        KeyPayload :: {reads, [{key(), crdt()}]} | {updates, [{key(), operation()}]}) -> ok.

multikey_snapshot(Promise, Partition, TxId, VC, KeyPayload) ->
    gen_server:cast(random_reader(Partition), {multikey_snapshot, Promise, TxId, VC, KeyPayload}).

-spec multikey_snapshot_bypass(Promise :: grb_promise:t(),
                        Partition :: partition_id(),
                        TxId :: term(),
                        VC :: vclock(),
                        KeyPayload :: {reads, [{key(), crdt()}]} | {updates, [{key(), operation()}]}) -> ok.

multikey_snapshot_bypass(Promise, Partition, TxId, VC, KeyPayload) ->
    gen_server:cast(random_reader(Partition), {multikey_snapshot_bypass, Promise, TxId, VC, KeyPayload}).

-spec decide_blue(partition_id(), _, vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    gen_server:cast(random_reader(Partition), {decide_blue, TxId, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    ok = persist_reader_pid(Partition, Id, self()),
    {ok, WaitMs} = application:get_env(grb, partition_ready_wait_ms),
    {ok, #state{partition=Partition,
                replica_id=grb_dc_manager:replica_id(),
                known_barrier_wait_ms=WaitMs}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [Request]),
    {noreply, State}.

handle_cast({empty_wait, Promise, TxId, VC}, S0=#state{partition=Partition,
                                                       replica_id=ReplicaId,
                                                       known_barrier_wait_ms=WaitMs,
                                                       simple_waiting_reads=WaitingReads}) ->

    S = case grb_propagation_vnode:partition_ready(Partition, ReplicaId, VC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_partition_wait, Promise, TxId}),
            S0#state{simple_waiting_reads=WaitingReads#{{Promise, TxId} => { VC }}};

        ready ->
            grb_promise:resolve({ok, <<>>}, Promise),
            S0
    end,
    {noreply, S};

handle_cast({key_snapshot, Promise, TxId, Key, Type, VC}, S0=#state{partition=Partition,
                                                                    replica_id=ReplicaId,
                                                                    known_barrier_wait_ms=WaitMs,
                                                                    simple_waiting_reads=WaitingReads}) ->

    S = case grb_propagation_vnode:partition_ready(Partition, ReplicaId, VC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_partition_wait, Promise, TxId}),
            false = maps:is_key({Promise, TxId}, WaitingReads),
            S0#state{simple_waiting_reads=WaitingReads#{{Promise, TxId} => { VC, Key, Type }}};

        ready ->
            grb_promise:resolve(grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, VC),
                                Promise),
            S0
    end,
    {noreply, S};

handle_cast({key_version, Promise, TxId, Key, Type, ReadOp, VC}, S0=#state{partition=Partition,
                                                                           replica_id=ReplicaId,
                                                                           known_barrier_wait_ms=WaitMs,
                                                                           simple_waiting_reads=WaitingReads}) ->

    S = case grb_propagation_vnode:partition_ready(Partition, ReplicaId, VC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_partition_wait, Promise, TxId}),
            false = maps:is_key({Promise, TxId}, WaitingReads),
            S0#state{simple_waiting_reads=WaitingReads#{{Promise, TxId} => { VC, Key, Type, ReadOp }}};

        ready ->
            Version = grb_oplog_vnode:get_key_version(Partition, TxId, Key, Type, VC),
            grb_promise:resolve({ok, grb_crdt:apply_read_op(ReadOp, Version)}, Promise),
            S0
    end,
    {noreply, S};

handle_cast({key_snapshot_bypass, Promise, TxId, Key, Type, VC}, State=#state{partition=Partition}) ->
    {ok, Snapshot} = grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, VC),
    ok = grb_promise:resolve({ok, Key, Snapshot}, Promise),
    {noreply, State};

handle_cast({key_snapshot_bypass, Promise, TxId, Key, Type, Operation, VC}, State=#state{partition=Partition}) ->
    ok = grb_oplog_vnode:put_client_op(Partition, TxId, Key, Operation),
    {ok, Snapshot} = grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, VC),
    ok = grb_promise:resolve({ok, Key, Snapshot}, Promise),
    {noreply, State};

handle_cast({multikey_snapshot, Promise, TxId, VC, KeyPayload}, S0=#state{partition=Partition,
                                                                          replica_id=ReplicaId,
                                                                          pending_reads=PendingReads,
                                                                          known_barrier_wait_ms=WaitMs}) ->

    S = case grb_propagation_vnode:partition_ready(Partition, ReplicaId, VC) of
        ready ->
            send_multi_read(Promise, TxId, VC, KeyPayload, S0);

        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_multikey_snapshot, TxId}),
            PendingState = #waiting_reads{promise=Promise,
                                          pending_snapshot_vc=VC,
                                          pending_key_payload=KeyPayload},
            S0#state{pending_reads=PendingReads#{TxId => PendingState}}
    end,
    {noreply, S};

handle_cast({multikey_snapshot_bypass, Promise, TxId, VC, KeyPayload}, State) ->
    {noreply, send_multi_read(Promise, TxId, VC, KeyPayload, State)};

handle_cast({decide_blue, TxId, VC}, S=#state{partition=Partition,
                                              replica_id=ReplicaId,
                                              known_barrier_wait_ms=WaitMs}) ->

    case grb_oplog_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId, VC});
        ready ->
            grb_oplog_vnode:decide_blue(Partition, TxId, VC)
    end,
    {noreply, S};

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [Request]),
    {noreply, State}.

handle_info({retry_partition_wait, Promise, TxId}, S0=#state{partition=Partition,
                                                             replica_id=ReplicaId,
                                                             known_barrier_wait_ms=WaitMs,
                                                             simple_waiting_reads=WaitingReads}) ->

    Payload = maps:get({Promise, TxId}, WaitingReads),
    VC = element(1, Payload),

    S = case grb_propagation_vnode:partition_ready(Partition, ReplicaId, VC) of
            not_ready ->
                erlang:send_after(WaitMs, self(), {retry_partition_wait, Promise, TxId}),
                S0;

            ready ->
                Result = case Payload of
                    {VC, Key, Type, ReadOp} ->
                        Version = grb_oplog_vnode:get_key_version(Partition, TxId, Key, Type, VC),
                        grb_crdt:apply_read_op(ReadOp, Version);
                    {VC, Key, Type} ->
                        Version = grb_oplog_vnode:get_key_version(Partition, TxId, Key, Type, VC),
                        grb_crdt:value(Version);
                    _ ->
                        %% nothing to do on empty waits
                        <<>>
                end,
                grb_promise:resolve({ok, Result}, Promise),
                S0#state{simple_waiting_reads=maps:remove({Promise, TxId}, WaitingReads)}
        end,
    {noreply, S};

handle_info({retry_multikey_snapshot, TxId}, S0=#state{partition=Partition,
                                                       replica_id=ReplicaId,
                                                       pending_reads=PendingReads,
                                                       known_barrier_wait_ms=WaitMs}) ->

    #waiting_reads{promise=ClientPromise,
                   pending_snapshot_vc=VC,
                   pending_key_payload=KeyPayload} = maps:get(TxId, PendingReads),

    S = case grb_propagation_vnode:partition_ready(Partition, ReplicaId, VC) of
        ready ->
            send_multi_read(ClientPromise, TxId, VC, KeyPayload,
                          S0#state{pending_reads=maps:remove(TxId, PendingReads)});

        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_multikey_snapshot, TxId}),
            S0
    end,
    {noreply, S};

handle_info({'$grb_promise_resolve', {ok, Key, Snapshot}, TxId}, S0=#state{pending_reads=PendingReads}) ->
    PendingAcc = #pending_reads{accumulator=Acc0} = maps:get(TxId, PendingReads),
    Acc = Acc0#{Key => Snapshot},
    S = case PendingAcc of
        #pending_reads{to_ack=ToAck} when ToAck > 1 ->
            S0#state{pending_reads=PendingReads#{TxId := PendingAcc#pending_reads{to_ack=ToAck - 1,
                                                                                  accumulator=Acc}}};

        #pending_reads{promise=Promise} ->
            ok = grb_promise:resolve(Acc, Promise),
            S0#state{pending_reads=maps:remove(TxId, PendingReads)}
    end,
    {noreply, S};

handle_info({retry_decide, TxId, VC}, S=#state{partition=Partition,
                                               replica_id=ReplicaId,
                                               known_barrier_wait_ms=WaitMs}) ->

    case grb_oplog_vnode:decide_blue_ready(ReplicaId, VC) of
        not_ready ->
            %% todo(borja, efficiency): can we use hybrid clocks here?
            erlang:send_after(WaitMs, self(), {retry_decide, TxId, VC});
        ready ->
            grb_oplog_vnode:decide_blue(Partition, TxId, VC)
    end,
    {noreply, S};

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec send_multi_read(Promise :: grb_promise:t(),
                      TxId :: term(),
                      VC :: vclock(),
                      KeyPayload :: {reads, [{key(), crdt()}]} | {updates, [{key(), operation()}]},
                      State0 :: state()) -> State :: state().

send_multi_read(ClientPromise, TxId, VC, {reads, KeyTypes}, S=#state{partition=Partition,
                                                                     pending_reads=PendingReads}) ->
    SelfPromise = grb_promise:new(self(), TxId),
    ToAck = lists:foldl(fun({Key, Type}, Acc) ->
        ok = key_snapshot_bypass(SelfPromise, Partition, TxId, Key, Type, VC),
        Acc + 1
    end, 0, KeyTypes),
    PendingState = #pending_reads{promise=ClientPromise, to_ack=ToAck, accumulator = #{}},
    S#state{pending_reads=PendingReads#{TxId => PendingState}};

send_multi_read(ClientPromise, TxId, VC, {updates, KeyOps}, S=#state{partition=Partition,
                                                                     pending_reads=PendingReads}) ->
    SelfPromise = grb_promise:new(self(), TxId),
    ToAck = lists:foldl(fun({Key, Operation}, Acc) ->
        Type = grb_crdt:op_type(Operation),
        ok = key_snapshot_bypass(SelfPromise, Partition, TxId, Key, Type, Operation, VC),
        Acc + 1
    end, 0, KeyOps),
    PendingState = #pending_reads{promise=ClientPromise, to_ack=ToAck, accumulator = #{}},
    S#state{pending_reads=PendingReads#{TxId => PendingState}}.

-spec key_snapshot_bypass(grb_promise:t(), partition_id(), term(), key(), crdt(), vclock()) -> ok.
key_snapshot_bypass(Promise, Partition, TxId, Key, Type, VC) ->
    gen_server:cast(hashed_reader(Partition, Key), {key_snapshot_bypass, Promise, TxId, Key, Type, VC}).

-spec key_snapshot_bypass(grb_promise:t(), partition_id(), term(), key(), crdt(), operation(), vclock()) -> ok.
key_snapshot_bypass(Promise, Partition, TxId, Key, Type, Operation, VC) ->
    gen_server:cast(hashed_reader(Partition, Key), {key_snapshot_bypass, Promise, TxId, Key, Type, Operation, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Start / Ready / Stop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_readers_internal(partition_id(), non_neg_integer()) -> ok.
start_readers_internal(_Partition, 0) ->
    ok;

start_readers_internal(Partition, N) ->
    case grb_oplog_reader_sup:start_reader(Partition, N) of
        {ok, _} ->
            start_readers_internal(Partition, N - 1);
        {error, {already_started, _}} ->
            start_readers_internal(Partition, N - 1);
        _Other ->
            ?LOG_ERROR("Unable to start oplog reader for ~p, will skip", [Partition]),
            try
                ok = gen_server:call(reader_pid(Partition, N), shutdown)
            catch _:_ ->
                ok
            end,
            start_readers_internal(Partition, N - 1)
    end.

-spec reader_ready_internal(partition_id(), non_neg_integer()) -> boolean().
reader_ready_internal(_Partition, 0) ->
    true;
reader_ready_internal(Partition, N) ->
    try
        case gen_server:call(reader_pid(Partition, N), ready) of
            ready ->
                reader_ready_internal(Partition, N - 1);
            _ ->
                false
        end
    catch _:_ ->
        false
    end.

-spec stop_readers_internal(partition_id(), non_neg_integer()) -> ok.
stop_readers_internal(_Partition, 0) ->
    ok;

stop_readers_internal(Partition, N) ->
    try
        ok = gen_server:call(reader_pid(Partition, N), shutdown)
    catch _:_ ->
        ok
    end,
    stop_readers_internal(Partition, N - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Naming
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec reader_pid(partition_id(), non_neg_integer()) -> pid().
reader_pid(Partition, Id) ->
    persistent_term:get({?MODULE, Partition, Id}).

-spec persist_reader_pid(partition_id(), non_neg_integer(), pid()) -> ok.
persist_reader_pid(Partition, Id, Pid) ->
    persistent_term:put({?MODULE, Partition, Id}, Pid).

-spec random_reader(partition_id()) -> pid().
random_reader(Partition) ->
    reader_pid(Partition, rand:uniform(num_readers(Partition))).

-spec hashed_reader(partition_id(), key()) -> pid().
hashed_reader(Partition, Key) ->
    Pos = grb_dc_utils:convert_key(Key) rem num_readers(Partition) + 1,
    reader_pid(Partition, Pos).

-spec num_readers(partition_id()) -> non_neg_integer().
num_readers(Partition) ->
    persistent_term:get({?MODULE, Partition, ?NUM_READERS_KEY}, ?OPLOG_READER_NUM).

-spec persist_num_readers(partition_id(), non_neg_integer()) -> ok.
persist_num_readers(Partition, N) ->
    persistent_term:put({?MODULE, Partition, ?NUM_READERS_KEY}, N).
