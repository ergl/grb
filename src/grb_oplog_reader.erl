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
-export([async_key_snapshot/6,
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

-record(state, {
    %% Partition that this server is replicating
    partition :: partition_id(),
    replica_id :: replica_id(),

    known_barrier_wait_ms :: non_neg_integer(),
    pending_decides = #{} :: #{term() => vclock()}
}).

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

-spec async_key_snapshot(grb_promise:t(), partition_id(), term(), key(), crdt(), vclock()) -> ok.
async_key_snapshot(Promise, Partition, TxId, Key, Type, VC) ->
    gen_server:cast(random_reader(Partition), {key_snapshot, Promise, TxId, Key, Type, VC}).

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

handle_cast({key_snapshot, Promise, TxId, Key, Type, VC}, State) ->
    ok = key_snapshot_wait(Promise, TxId, Key, Type, VC, State),
    {noreply, State};

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
            grb_oplog_vnode:decide_blue(Partition, TxId, VC),
            S0
    end,
    {noreply, S};

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [Request]),
    {noreply, State}.

handle_info({retry_key_snapshot_wait, Promise, TxId, Key, Type, VC}, State) ->
    ok = key_snapshot_wait(Promise, TxId, Key, Type, VC, State),
    {noreply, State};

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
            grb_oplog_vnode:decide_blue(Partition, TxId, VC),
            S0#state{pending_decides=maps:remove(TxId, Pending)}
    end,
    {noreply, S};

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec key_snapshot_wait(Promise :: grb_promise:t(),
                        TxId :: term(),
                        Key :: key(),
                        Type :: crdt(),
                        SnapshotVC :: vclock(),
                        State :: #state{}) -> ok.

key_snapshot_wait(Promise, TxId, Key, Type, SnapshotVC, #state{partition=Partition,
                                                               replica_id=ReplicaId,
                                                               known_barrier_wait_ms=WaitMs}) ->

    case grb_propagation_vnode:partition_ready(Partition, ReplicaId, SnapshotVC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_key_snapshot_wait, Promise, TxId, Key, Type, SnapshotVC}),
            ok;
        ready ->
            grb_promise:resolve(grb_oplog_vnode:get_key_snapshot(Partition, TxId, Key, Type, SnapshotVC),
                                Promise)
    end.

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

-spec num_readers(partition_id()) -> non_neg_integer().
num_readers(Partition) ->
    persistent_term:get({?MODULE, Partition, ?NUM_READERS_KEY}, ?OPLOG_READER_NUM).

-spec persist_num_readers(partition_id(), non_neg_integer()) -> ok.
persist_num_readers(Partition, N) ->
    persistent_term:put({?MODULE, Partition, ?NUM_READERS_KEY}, N).
