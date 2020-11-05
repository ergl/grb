-module(grb_oplog_reader).
-behavior(gen_server).
-behavior(grb_vnode_worker).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/2]).

%% supervision tree
-export([start_link/2]).

%% protocol api
-export([async_key_vsn/4,
         decide_blue/3]).

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

-define(NUM_READERS_KEY, num_replicas).

-record(state, {
    %% Partition that this server is replicating
    partition :: partition_id(),
    replica_id :: replica_id(),

    known_barrier_wait_ms :: non_neg_integer(),
    pending_decides = #{} :: #{term() => vclock()},

    op_log_reference :: atom()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Reader Management API
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

-spec persist_worker_num(partition_id(), non_neg_integer()) -> ok.
persist_worker_num(Partition, N) ->
    persistent_term:put({?MODULE, Partition, ?NUM_READERS_KEY}, N).

start_worker(Partition, Id) ->
    grb_oplog_reader_sup:start_reader(Partition, Id).

is_ready(Partition, Id) ->
    gen_server:call(reader_pid(Partition, Id), ready).

terminate_worker(Partition, Id) ->
    gen_server:call(reader_pid(Partition, Id), shutdown).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec async_key_vsn(grb_promise:t(), partition_id(), key(), vclock()) -> ok.
async_key_vsn(Promise, Partition, Key, VC) ->
    gen_server:cast(random_reader(Partition), {key_vsn, Promise, Key, VC}).

-spec decide_blue(partition_id(), _, vclock()) -> ok.
decide_blue(Partition, TxId, VC) ->
    gen_server:cast(random_reader(Partition), {decide_blue, TxId, VC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    ok = persist_reader_pid(Partition, Id, self()),
    OpLog = grb_oplog_vnode:op_log_table(Partition),
    {ok, WaitMs} = application:get_env(grb, partition_ready_wait_ms),
    {ok, #state{partition=Partition,
                replica_id=grb_dc_manager:replica_id(),
                known_barrier_wait_ms=WaitMs,
                op_log_reference=OpLog}}.

handle_call(ready, _From, State) ->
    {reply, true, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [Request]),
    {noreply, State}.

handle_cast({key_vsn, Promise, Key, VC}, State) ->
    ok = key_vsn_wait(Promise, Key, VC, State),
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

handle_info({retry_key_vsn_wait, Promise, Key, VC}, State) ->
    ok = key_vsn_wait(Promise, Key, VC, State),
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

-spec key_vsn_wait(Promise :: grb_promise:t(),
                   Key :: key(),
                   SnapshotVC :: vclock(),
                   State :: #state{}) -> ok.

key_vsn_wait(Promise, Key, SnapshotVC, #state{partition=Partition,
                                              replica_id=ReplicaId,
                                              op_log_reference=OpLogReplica,
                                              known_barrier_wait_ms=WaitMs}) ->

    case grb_propagation_vnode:partition_ready(Partition, ReplicaId, SnapshotVC) of
        not_ready ->
            erlang:send_after(WaitMs, self(), {retry_key_vsn_wait, Promise, Key, SnapshotVC}),
            ok;
        ready ->
            grb_promise:resolve(grb_oplog_vnode:get_key_version_with_table(OpLogReplica, Key, SnapshotVC),
                                Promise)
    end.

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
