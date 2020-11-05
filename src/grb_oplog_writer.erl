-module(grb_oplog_writer).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ignore_xref([start_link/2]).

%% supervision tree
-export([start_link/2]).

%% protocol api
-export([async_append/6]).

%% replica management API
-export([start_writers/2,
         stop_writers/1,
         writers_ready/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(NUM_WRITERS_KEY, num_writers).

-record(state, {
    partition :: partition_id(),
    active_replicas :: [replica_id()],
    op_log_reference :: cache_id(),
    op_log_size :: non_neg_integer(),
    last_vc_reference :: cache_id()
}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Writer management API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(Partition :: partition_id(),
                 Id :: non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.

start_link(Partition, Id) ->
    gen_server:start_link(?MODULE, [Partition, Id], []).

%% @doc Start `Count` readers for the given partition
-spec start_writers(partition_id(), non_neg_integer()) -> ok.
start_writers(Partition, Count) ->
    ok = persist_num_writers(Partition, Count),
    start_writers_internal(Partition, Count).

%% @doc Check if all writers at this node and partition are ready
-spec writers_ready(partition_id(), non_neg_integer()) -> boolean().
writers_ready(Partition, N) ->
    reader_ready_internal(Partition, N).

%% @doc Stop writers for the given partition
-spec stop_writers(partition_id()) -> ok.
stop_writers(Partition) ->
    stop_writers_internal(Partition, num_writers(Partition)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec async_append(grb_promise:t(), partition_id(), transaction_type(), key(), val(), vclock()) -> ok.
async_append(Promise, Partition, TxType, Key, Value, CommitVC) ->
    gen_server:cast(writer_for_key(Partition, Key), {append, Promise, TxType, Key, Value, CommitVC}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    ok = persist_writer_pid(Partition, Id, self()),
    OpLog = grb_oplog_vnode:op_log_table(Partition),
    OpLogSize = grb_oplog_vnode:op_log_table_size(Partition),
    LastVCTable = grb_oplog_vnode:last_vc_table(Partition),
    AllReplicas = [?RED_REPLICA | grb_dc_manager:all_replicas()],
    {ok, #state{partition=Partition,
                active_replicas=AllReplicas,
                op_log_reference=OpLog,
                op_log_size=OpLogSize,
                last_vc_reference=LastVCTable}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING("Unhandled call ~p", [Request]),
    {noreply, State}.

handle_cast({append, Promise, TxType, Key, Value, CommitVC}, S=#state{active_replicas=AllReplicas,
                                                                            op_log_size=Size,
                                                                            op_log_reference=OpLog,
                                                                            last_vc_reference=LastVC}) ->
    %% The reason why we go through a separate process is that appending to a Key's oplog is not atomic,
    %% and as such, we could have an issue where an unsafe access happens, and we lose an update.
    %% By routing all updates for a key through the same process, we ensure sequential updates to its
    %% opLog entry.
    %%
    %% Since the opLog table is a set with 64 different locks, we have one process per lock in the table,
    %% so that (hopefully), we can do all updates at the same time. We don't use the same hash as ETS, since
    %% it uses an internal representation, but we should be okay by using phash2.
    ok = grb_oplog_vnode:append_key_update_with_table(TxType, Key, Value, CommitVC,
                                                      OpLog, Size, LastVC, AllReplicas),
    ok = grb_promise:resolve(ok, Promise),
    {noreply, S};

handle_cast(Request, State) ->
    ?LOG_WARNING("Unhandled cast ~p", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_WARNING("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Start / Ready / Stop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_writers_internal(partition_id(), non_neg_integer()) -> ok.
start_writers_internal(_Partition, 0) ->
    ok;

start_writers_internal(Partition, N) ->
    case grb_oplog_writer_sup:start_writer(Partition, N) of
        {ok, _} ->
            start_writers_internal(Partition, N - 1);
        {error, {already_started, _}} ->
            start_writers_internal(Partition, N - 1);
        _Other ->
            ?LOG_ERROR("Unable to start oplog reader for ~p, will skip", [Partition]),
            try
                ok = gen_server:call(writer_pid(Partition, N), shutdown)
            catch _:_ ->
                ok
            end,
            start_writers_internal(Partition, N - 1)
    end.

-spec reader_ready_internal(partition_id(), non_neg_integer()) -> boolean().
reader_ready_internal(_Partition, 0) ->
    true;
reader_ready_internal(Partition, N) ->
    try
        case gen_server:call(writer_pid(Partition, N), ready) of
            ready ->
                reader_ready_internal(Partition, N - 1);
            _ ->
                false
        end
    catch _:_ ->
        false
    end.

-spec stop_writers_internal(partition_id(), non_neg_integer()) -> ok.
stop_writers_internal(_Partition, 0) ->
    ok;

stop_writers_internal(Partition, N) ->
    try
        ok = gen_server:call(writer_pid(Partition, N), shutdown)
    catch _:_ ->
        ok
    end,
    stop_writers_internal(Partition, N - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Naming
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec writer_pid(partition_id(), non_neg_integer()) -> pid().
writer_pid(Partition, Id) ->
    persistent_term:get({?MODULE, Partition, Id}).

-spec persist_writer_pid(partition_id(), non_neg_integer(), pid()) -> ok.
persist_writer_pid(Partition, Id, Pid) ->
    persistent_term:put({?MODULE, Partition, Id}, Pid).

-spec writer_for_key(partition_id(), binary()) -> pid().
writer_for_key(Partition, Key) ->
    %% phash2/2 is [0..Range-1]
    writer_pid(Partition, (1 + erlang:phash2(Key, num_writers(Partition)))).

-spec num_writers(partition_id()) -> non_neg_integer().
num_writers(Partition) ->
    persistent_term:get({?MODULE, Partition, ?NUM_WRITERS_KEY}, ?OPLOG_WRITER_NUM).

-spec persist_num_writers(partition_id(), non_neg_integer()) -> ok.
persist_num_writers(Partition, N) ->
    persistent_term:put({?MODULE, Partition, ?NUM_WRITERS_KEY}, N).
