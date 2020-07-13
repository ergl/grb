-module(grb_dc_connection_manager).
-behavior(gen_server).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REPLICAS_TABLE, connected_replicas).
-define(REPLICAS_TABLE_KEY, replicas).
-define(CONN_SOCKS_TABLE, connection_sockets).

%% External API
-export([connect_to/1,
         connected_replicas/0,
         send_heartbeat/4,
         send_tx/4,
         send_clocks/5,
         broadcast_tx/3,
         broadcast_heartbeat/3]).

%% Managemenet API
-export([connection_closed/2,
         close/1]).

%% Used through erpc or supervisor machinery
-ignore_xref([start_link/0,
              connect_to/1,
              close/1]).

%% Supervisor
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {
    replicas :: cache(replicas, ordsets:ordset(replica_id())),
    connection_sockets :: cache({partition_id(), replica_id()}, inet:socket()),
    %% for cleanup purposes, no need to expose with ETS to other nodes
    reverse_pid_index = #{} :: #{inet:socket() => pid()}
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DC API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec connect_to(replica_descriptor()) -> ok | {error, term()}.
connect_to(#replica_descriptor{replica_id=ReplicaID, remote_addresses=RemoteNodes}) ->
    ?LOG_DEBUG("Node ~p connected succesfully to DC ~p", [ReplicaID, RemoteNodes]),
    %% RemoteNodes is a map, with Partitions as keys
    %% The value is a tuple {IP, Port}, to allow addressing
    %% that specific partition at the replica
    %%
    %% Get our local partitions (to our nodes),
    %% and get those from the remote addresses
    try
        Result = lists:foldl(fun(Partition, {EntryMap, PartitionMap}) ->
            Entry={RemoteIP, RemotePort} = maps:get(Partition, RemoteNodes),
            ConnPid = case maps:is_key(Entry, EntryMap) of
                true ->
                    maps:get(Entry, EntryMap);
                false ->
                    case grb_dc_connection_sender_sup:start_connection(ReplicaID, RemoteIP, RemotePort) of
                        {ok, Pid} ->
                            Pid;
                        Err ->
                            throw({error, {sender_connection, ReplicaID, RemoteIP, Err}})
                    end
            end,
            {EntryMap#{Entry => ConnPid}, PartitionMap#{Partition => ConnPid}}
        end, {#{}, #{}}, grb_dc_utils:my_partitions()),
        {EntryMap, PartitionConnections} = Result,
        ?LOG_DEBUG("EntryMap: ~p", [EntryMap]),
        ?LOG_DEBUG("PartitionConnections: ~p", [PartitionConnections]),
        ok = add_replica_connections(ReplicaID, PartitionConnections),
        ok
    catch Exn -> Exn
    end.

%% @doc Mark a replica as lost, connection has been closed.
%%
%%      Although connections are partition-aware, we will remove
%%      the connections to all nodes at the remote replica, and
%%      mark it as down.
-spec connection_closed(replica_id(), inet:socket()) -> ok.
connection_closed(ReplicaId, Socket) ->
    gen_server:cast(?MODULE, {closed, ReplicaId, Socket}).

%% @doc Close the connection to (all nodes at) the remote replica
-spec close(replica_id()) -> ok.
close(ReplicaId) ->
    gen_server:call(?MODULE, {close, ReplicaId}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Node API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_replica_connections(replica_id(), #{partition_id() => pid()}) -> ok.
add_replica_connections(Id, PartitionConnections) ->
    gen_server:call(?MODULE, {add_replica_connections, Id, PartitionConnections}).

-spec connected_replicas() -> [replica_id()].
connected_replicas() ->
    ets:lookup_element(?REPLICAS_TABLE, ?REPLICAS_TABLE_KEY, 2).

-spec send_msg(replica_id(), partition_id(), any()) -> ok.
send_msg(Replica, Partition, Msg) ->
    Sock = ets:lookup_element(?CONN_SOCKS_TABLE, {Partition, Replica}, 2),
    ok = gen_tcp:send(Sock, Msg),
    ok.

-spec send_heartbeat(To :: replica_id(),
                     From :: replica_id(),
                     Partition :: partition_id(),
                     Time :: grb_time:ts()) -> ok.

send_heartbeat(ToId, FromId, Partition, Time) ->
    ?LOG_DEBUG("Sending blue_hearbeat to ~p:~p on behalf of ~p: ~p", [ToId, Partition, FromId, Time]),
    send_msg(ToId, Partition, heartbeat(FromId, Partition, Time)).

-spec send_tx(From :: replica_id(),
              To :: replica_id(),
              Partition :: partition_id(),
              Tx :: {term(), #{}, vclock()}) -> ok.

send_tx(ToId, FromId, Partition, Transaction) ->
    ?LOG_DEBUG("Sending transaction to ~p:~p on behalf of ~p", [ToId, Partition, FromId, Transaction]),
    send_msg(ToId, Partition, replicate_tx(FromId, Partition, Transaction)).

-spec send_clocks(From :: replica_id(),
                  To :: replica_id(),
                  Partition :: partition_id(),
                  KnownVC :: vclock(),
                  StableVC :: vclock()) -> ok.

send_clocks(ToId, FromId, Partition, KnownVC, StableVC) ->
    ?LOG_DEBUG("Sending clocks to ~p:~p", [ToId, Partition]),
    send_msg(ToId, Partition, clock_msg(FromId, Partition, KnownVC, StableVC)).

%% @doc Send a message to all replicas of the given partition
-spec broadcast_msg(partition_id(), any()) -> ok.
broadcast_msg(Partition, Msg) ->
    %% fixme(borja): This will throw if the underlying table has been deleted
    %% This happens during system termination if stop_bg_processes is not called
    %% before terminating the node
    Socks = ets:select(?CONN_SOCKS_TABLE, [{{{Partition, '_'}, '$1'}, [], ['$1']}]),
    lists:foreach(fun(S) -> gen_tcp:send(S, Msg) end, Socks),
    ok.

-spec broadcast_heartbeat(replica_id(), partition_id(), grb_time:ts()) -> ok.
broadcast_heartbeat(FromId, ToPartition, Time) ->
    broadcast_msg(ToPartition, heartbeat(FromId, ToPartition, Time)).

-spec broadcast_tx(replica_id(), partition_id(), {term(), #{}, vclock()}) -> ok.
broadcast_tx(FromId, ToPartition, Transaction) ->
    broadcast_msg(ToPartition, replicate_tx(FromId, ToPartition, Transaction)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    ReplicaTable = ets:new(?REPLICAS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:new()}),
    ConnPidTable = ets:new(?CONN_SOCKS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{replicas=ReplicaTable,
                connection_sockets=ConnPidTable}}.

handle_call({add_replica_connections, ReplicaId, PartitionConnections}, _From, State=#state{reverse_pid_index=Index0}) ->
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:add_element(ReplicaId, Replicas)}),
    {Objects, Index} = maps:fold(fun(Partition, Pid, {ObjectAcc, IndexAcc}) ->
        ConnSocket = grb_dc_connection_sender:get_socket(Pid),
        {
            [{{Partition, ReplicaId}, ConnSocket} | ObjectAcc],
            IndexAcc#{ConnSocket => Pid}
        }
    end, {[], Index0}, PartitionConnections),
    true = ets:insert(?CONN_SOCKS_TABLE, Objects),
    {reply, ok, State#state{reverse_pid_index=Index}};

handle_call({close, ReplicaId}, _From, State=#state{reverse_pid_index=Index}) ->
    ?LOG_INFO("Closing connections to ~p", [ReplicaId]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    Socks = ets:select(?CONN_SOCKS_TABLE, [{{{'_', ReplicaId}, '$1'}, [], ['$1']}]),
    _ = ets:select_delete(?CONN_SOCKS_TABLE, [{{{'_', ReplicaId}, '_'}, [], [true]}]),
    [begin
         ok = grb_dc_connection_sender:close(maps:get(S, Index))
    end || S <- Socks],
    {reply, ok, State#state{reverse_pid_index=maps:without(Socks, Index)}};

handle_call(E, _From, S) ->
    ?LOG_WARNING("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast({closed, ReplicaId, Socket}, State=#state{reverse_pid_index=Index}) ->
    ?LOG_INFO("Connection lost to ~p (~p), removing all references", [ReplicaId, Socket]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    Socks = ets:select(?CONN_SOCKS_TABLE, [{{{'_', ReplicaId}, '$1'}, [], ['$1']}]),
    _ = ets:select_delete(?CONN_SOCKS_TABLE, [{{{'_', ReplicaId}, '_'}, [], [true]}]),
    [case S of
        Socket -> ok;
        _ -> ok = grb_dc_connection_sender:close(maps:get(S, Index))
     end || S <- Socks],
    {noreply, State#state{reverse_pid_index=maps:without(Socks, Index)}};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec heartbeat(replica_id(), partition_id(), grb_time:ts()) -> binary().
heartbeat(FromId, ToPartition, Timestamp) ->
    dc_message(FromId, ToPartition, #blue_heartbeat{timestamp=Timestamp}).

-spec replicate_tx(replica_id(), partition_id(), {term(), #{}, vclock()}) -> binary().
replicate_tx(FromId, ToPartition, {TxId, WS, CommitVC}) ->
    dc_message(FromId, ToPartition, #replicate_tx{tx_id=TxId,
                                                  writeset=WS,
                                                  commit_vc=CommitVC}).

-spec clock_msg(replica_id(), partition_id(), vclock(), vclock()) -> binary().
clock_msg(FromId, ToPartition, KnownVC, StableVC) ->
    dc_message(FromId, ToPartition, #update_clocks{known_vc=KnownVC, stable_vc=StableVC}).

-spec dc_message(replica_id(), partition_id(), term()) -> binary().
dc_message(FromId, Partition, Payload) ->
    PBin = pad(?PARTITION_BYTES, binary:encode_unsigned(Partition)),
    Msg = #inter_dc_message{source_id=FromId, payload=Payload},
    <<?VERSION:?VERSION_BITS, PBin/binary, (term_to_binary(Msg))/binary>>.

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
    case Width - byte_size(Binary) of
        0 -> Binary;
        N when N > 0-> <<0:(N*8), Binary/binary>>
    end.
