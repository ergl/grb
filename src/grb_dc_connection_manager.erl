-module(grb_dc_connection_manager).
-behavior(gen_server).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REPLICAS_TABLE, connected_replicas).
-define(REPLICAS_TABLE_KEY, replicas).
-define(CONN_POOL_TABLE, connection_pools).

%% External API
-export([connect_to/1,
         connected_replicas/0,
         send_tx/4,
         send_clocks/5,
         send_heartbeat/4,
         send_clocks_heartbeat/5,
         send_red_prepare/7,
         send_red_accept/8,
         send_red_heartbeat/5,
         send_red_heartbeat_ack/4,
         send_red_decide_heartbeat/4]).

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
    connections :: cache({partition_id(), replica_id()}, inter_dc_conn()),
    %% for cleanup purposes, no need to expose with ETS to other nodes
    conn_index = #{} :: #{inter_dc_conn() => undefined}
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
            ConnPool = case maps:is_key(Entry, EntryMap) of
                true ->
                    maps:get(Entry, EntryMap);
                false ->
                    case grb_dc_connection_sender:start_connection(ReplicaID, RemoteIP, RemotePort, 16) of
                        {ok, Pool} ->
                            Pool;
                        Err ->
                            throw({error, {sender_connection, ReplicaID, RemoteIP, Err}})
                    end
            end,
            {EntryMap#{Entry => ConnPool}, PartitionMap#{Partition => ConnPool}}
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
-spec connection_closed(replica_id(), inter_dc_conn()) -> ok.
connection_closed(ReplicaId, PoolName) ->
    gen_server:cast(?MODULE, {closed, ReplicaId, PoolName}).

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

-spec add_replica_connections(replica_id(), #{partition_id() => inter_dc_conn()}) -> ok.
add_replica_connections(Id, PartitionConnections) ->
    gen_server:call(?MODULE, {add_replica_connections, Id, PartitionConnections}).

-spec connected_replicas() -> [replica_id()].
connected_replicas() ->
    ets:lookup_element(?REPLICAS_TABLE, ?REPLICAS_TABLE_KEY, 2).

-spec send_heartbeat(ToId :: replica_id(),
                     FromId :: replica_id(),
                     Partition :: partition_id(),
                     Time :: grb_time:ts()) -> ok | {error, term()}.

send_heartbeat(ToId, FromId, Partition, Time) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_heartbeat(PoolName, FromId, Partition, Time)
    catch _:_ ->
        {error, gone}
    end.

-spec send_tx(ToId :: replica_id(),
              FromId :: replica_id(),
              Partition :: partition_id(),
              Tx :: {term(), #{}, vclock()}) -> ok | {error, term()}.

send_tx(ToId, FromId, Partition, Transaction) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_transaction(PoolName, FromId, Partition, Transaction)
    catch _:_ ->
        {error, gone}
    end.

-spec send_clocks(ToId :: replica_id(),
                  FromId :: replica_id(),
                  Partition :: partition_id(),
                  KnownVC :: vclock(),
                  StableVC :: vclock()) -> ok | {error, term()}.

send_clocks(ToId, FromId, Partition, KnownVC, StableVC) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_clocks(PoolName, FromId, Partition, KnownVC, StableVC)
    catch _:_ ->
        {error, gone}
    end.

%% @doc Same as send_clocks/5, but let the remote node to use knownVC as a heartbeat
-spec send_clocks_heartbeat(ToId :: replica_id(),
                            FromId :: replica_id(),
                            Partition :: partition_id(),
                            KnownVC :: vclock(),
                            StableVC :: vclock()) -> ok | {error, term()}.

send_clocks_heartbeat(ToId, FromId, Partition, KnownVC, StableVC) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_clocks_heartbeat(PoolName, FromId, Partition, KnownVC, StableVC)
    catch _:_ ->
        {error, gone}
    end.

-spec send_red_prepare(ToId :: replica_id(),
                       FromId :: replica_id(),
                       Partition :: partition_id(),
                       TxId :: term(),
                       RS :: #{},
                       WS :: #{},
                       VC :: vclock()) -> ok | {error, term()}.
send_red_prepare(ToId, FromId, Partition, TxId, RS, WS, VC) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_red_prepare(PoolName, FromId, Partition, TxId, RS, WS, VC)
    catch _:_ ->
        {error, gone}
    end.

-spec send_red_accept(ToId :: replica_id(),
                      FromId :: replica_id(),
                      Partition :: partition_id(),
                      TxId :: term(),
                      RS :: #{},
                      WS :: #{},
                      Prepare :: {red_vote(), ballot(), vclock()},
                      Coordinator :: red_coord_location()) -> ok | {error, term()}.

send_red_accept(ToId, FromId, Partition, TxId, RS, WS, PrepareMsg, Coordinator) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_red_accept(PoolName, FromId, Partition, TxId, RS, WS, PrepareMsg, Coordinator)
    catch _:_ ->
        {error, gone}
    end.

-spec send_red_heartbeat(ToId :: replica_id(),
                         FromId :: replica_id(),
                         Partition :: partition_id(),
                         Ballot :: ballot(),
                         Time :: grb_time:ts()) -> ok | {error, term()}.

send_red_heartbeat(ToId, FromId, Partition, Ballot, Time) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_red_heartbeat(PoolName, FromId, Partition, Ballot, Time)
    catch _:_ ->
        {error, gone}
    end.

-spec send_red_heartbeat_ack(replica_id(), replica_id(), partition_id(), ballot()) -> ok | {error, term()}.
send_red_heartbeat_ack(ToId, FromId, Partition, Ballot) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_red_heartbeat_ack(PoolName, FromId, Partition, Ballot)
    catch _:_ ->
        {error, gone}
    end.

-spec send_red_decide_heartbeat(replica_id(), replica_id(), partition_id(), ballot()) -> ok | {error, term()}.
send_red_decide_heartbeat(ToId, FromId, Partition, Ballot) ->
    try
        PoolName = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_red_decide_heartbeat(PoolName, FromId, Partition, Ballot)
    catch _:_ ->
        {error, gone}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    ReplicaTable = ets:new(?REPLICAS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:new()}),
    ConnPoolTable = ets:new(?CONN_POOL_TABLE, [ordered_set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{replicas=ReplicaTable,
                connections=ConnPoolTable}}.

handle_call({add_replica_connections, ReplicaId, PartitionConnections}, _From, State=#state{conn_index=Index0}) ->
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:add_element(ReplicaId, Replicas)}),
    {Objects, Index} = maps:fold(fun(Partition, PoolName, {ObjectAcc, IndexAcc}) ->
        {
            [{{Partition, ReplicaId}, PoolName} | ObjectAcc],
            IndexAcc#{PoolName => undefined}
        }
    end, {[], Index0}, PartitionConnections),
    true = ets:insert(?CONN_POOL_TABLE, Objects),
    {reply, ok, State#state{conn_index=Index}};

handle_call({close, ReplicaId}, _From, State=#state{conn_index=Index}) ->
    ?LOG_INFO("Closing connections to ~p", [ReplicaId]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    Pools = ets:select(?CONN_POOL_TABLE, [{{{'_', ReplicaId}, '$1'}, [], ['$1']}]),
    _ = ets:select_delete(?CONN_POOL_TABLE, [{{{'_', ReplicaId}, '_'}, [], [true]}]),
    [try
         grb_dc_connection_sender:close(P)
     catch _:_ -> ok
     end || P <- Pools],
    {reply, ok, State#state{conn_index=maps:without(Pools, Index)}};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({closed, ReplicaId, PoolName}, State=#state{conn_index=Index}) ->
    ?LOG_INFO("Connection lost to ~p (~p), removing all references", [ReplicaId, PoolName]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    Pools = ets:select(?CONN_POOL_TABLE, [{{{'_', ReplicaId}, '$1'}, [], ['$1']}]),
    _ = ets:select_delete(?CONN_POOL_TABLE, [{{{'_', ReplicaId}, '_'}, [], [true]}]),
    [case P of
        PoolName -> ok;
        _ -> grb_dc_connection_sender:close(P)
     end || P <- Pools],
    {noreply, State#state{conn_index=maps:without(Pools, Index)}};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.
