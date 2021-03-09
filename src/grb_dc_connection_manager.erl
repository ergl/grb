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
         send_heartbeat/3,
         forward_heartbeat/4,
         send_tx/4,
         send_tx_array/6,
         send_tx_array/10,
         forward_tx/5,
         send_clocks/4,
         send_clocks_heartbeat/4]).

%% Red transactions
-export([send_red_prepare/8,
         send_red_accept/10,
         send_red_accept_ack/7,
         send_red_already_decided/6,
         send_red_decision/6,
         send_red_abort/6,
         send_red_deliver/5]).

%% Red heartbeats
-export([send_red_heartbeat/5,
         send_red_heartbeat_ack/5]).

-export([send_raw/3,
         send_raw_framed/3]).

%% Managemenet API
-export([connection_closed/2,
         close/1]).

%% Used through erpc or supervisor machinery
-ignore_xref([start_link/0,
              send_raw/3,
              connect_to/1,
              close/1]).

-ifndef(NO_FWD_REPLICATION).
-ignore_xref([send_heartbeat/3]).
-endif.

%% Supervisor
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {
    replicas :: cache(replicas, ordsets:ordset(replica_id())),
    connections :: cache({partition_id(), replica_id()}, inter_dc_conn())
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
        Connections = lists:map(fun(LocalPartition) ->
            {RemoteIP, Port} = maps:get(LocalPartition, RemoteNodes),
            {ok, Conn} = grb_dc_connection_sender:start_connection(
                ReplicaID, LocalPartition, RemoteIP, Port
            ),
            {LocalPartition, Conn}
        end, grb_dc_utils:my_partitions()),
        ?LOG_DEBUG("DC connections: ~p", [Connections]),
        ok = add_replica_connections(ReplicaID, Connections),
        ok
    catch Exn -> Exn
    end.

%% @doc Mark a replica as lost, connection has been closed.
%%
%%      Although connections are partition-aware, we will remove
%%      the connections to all nodes at the remote replica, and
%%      mark it as down.
-spec connection_closed(replica_id(), partition_id()) -> ok.
connection_closed(ReplicaId, Partition) ->
    gen_server:cast(?MODULE, {closed, ReplicaId, Partition}).

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

-spec add_replica_connections(Id :: replica_id(),
                              PartitionConnections :: [{partition_id(), inter_dc_conn()}]) -> ok.
add_replica_connections(Id, PartitionConnections) ->
    gen_server:call(?MODULE, {add_replica_connections, Id, PartitionConnections}).

-spec connected_replicas() -> [replica_id()].
connected_replicas() ->
    ets:lookup_element(?REPLICAS_TABLE, ?REPLICAS_TABLE_KEY, 2).

-spec send_heartbeat(ToId :: replica_id(),
                     Partition :: partition_id(),
                     Time :: grb_time:ts()) -> ok | {error, term()}.

send_heartbeat(ToId, Partition, Time) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:blue_heartbeat(Time)).

-spec forward_heartbeat(ToId :: replica_id(),
                        FromId :: replica_id(),
                        Partition :: partition_id(),
                        Time :: grb_time:ts()) -> ok | {error, term()}.

forward_heartbeat(ToId, FromId, Partition, Time) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:forward_heartbeat(FromId, Time)).

-spec send_tx(ToId :: replica_id(),
              Partition :: partition_id(),
              WS :: writeset(),
              VC :: vclock()) -> ok | {error, term()}.

send_tx(ToId, Partition, WS, VC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:transaction(WS, VC)).

-spec send_tx_array(ToId :: replica_id(),
                    Partition :: partition_id(),
                    Tx1 :: tx_entry(),
                    Tx2 :: tx_entry(),
                    Tx3 :: tx_entry(),
                    Tx4 :: tx_entry()) -> ok | {error, term()}.

send_tx_array(ToId, Partition, Tx1, Tx2, Tx3, Tx4) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:transaction_array(Tx1, Tx2, Tx3, Tx4)).

-spec send_tx_array(ToId :: replica_id(),
                    Partition :: partition_id(),
                    Tx1 :: tx_entry(),
                    Tx2 :: tx_entry(),
                    Tx3 :: tx_entry(),
                    Tx4 :: tx_entry(),
                    Tx5 :: tx_entry(),
                    Tx6 :: tx_entry(),
                    Tx7 :: tx_entry(),
                    Tx8 :: tx_entry()) -> ok | {error, term()}.

send_tx_array(ToId, Partition, Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:transaction_array(Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8)).

-spec forward_tx(ToId :: replica_id(),
                 FromId :: replica_id(),
                 Partition :: partition_id(),
                 WS :: writeset(),
                 VC :: vclock()) -> ok | {error, term()}.

forward_tx(ToId, FromId, Partition, WS, VC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:forward_transaction(FromId, WS, VC)).

-spec send_clocks(ToId :: replica_id(),
                  Partition :: partition_id(),
                  KnownVC :: vclock(),
                  StableVC :: vclock()) -> ok | {error, term()}.

-ifdef(STABLE_SNAPSHOT).
send_clocks(ToId, Partition, KnownVC, _StableVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:clocks(KnownVC)).
-else.
send_clocks(ToId, Partition, KnownVC, StableVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:clocks(KnownVC, StableVC)).
-endif.

%% @doc Same as send_clocks/5, but let the remote node to use knownVC as a heartbeat
-spec send_clocks_heartbeat(ToId :: replica_id(),
                            Partition :: partition_id(),
                            KnownVC :: vclock(),
                            StableVC :: vclock()) -> ok | {error, term()}.
-ifdef(STABLE_SNAPSHOT).
send_clocks_heartbeat(ToId, Partition, KnownVC, _StableVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:clocks_heartbeat(KnownVC)).
-else.
send_clocks_heartbeat(ToId, Partition, KnownVC, StableVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:clocks_heartbeat(KnownVC, StableVC)).
-endif.

-spec send_red_prepare(ToId :: replica_id(),
                       Coordinator :: red_coord_location(),
                       Partition :: partition_id(),
                       TxId :: term(),
                       Label :: tx_label(),
                       RS :: readset(),
                       WS :: writeset(),
                       VC :: vclock()) -> ok | {error, term()}.

send_red_prepare(ToId, Coordinator, Partition, TxId, Label, RS, WS, VC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:red_prepare(Coordinator, TxId, Label, RS, WS, VC)).

-spec send_red_accept(ToId :: replica_id(),
                      Coordinator :: red_coord_location(),
                      Partition :: partition_id(),
                      Ballot :: ballot(),
                      Vote :: red_vote(),
                      TxId :: term(),
                      Label :: tx_label(),
                      RS :: readset(),
                      WS :: writeset(),
                      PrepareVC :: vclock()) -> ok | {error, term()}.

send_red_accept(ToId, Coordinator, Partition, Ballot, Vote, TxId, Label, RS, WS, VC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition,
             grb_dc_messages:red_accept(Coordinator, Ballot, Vote, TxId, Label, RS, WS, VC)).

-spec send_red_accept_ack(replica_id(), node(), partition_id(), ballot(), term(), red_vote(), grb_time:ts()) -> ok.
send_red_accept_ack(ToId, ToNode, Partition, Ballot, TxId, Vote, PrepareTS) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition,
             grb_dc_messages:red_accept_ack(ToNode, Ballot, Vote, TxId, PrepareTS)).

-spec send_red_already_decided(replica_id(), node(), partition_id(), term(), red_vote(), vclock()) -> ok.
send_red_already_decided(ToId, ToNode, Partition, TxId, Decision, CommitVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition,
             grb_dc_messages:red_already_decided(ToNode, Decision, TxId, CommitVC)).

-spec send_red_decision(replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
send_red_decision(ToId, Partition, Ballot, TxId, Decision, CommitVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition,
             grb_dc_messages:red_decision(Ballot, Decision, TxId, CommitVC)).

-spec send_red_abort(replica_id(), partition_id(), ballot(), term(), term(), vclock()) -> ok.
send_red_abort(ToId, Partition, Ballot, TxId, Reason, CommitVC) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition,
             grb_dc_messages:red_learn_abort(Ballot, TxId, Reason, CommitVC)).

-spec send_red_deliver(ToId :: replica_id(),
                       Partition :: partition_id(),
                       Ballot :: ballot(),
                       Timestamp :: grb_time:ts(),
                       TransactionIds :: [{term(), tx_label(), vclock()} | red_heartbeat_id() ]) -> ok.

send_red_deliver(ToId, Partition, Ballot, Timestamp, TransactionIds) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition,
             grb_dc_messages:red_deliver(Partition, Ballot, Timestamp, TransactionIds)).

-spec send_red_heartbeat(ToId :: replica_id(),
                         Partition :: partition_id(),
                         Ballot :: ballot(),
                         Id :: term(),
                         Time :: grb_time:ts()) -> ok | {error, term()}.

send_red_heartbeat(ToId, Partition, Ballot, Id, Time) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:red_heartbeat(Ballot, Id, Time)).

-spec send_red_heartbeat_ack(replica_id(), partition_id(), ballot(), term(), grb_time:ts()) -> ok | {error, term()}.
send_red_heartbeat_ack(ToId, Partition, Ballot, Id, Time) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, grb_dc_messages:red_heartbeat_ack(Ballot, Id, Time)).

-spec send_raw(replica_id(), partition_id(), binary()) -> ok | {error, term()}.
send_raw(ToId, Partition, Msg) ->
    send_raw(?CONN_POOL_TABLE, ToId, Partition, Msg).

-spec send_raw_framed(replica_id(), partition_id(), iolist()) -> ok | {error, term()}.
send_raw_framed(ToId, Partition, IOList) ->
    try
        Connection = ets:lookup_element(?CONN_POOL_TABLE, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_process_framed(Connection, IOList)
    catch _:_  ->
        {error, gone}
    end.

-spec send_raw(ets:tab(), replica_id(), partition_id(), binary()) -> ok | {error, term()}.
send_raw(Table, ToId, Partition, Msg) ->
    try
        Connection = ets:lookup_element(Table, {Partition, ToId}, 2),
        grb_dc_connection_sender:send_process(Connection, Msg)
    catch _:_  ->
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

handle_call({add_replica_connections, ReplicaId, PartitionConnections}, _From, State) ->
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:add_element(ReplicaId, Replicas)}),
    ok = add_replica_connections_internal(ReplicaId, PartitionConnections),
    {reply, ok, State};

handle_call({close, ReplicaId}, _From, State) ->
    ?LOG_INFO("Closing connections to ~p", [ReplicaId]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    ok = close_replica_connections(ReplicaId, State),
    {reply, ok, State};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({closed, ReplicaId, Partition}, State) ->
    ?LOG_INFO("Connection lost to ~p (~p), removing reference", [ReplicaId, Partition]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    ok = close_replica_connections(ReplicaId, Partition, State),
    {noreply, State};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_replica_connections_internal(ReplicaId :: replica_id(),
                                       PartitionConnections :: [{partition_id(), inter_dc_conn()}]) -> ok.

add_replica_connections_internal(ReplicaId, PartitionConnections) ->
    Objects = [ {{P, ReplicaId}, C} || {P, C} <- PartitionConnections],
    true = ets:insert(?CONN_POOL_TABLE, Objects),
    ok.

-spec close_replica_connections(replica_id(), #state{}) -> ok.
close_replica_connections(ReplicaId, #state{connections=BlueConnTable}) ->
    MatchSpec = [{{{'_', ReplicaId}, '$1'}, [], ['$1']}],
    DeleteMatchSpec = [{{{'_', ReplicaId}, '$1'}, [], [true]}],
    BlueConns = ets:select(BlueConnTable, MatchSpec),
    _ = ets:select_delete(BlueConnTable, DeleteMatchSpec),
    [ grb_dc_connection_sender:close(C) || C <- BlueConns],
    ok.

-spec close_replica_connections(replica_id(), partition_id(), #state{}) -> ok.
close_replica_connections(ReplicaId, Partition, #state{connections=BlueConns}) ->
    true = ets:delete(BlueConns, {Partition, ReplicaId}),
    ok.
