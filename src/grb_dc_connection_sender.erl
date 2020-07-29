-module(grb_dc_connection_sender).
-behaviour(shackle_client).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% External API
-export([start_connection/4,
         send_heartbeat/4,
         send_transaction/4,
         send_clocks/5,
         send_clocks_heartbeat/5,
         close/1]).

%% Shackle API
-export([init/1,
         setup/2,
         handle_request/2,
         handle_data/2,
         handle_timeout/2,
         terminate/1]).

-record(state, { connected_dc :: replica_id(), conn :: inter_dc_conn() }).

-spec start_connection(ReplicaID :: replica_id(),
                       IP :: inet:ip_address(),
                       Port :: inet:port_number(),
                       PoolSize :: non_neg_integer()) -> {ok, inter_dc_conn()} | {error, term()}.

start_connection(ReplicaID, IP, Port, PoolSize) ->
    PoolName = pool_name(IP, Port),
    PoolRes = shackle_pool:start(PoolName,
                                 grb_dc_connection_sender,
                                 [{address, IP},
                                  {port, Port},
                                  {reconnect, false},
                                  {socket_options, ?INTER_DC_SOCK_OPTS},
                                  {init_options, [ReplicaID, PoolName]}],
                                 [{pool_size, PoolSize}]),
    case PoolRes of
        ok -> {ok, PoolName};
        {error, pool_already_started} -> {ok, PoolName};
        {error, Res} -> {error, Res}
    end.

-spec send_heartbeat(inter_dc_conn(), replica_id(), partition_id(), grb_time:ts()) -> ok.
send_heartbeat(Pool, FromReplica, Partition, Timestamp) ->
    shackle:call(Pool, {heartbeat, FromReplica, Partition, Timestamp}).

-spec send_transaction(inter_dc_conn(), replica_id(), partition_id(), {term(), #{}, vclock()}) -> ok.
send_transaction(Pool, FromReplica, Partition, Transaction) ->
    shackle:call(Pool, {transaction, FromReplica, Partition, Transaction}).

-spec send_clocks(inter_dc_conn(), replica_id(), partition_id(), vclock(), vclock()) -> ok.
send_clocks(Pool, FromReplica, Partition, KnownVC, StableVC) ->
    shackle:call(Pool, {clocks, FromReplica, Partition, KnownVC, StableVC}).

-spec send_clocks_heartbeat(inter_dc_conn(), replica_id(), partition_id(), vclock(), vclock()) -> ok.
send_clocks_heartbeat(Pool, FromReplica, Partition, KnownVC, StableVC) ->
    shackle:call(Pool, {clocks_heartbeat, FromReplica, Partition, KnownVC, StableVC}).

-spec close(inter_dc_conn()) -> ok.
close(PoolName) ->
    _ = shackle_pool:stop(PoolName),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% shackle callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ReplicaId, PoolName]) ->
    {ok, #state{connected_dc=ReplicaId, conn=PoolName}}.

setup(_Socket, State) ->
    {ok, State}.

handle_request({heartbeat, FromReplica, Partition, Timestamp}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #blue_heartbeat{timestamp=Timestamp}),
    {ok, Msg, State};

handle_request({transaction, FromReplica, Partition, {TxId, WS, CommitVC}}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #replicate_tx{tx_id=TxId,
                                                                                writeset=WS,
                                                                                commit_vc=CommitVC}),
    {ok, Msg, State};

handle_request({clocks, FromReplica, Partition, KnownVC, StableVC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #update_clocks{known_vc=KnownVC,
                                                                                 stable_vc=StableVC}),
    {ok, Msg, State};

handle_request({clocks_heartbeat, FromReplica, Partition, KnownVC, StableVC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #update_clocks_heartbeat{known_vc=KnownVC,
                                                                                           stable_vc=StableVC}),
    {ok, Msg, State};

handle_request(_, _) ->
    erlang:error(unknown_request).

handle_data(Data, State=#state{connected_dc=Replica, conn=PoolName}) ->
    ?LOG_INFO("~p to ~p (pool ~p) received unexpected data ~p", [?MODULE, Replica, PoolName, Data]),
    {ok, [], State}.

handle_timeout(RequestId, State) ->
    {ok, {RequestId, {error, timeout}}, State}.

%% Called if shackle_server receives tcp_closed or tcp_error
terminate(#state{conn=PoolName, connected_dc=ReplicaId}) ->
    grb_dc_connection_manager:connection_closed(ReplicaId, PoolName),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec pool_name(inet:ip_address(), inet:port_number()) -> inter_dc_conn().
pool_name(IP, Port) ->
    Str = "inter_dc_sender_" ++ inet:ntoa(IP) ++ ":" ++ integer_to_list(Port),
    try
        list_to_existing_atom(Str)
    catch _:_  ->
        list_to_atom(Str)
    end.
