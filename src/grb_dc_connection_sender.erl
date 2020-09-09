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
         send_red_prepare/7,
         send_red_accept/8,
         send_red_heartbeat/5,
         send_red_heartbeat_ack/4,
         send_red_decide_heartbeat/4,
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

%% I don't get why dialyzer fails here
-dialyzer({no_return, start_connection/4}).
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

-spec send_red_prepare(inter_dc_conn(), replica_id(), partition_id(), term(), #{}, #{}, vclock()) -> ok.
send_red_prepare(Pool, FromReplica, Partition, TxId, RS, WS, VC) ->
    shackle:call(Pool, {red_prepare, FromReplica, Partition, TxId, RS, WS, VC}).

-spec send_red_accept(Pool :: inter_dc_conn(),
                      FromReplica :: replica_id(),
                      Partition :: partition_id(),
                      TxId :: term(),
                      RS :: #{},
                      WS :: #{},
                      PrepareMsg :: {red_vote(), ballot(), vclock()},
                      Coord :: red_coord_location()) -> ok.

send_red_accept(Pool, FromReplica, Partition, TxId, RS, WS, PrepareMsg, Coord) ->
    shackle:call(Pool, {red_accept, FromReplica, Partition, TxId, RS, WS, PrepareMsg, Coord}).

-spec send_red_heartbeat(inter_dc_conn(), replica_id(), partition_id(), ballot(), grb_time:ts()) -> ok.
send_red_heartbeat(Pool, FromReplica, Partition, Ballot, Time) ->
    shackle:call(Pool, {red_hb, FromReplica, Partition, Ballot, Time}).

-spec send_red_heartbeat_ack(inter_dc_conn(), replica_id(), partition_id(), ballot()) -> ok.
send_red_heartbeat_ack(Pool, FromReplica, Partition, Ballot) ->
    shackle:call(Pool, {red_hb_ack, FromReplica, Partition, Ballot}).

-spec send_red_decide_heartbeat(inter_dc_conn(), replica_id(), partition_id(), ballot()) -> ok.
send_red_decide_heartbeat(Pool, FromReplica, Partition, Ballot) ->
    shackle:call(Pool, {red_hb_decide, FromReplica, Partition, Ballot}).

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

handle_request({red_prepare, FromReplica, Partition, TxId, RS, WS, VC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #prepare_red{tx_id=TxId,
                                                                               readset=RS,
                                                                               writeset=WS,
                                                                               snapshot_vc=VC}),
    {ok, Msg, State};

handle_request({red_accept, _FromReplica, _Partition, _TxId, _RS, _WS, _PrepareMsg, _Coord}, State) ->
    %% todo(borja, red): Implement
    %% Also, think about ways of not marshalling / unmarshalling data between nodes when using proxy,
    %% maybe pre-serialize something into an io_list(), and send that to the proxy, so it doesn't have
    %% to parse, then serialize again. An io_list() or a binary should be cheap to receive and send again,
    %% since most probably is larger than 64 bytes and passed by reference.
    {ok, <<>>, State};

handle_request({red_hb, FromReplica, Partition, Ballot, TimeStamp}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #red_heartbeat{ballot=Ballot, timestamp=TimeStamp}),
    {ok, Msg, State};

handle_request({red_hb_ack, FromReplica, Partition, Ballot}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #red_heartbeat_ack{ballot=Ballot}),
    {ok, Msg, State};

handle_request({red_hb_decide, FromReplica, Partition, Ballot}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #red_heartbeat_decide{ballot=Ballot}),
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
