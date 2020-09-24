-module(grb_dc_connection_sender).
-behaviour(shackle_client).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% Constructors
-export([start_connection/3,
         start_red_connection/3,
         close/1]).

%% Blue Transactions
-export([send_heartbeat/4,
         send_transaction/4,
         send_clocks/5,
         send_clocks_heartbeat/5]).

%% Red transactions
-export([send_red_prepare/7,
         send_red_accept/7,
         send_red_accept_ack/7,
         send_red_decided/6,
         send_red_decision/7]).

%% Red hearbeats
-export([send_red_heartbeat/6,
         send_red_heartbeat_ack/6,
         send_red_decide_heartbeat/6]).

%% Shackle API
-export([init/1,
         setup/2,
         handle_request/2,
         handle_data/2,
         handle_timeout/2,
         terminate/1]).

-type conn_kind() :: {blue, inter_dc_conn()} | {red, inter_dc_red_conn()}.
-record(state, { connected_dc :: replica_id(), conn :: conn_kind() }).

-spec start_connection(ReplicaID :: replica_id(),
                       IP :: inet:ip_addres(),
                       Port :: inet:port_number()) -> {ok, inter_dc_conn()} | {error, term()}.

%% I don't get why dialyzer fails here
-dialyzer({no_return, start_connection/3}).
start_connection(ReplicaID, IP, Port) ->
    {ok, PoolSize} = application:get_env(grb, inter_dc_pool_size),
    start_connection(ReplicaID, IP, Port, PoolSize).

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
                                  {init_options, [blue, ReplicaID, PoolName]}],
                                 [{pool_size, PoolSize}]),
    case PoolRes of
        ok -> {ok, PoolName};
        {error, pool_already_started} -> {ok, PoolName};
        {error, Res} -> {error, Res}
    end.

-spec start_red_connection(ReplicaID :: replica_id(),
                           IP :: inet:ip_addres(),
                           Port :: inet:port_number()) -> {ok, inter_dc_red_conn()} | {error, term()}.

-dialyzer({no_return, start_red_connection/3}).
start_red_connection(ReplicaID, IP, Port) ->
    {ok, PoolSize} = application:get_env(grb, inter_dc_red_pool_size),
    start_red_connection(ReplicaID, IP, Port, PoolSize).

-spec start_red_connection(ReplicaID :: replica_id(),
                           IP :: inet:ip_address(),
                           Port :: inet:port_number(),
                           PoolSize :: non_neg_integer()) -> {ok, inter_dc_red_conn()} | {error, term()}.

%% I don't get why dialyzer fails here
-dialyzer({no_return, start_red_connection/4}).
start_red_connection(ReplicaID, IP, Port, PoolSize) ->
    PoolName = red_pool_name(IP, Port),
    PoolRes = shackle_pool:start(PoolName,
                                 grb_dc_connection_sender,
                                 [{address, IP},
                                  {port, Port},
                                  {reconnect, false},
                                  {socket_options, ?INTER_DC_SOCK_OPTS},
                                  {init_options, [red, ReplicaID, PoolName]}],
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

-spec send_red_prepare(inter_dc_red_conn(), red_coord_location(), partition_id(), term(), #{}, #{}, vclock()) -> ok.
send_red_prepare(Pool, Coordinator, Partition, TxId, RS, WS, VC) ->
    shackle:call(Pool, {red_prepare, Coordinator, Partition, TxId, RS, WS, VC}).

-spec send_red_accept(Pool :: inter_dc_red_conn(),
                      Coord :: red_coord_location(),
                      Partition :: partition_id(),
                      TxId :: term(),
                      RS :: #{},
                      WS :: #{},
                      Prepare :: {red_vote(), ballot(), vclock()}) -> ok.

send_red_accept(Pool, Coord, Partition, TxId, RS, WS, Prepare) ->
    shackle:call(Pool, {red_accept, Coord, Partition, TxId, RS, WS, Prepare}).

-spec send_red_accept_ack(inter_dc_red_conn(), node(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
send_red_accept_ack(Pool, ToNode, Partition, Ballot, TxId, Vote, PrepareVC) ->
    shackle:call(Pool, {red_accept_ack, ToNode, Partition, Ballot, TxId, Vote, PrepareVC}).

-spec send_red_decided(inter_dc_red_conn(), node(), partition_id(), term(), red_vote(), vclock()) -> ok.
send_red_decided(Pool, ToNode, Partition, TxId, Decision, CommitVC) ->
    shackle:call(Pool, {red_decided, ToNode, Partition, TxId, Decision, CommitVC}).

-spec send_red_decision(inter_dc_red_conn(), replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
send_red_decision(Pool, FromReplica, Partition, Ballot, TxId, Decision, CommitVC) ->
    shackle:call(Pool, {red_decision, FromReplica, Partition, Ballot, TxId, Decision, CommitVC}).

-spec send_red_heartbeat(inter_dc_red_conn(), replica_id(), partition_id(), ballot(), term(), grb_time:ts()) -> ok.
send_red_heartbeat(Pool, FromReplica, Partition, Ballot, Id, Time) ->
    shackle:call(Pool, {red_hb, FromReplica, Partition, Ballot, Id, Time}).

-spec send_red_heartbeat_ack(inter_dc_red_conn(), replica_id(), partition_id(), ballot(), term(), grb_time:ts()) -> ok.
send_red_heartbeat_ack(Pool, FromReplica, Partition, Ballot, Id, Time) ->
    shackle:call(Pool, {red_hb_ack, FromReplica, Partition, Ballot, Id, Time}).

-spec send_red_decide_heartbeat(inter_dc_red_conn(), replica_id(), partition_id(), ballot(), term(), grb_time:ts()) -> ok.
send_red_decide_heartbeat(Pool, FromReplica, Partition, Ballot, Id, Time) ->
    shackle:call(Pool, {red_hb_decide, FromReplica, Partition, Ballot, Id, Time}).

-spec close(inter_dc_conn() | inter_dc_red_conn()) -> ok.
close(PoolName) ->
    _ = shackle_pool:stop(PoolName),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% shackle callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Kind, ReplicaId, PoolName]) ->
    {ok, #state{connected_dc=ReplicaId, conn={Kind, PoolName}}}.

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

handle_request({red_prepare, Coordinator, Partition, TxId, RS, WS, VC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(Coordinator, Partition, #red_prepare{tx_id=TxId,
                                                                               readset=RS,
                                                                               writeset=WS,
                                                                               snapshot_vc=VC}),
    {ok, Msg, State};

handle_request({red_accept, Coordinator, Partition, TxId, RS, WS, {Vote, Ballot, VC}}, State) ->
    Msg = grb_dc_message_utils:encode_msg(Coordinator, Partition, #red_accept{ballot=Ballot,
                                                                              tx_id=TxId,
                                                                              readset=RS,
                                                                              writeset=WS,
                                                                              decision=Vote,
                                                                              prepare_vc=VC}),
    {ok, Msg, State};

handle_request({red_accept_ack, ToNode, Partition, Ballot, TxId, Vote, PrepareVC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(ToNode, Partition, #red_accept_ack{ballot=Ballot,
                                                                             tx_id=TxId,
                                                                             decision=Vote,
                                                                             prepare_vc=PrepareVC}),
    {ok, Msg, State};

handle_request({red_decided, ToNode, Partition, TxId, Decision, CommitVC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(ToNode, Partition, #red_already_decided{tx_id=TxId,
                                                                                  decision=Decision,
                                                                                  commit_vc=CommitVC}),
    {ok, Msg, State};

handle_request({red_decision, FromReplica, Partition, Ballot, TxId, Decision, CommitVC}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition, #red_decision{ballot=Ballot,
                                                                                tx_id=TxId,
                                                                                decision=Decision,
                                                                                commit_vc=CommitVC}),
    {ok, Msg, State};

handle_request({red_hb, FromReplica, Partition, Ballot, Id, Timestamp}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition,
                                          #red_heartbeat{ballot=Ballot,
                                                         heartbeat_id=Id,
                                                         timestamp=Timestamp}),
    {ok, Msg, State};

handle_request({red_hb_ack, FromReplica, Partition, Ballot, Id, Timestamp}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition,
                                          #red_heartbeat_ack{ballot=Ballot,
                                                             heartbeat_id=Id,
                                                             timestamp=Timestamp}),
    {ok, Msg, State};

handle_request({red_hb_decide, FromReplica, Partition, Ballot, Id, Timestamp}, State) ->
    Msg = grb_dc_message_utils:encode_msg(FromReplica, Partition,
                                          #red_heartbeat_decide{ballot=Ballot,
                                                                heartbeat_id=Id,
                                                                timestamp=Timestamp}),
    {ok, Msg, State};

handle_request(_, _) ->
    erlang:error(unknown_request).

handle_data(Data, State=#state{connected_dc=Replica, conn=PoolName}) ->
    ?LOG_INFO("~p to ~p (pool ~p) received unexpected data ~p", [?MODULE, Replica, PoolName, Data]),
    {ok, [], State}.

handle_timeout(RequestId, State) ->
    {ok, {RequestId, {error, timeout}}, State}.

%% Called if shackle_server receives tcp_closed or tcp_error
terminate(#state{conn={_, PoolName}, connected_dc=ReplicaId}) ->
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

-spec red_pool_name(inet:ip_address(), inet:port_number()) -> inter_dc_red_conn().
red_pool_name(IP, Port) ->
    Str = "inter_dc_red_sender_" ++ inet:ntoa(IP) ++ ":" ++ integer_to_list(Port),
    try
        list_to_existing_atom(Str)
    catch _:_  ->
        list_to_atom(Str)
    end.
