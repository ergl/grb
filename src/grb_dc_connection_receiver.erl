-module(grb_dc_connection_receiver).

-behaviour(gen_server).
-behavior(ranch_protocol).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% Module API
-export([start_service/0]).

%% ranch_protocol callback
-export([start_link/4]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(SERVICE, grb_inter_dc).
-define(SERVICE_POOL, (1 * erlang:system_info(schedulers_online))).

-define(EXPAND_BUFFER_INTERVAL, 100).

-record(state, {
    socket :: inet:socket(),
    transport :: module(),
    sender_partition = undefined :: partition_id() | undefined,
    sender_replica = undefined :: replica_id() | undefined,
    recalc_buffer_timer = undefined :: reference() | undefined
}).

-spec start_service() -> ok.
start_service() ->
    start_service(?SERVICE, application:get_env(grb, inter_dc_port)).

start_service(ServiceName, {ok, Port}) ->
    {ok, _} = ranch:start_listener(ServiceName, ranch_tcp,
                                   [{port, Port}, {num_acceptors, ?SERVICE_POOL}, {max_connections, infinity}],
                                   ?MODULE, []),
    ActualPort = ranch:get_port(ServiceName),
    ?LOG_INFO("~p server started on port ~p", [?MODULE, ActualPort]),
    ok.

%% Ranch workaround for gen_server
%% Socket is deprecated, will be removed
start_link(Ref, _Sock, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, ?INTER_DC_SOCK_OPTS),
    State = #state{socket=Socket, transport=Transport},
    gen_server:enter_loop(?MODULE, [], State).

handle_call(E, From, S) ->
    ?LOG_WARNING("~p got unexpected call with msg ~w from ~w", [?MODULE, E, From]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p got unexpected cast with msg ~w", [?MODULE, E]),
    {noreply, S}.

terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_CREATE:?MSG_KIND_BITS,
                    IPsBin/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    IPs = binary_to_term(IPsBin, [safe]),
    Resp = grb_dc_manager:create_replica_groups(ip_addresses, IPs),
    Transport:send(Socket, term_to_binary(Resp)),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_GET_DESCRIPTOR:?MSG_KIND_BITS>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    Transport:send(Socket, term_to_binary(grb_dc_manager:replica_descriptor())),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_CONNECT_TO_DESCR:?MSG_KIND_BITS,
                    Payload/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    Descriptors = binary_to_term(Payload),
    Res = grb_dc_manager:connect_to_replicas(Descriptors),
    Transport:send(Socket, term_to_binary(Res)),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_START_BLUE_PROCESSES:?MSG_KIND_BITS>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    Transport:send(Socket, term_to_binary(grb_dc_manager:start_propagation_processes())),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_START_RED_FOLLOWER:?MSG_KIND_BITS,
                    LeaderIdB/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    LeaderId = binary_to_term(LeaderIdB),
    ok = grb_dc_manager:start_paxos_follower(LeaderId),
    Transport:send(Socket, <<>>),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_PING:?MSG_KIND_BITS,
                    P:?PARTITION_BITS/big-unsigned-integer,
                    Payload/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    SenderReplica = binary_to_term(Payload),
    ?LOG_DEBUG("Received connect ping from ~p:~p", [SenderReplica, P]),
    Transport:setopts(Socket, [{active, once}]),
    ok = expand_drv_buffer(Transport, Socket),
    {noreply, State#state{sender_partition=P, sender_replica=SenderReplica,
                          recalc_buffer_timer=erlang:send_after(?EXPAND_BUFFER_INTERVAL, self(), recalc_buffer)}};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, Payload/binary>>},
    State = #state{socket=Socket,
                   transport=Transport,
                   sender_partition=Partition,
                   sender_replica=SenderReplica}
) ->
    Request = grb_dc_messages:decode_payload(SenderReplica, Partition, Payload),
    ?LOG_DEBUG("Received msg to ~p: ~p", [Partition, Request]),
    ok = handle_request(SenderReplica, Partition, Request),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp, Socket, Data}, State = #state{transport=Transport}) ->
    ?LOG_WARNING("~p received unknown data ~p", [?MODULE, Data]),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    ?LOG_INFO("replication server received tcp_closed"),
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    ?LOG_INFO("replication server received tcp_error ~p", [Reason]),
    {stop, Reason, S};

handle_info(timeout, State) ->
    ?LOG_INFO("replication server received timeout"),
    {stop, normal, State};

handle_info(recalc_buffer, State = #state{socket=Socket,
                                          transport=Transport,
                                          recalc_buffer_timer=Ref}) ->
    ?CANCEL_TIMER_FAST(Ref),
    ok = expand_drv_buffer(Transport, Socket),
    {noreply, State#state{recalc_buffer_timer=erlang:send_after(?EXPAND_BUFFER_INTERVAL, self(), recalc_buffer)}};

handle_info(E, S) ->
    ?LOG_WARNING("replication server received unexpected info with msg ~w", [E]),
    {noreply, S}.

%% Expand driver buffer size from time to time
%% This will eventually settle in a stable state if the recbuf stops changing.
-spec expand_drv_buffer(module(), gen_tcp:socket()) -> ok.
expand_drv_buffer(Transport, Socket) ->
    {ok, Proplist} = Transport:getopts(Socket, [recbuf, buffer]),
    {recbuf, RecBuffer} = lists:keyfind(recbuf, 1, Proplist),
    {buffer, DrvBuffer0} = lists:keyfind(buffer, 1, Proplist),
    DrvBuffer = erlang:max(RecBuffer * 2, DrvBuffer0),
    case Transport:setopts(Socket, [{buffer, DrvBuffer}]) of
        {error, _} ->
            %% No room to expand, keep it the same
            ok = Transport:setopts(Socket, [{buffer, DrvBuffer0}]);
        ok ->
            ok
    end.

-spec handle_request(replica_id(), partition_id(), replica_message()) -> ok.
handle_request(_, Partition, #red_prepare{coord_location=Coordinator, tx_id=TxId, tx_label=Label,
                                          readset=RS, writeset=WS, snapshot_vc=VC}) ->
    grb_paxos_vnode:prepare_local(Partition, TxId, Label, RS, WS, VC, Coordinator);

handle_request(_ConnReplica, Partition, #red_accept{coord_location=Coordinator, sequence_number=Sequence,
                                                    ballot=Ballot, tx_id=TxId, tx_label=Label,
                                                    readset=RS, writeset=WS,
                                                    decision=Vote, prepare_vc=VC}) ->

    grb_paxos_vnode:accept(Partition, Sequence, Ballot, TxId, Label, RS, WS, Vote, VC, Coordinator);

handle_request(ConnReplica, Partition, #red_accept_ack{target_node=Node, ballot=Ballot, tx_id=TxId,
                                                       decision=Vote, prepare_ts=PrepareTs}) ->

    grb_red_coordinator:accept_ack(Node, ConnReplica, Partition, Ballot, TxId, Vote, PrepareTs);

handle_request(_, Partition, #red_decision{ballot=Ballot, tx_id=TxId, decision=Decision, commit_ts=CommitTs}) ->
    grb_paxos_vnode:decide_local(Partition, Ballot, TxId, Decision, CommitTs);

handle_request(_, _, #red_already_decided{target_node=Node, tx_id=TxId, decision=Vote, commit_vc=CommitVC}) ->
    grb_red_coordinator:already_decided(Node, TxId, Vote, CommitVC);

handle_request(_, Partition, #red_learn_abort{ballot=Ballot, tx_id=TxId, reason=Reason, commit_ts=CommitTs}) ->
    grb_paxos_vnode:learn_abort(Partition, Ballot, TxId, Reason, CommitTs);

handle_request(_, Partition, #red_deliver{ballot=Ballot, timestamp=Ts,
                                          sequence_number=Sequence, transactions=TransactionIds}) ->

    grb_paxos_vnode:deliver(Partition, Sequence, Ballot, Ts, TransactionIds);

handle_request(ConnReplica, Partition, #red_heartbeat{ballot=B, heartbeat_id=Id,
                                                      timestamp=Ts, sequence_number=Seq}) ->

    grb_paxos_vnode:accept_heartbeat(Partition, ConnReplica, Seq, B, Id, Ts);

handle_request(_, Partition, #red_heartbeat_ack{ballot=B, heartbeat_id=Id, timestamp=Ts}) ->
    grb_red_heartbeat:handle_accept_ack(Partition, B, Id, Ts);

handle_request(ConnReplica, Partition, CausalMessage) ->
    grb_causal_sequencer:sequence(Partition, ConnReplica, CausalMessage).
