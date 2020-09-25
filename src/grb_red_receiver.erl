-module(grb_red_receiver).

-behaviour(gen_server).
-behavior(ranch_protocol).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% Module API
-export([start_server/0]).

%% ranch_protocol callback
-export([start_link/4]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(SERVICE_NAME, grb_inter_dc_red).
-define(SERVICE_POOL, (1 * erlang:system_info(schedulers_online))).

-record(state, {
    socket :: inet:socket(),
    transport :: module()
}).

-ifdef(BLUE_KNOWN_VC).
start_server() -> ok.
-else.
start_server() ->
    {ok, Port} = application:get_env(grb, inter_dc_red_port),
    {ok, _} = ranch:start_listener(?SERVICE_NAME,
                                   ranch_tcp,
                                   [{port, Port},
                                    {num_acceptors, ?SERVICE_POOL},
                                    {max_connections, infinity}],
                                   ?MODULE,
                                   []),
    ActualPort = ranch:get_port(?SERVICE_NAME),
    ?LOG_INFO("~p server started on port ~p", [?MODULE, ActualPort]),
    ok.
-endif.

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
    {tcp, Socket, <<?VERSION:?VERSION_BITS, P:?PARTITION_BITS/big-unsigned-integer, Payload/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    {SourceReplica, Request} = grb_dc_message_utils:decode_payload(Payload),
    ?LOG_DEBUG("Received msg from ~p to ~p: ~p", [SourceReplica, P, Request]),
    ok = handle_request(P, SourceReplica, Request),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp, Socket, Data}, State = #state{transport=Transport}) ->
    ?LOG_WARNING("~p received unknown data ~p", [?MODULE, Data]),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    ?LOG_INFO("red server received tcp_closed"),
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    ?LOG_INFO("red server received tcp_error ~p", [Reason]),
    {stop, Reason, S};

handle_info(timeout, State) ->
    ?LOG_INFO("red server received timeout"),
    {stop, normal, State};

handle_info(E, S) ->
    ?LOG_WARNING("red server received unexpected info with msg ~w", [E]),
    {noreply, S}.

%% We would have to add type information to every {_, replica_message} pair so that
%% dialyzer doesn't complain about the second argument being different types
-dialyzer({nowarn_function, handle_request/3}).
-spec handle_request(partition_id(), replica_id() | red_coord_location() | node(), replica_message()) -> ok.
handle_request(Partition, Coordinator, #red_prepare{tx_id=TxId, readset=RS, writeset=WS, snapshot_vc=VC}) ->
    grb_paxos_vnode:prepare({Partition, node()}, TxId, RS, WS, VC, Coordinator);

handle_request(Partition, Coordinator, #red_accept{ballot=Ballot, tx_id=TxId, readset=RS,
                                                   writeset=WS, decision=Vote, prepare_vc=VC}) ->

    grb_paxos_vnode:accept(Partition, Ballot, TxId, RS, WS, Vote, VC, Coordinator);

handle_request(Partition, Node, #red_accept_ack{ballot=Ballot, tx_id=TxId,
                                                  decision=Vote, prepare_vc=PrepareVC}) ->
    MyNode = node(),
    case Node of
        MyNode -> grb_red_coordinator:accept_ack(Partition, Ballot, TxId, Vote, PrepareVC);
        _ -> erpc:cast(Node, grb_red_coordinator, accept_ack, [Partition, Ballot, TxId, Vote, PrepareVC])
    end;

handle_request(Partition, _SourceReplica, #red_decision{ballot=Ballot, tx_id=TxId, decision=Decision, commit_vc=CommitVC}) ->
    grb_paxos_vnode:decide(Partition, Ballot, TxId, Decision, CommitVC);

handle_request(_Partition, Node, #red_already_decided{tx_id=TxId, decision=Vote, commit_vc=CommitVC}) ->
    MyNode = node(),
    case Node of
        MyNode -> grb_red_coordinator:already_decided(TxId, Vote, CommitVC);
        _ -> erpc:cast(Node, grb_red_coordinator, already_decided, [TxId, Vote, CommitVC])
    end;

handle_request(Partition, SourceReplica, #red_heartbeat{ballot=B, heartbeat_id=Id, timestamp=Ts}) ->
    grb_paxos_vnode:accept_heartbeat(Partition, SourceReplica, B, Id, Ts);

handle_request(Partition, _SourceReplica, #red_heartbeat_ack{ballot=B, heartbeat_id=Id, timestamp=Ts}) ->
    grb_red_timer:handle_accept_ack(Partition, B, Id, Ts);

handle_request(Partition, _SourceReplica, #red_heartbeat_decide{ballot=Ballot, heartbeat_id=Id, timestamp=Ts}) ->
    grb_paxos_vnode:decide_heartbeat(Partition, Ballot, Id, Ts).

