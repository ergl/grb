-module(grb_redblue_connection).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("pvc_types/include/sequencer_messages.hrl").

%% API
-export([start_link/3]).
-ignore_xref([start_link/3]).

-export([start_connection/3,
         send_msg/1,
         send_msg/2,
         close/1]).

%% Msgs
-export([send_conflicts/1,
         send_red_commit/5]).

%% Uniform barrier lifting
-export([notify_uniform_barrier/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(OFFLOAD_PID_KILL, offload_pid_kill).
-define(EXPAND_SND_BUF_INTERVAL, 100).
-define(SEND_MSG_FRAME(MsgCall),
    send_msg(
        sequencer_messages:frame(
            ?INTER_DC_SOCK_PACKET_OPT,
            MsgCall
        )
    )
).

-record(state, {
    replica_id :: replica_id(),
    socket :: gen_tcp:socket(),
    pending_buffer = <<>> :: binary(),
    expand_buffer_timer :: reference(),
    offload_pid :: pid(),
    %% Prepare messages queued until the uniform barrier is lifted
    %% Message is already serialized
    pending_prepares :: #{term() => iodata()}
}).

-type t() :: pid().
-export_type([t/0]).

start_link(Ip, Port, Partitions) ->
    gen_server:start_link(?MODULE, [Ip, Port, Partitions], []).

-spec start_connection(MyPartitions :: [partition_id()],
                       Ip :: inet:ip_address(),
                       Port :: inet:port_number()) -> {ok, grb_redblue_connection:t()}
                                                    | {error, term()}.
start_connection(MyPartitions, Ip, Port) ->
    case grb_redblue_connection_sup:start_connection_child(Ip, Port, MyPartitions) of
        {error, Reason} ->
            {error, Reason};

        {ok, Pid} ->
            {ok, Pid}
    end.

-spec send_conflicts(term()) -> ok.
send_conflicts(Conflicts) ->
    ?SEND_MSG_FRAME(sequencer_messages:put_conflicts(Conflicts)).

-spec send_red_commit(partition_id(), term(), tx_label(), vclock(), [{partition_id(), readset(), writeset()}]) -> ok.
send_red_commit(BarrierPartition, TxId, TxLabel, VC, Prepares) ->
    %% Convert the prepares into a neat structure so that the sequencer can
    %% know the partitions
    LoopFun = fun({Partition, Readset, Writeset}, {AccRs, AccWs}) ->
        {
            AccRs#{Partition => Readset},
            AccWs#{Partition => Writeset}
        }
    end,
    {RS, WS} = lists:foldl(LoopFun, {#{}, #{}}, Prepares),
    gen_server:cast(
        grb_redblue_manager:pool_connection(),
        {send_prepare, BarrierPartition, TxId, TxLabel, RS, WS, VC}
    ).

-spec notify_uniform_barrier(grb_redblue_connection:t(), term()) -> ok.
notify_uniform_barrier(Pid, TxId) ->
    gen_server:cast(Pid, {notify_barrier, TxId}).

-spec send_msg(iolist()) -> ok.
send_msg(Msg) ->
    send_msg(grb_redblue_manager:pool_connection(), Msg).

-spec send_msg(t(), iolist()) -> ok.
send_msg(Pid, Msg) ->
    gen_server:cast(Pid, {send, Msg}).

-spec close(t()) -> ok.
close(Pid) ->
    gen_server:cast(Pid, stop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Ip, Port, Partitions]) ->
    Opts = lists:keyreplace(packet, 1, ?INTER_DC_SOCK_OPTS, {packet, raw}),
    case gen_tcp:connect(Ip, Port, Opts) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            ok = establish_connection(Socket, Partitions),
            ok = expand_snd_buf(Socket),
            OffloadPid = erlang:spawn(fun offload_process_start/0),
            {ok, #state{replica_id=grb_dc_manager:replica_id(),
                        socket=Socket,
                        offload_pid=OffloadPid,
                        expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}}
    end.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({send, Msg}, State=#state{socket=Socket}) ->
    ok = gen_tcp:send(Socket, Msg),
    {noreply, State};

handle_cast({send_prepare, Partition, TxId, Label, PRS, PWS, VC}, S0=#state{replica_id=LocalId,
                                                                            socket=Socket}) ->
    Msg = sequencer_messages:frame(
        ?INTER_DC_SOCK_PACKET_OPT,
        sequencer_messages:prepare_request(TxId, Label, PRS, PWS, VC)
    ),
    Timestamp = grb_vclock:get_time(LocalId, VC),
    UniformTimestamp = grb_vclock:get_time(LocalId, grb_propagation_vnode:uniform_vc(Partition)),
    S = if
        Timestamp =< UniformTimestamp ->
            ?LOG_DEBUG("no need to register barrier for ~w", [TxId]),
            ok = gen_tcp:send(Socket, Msg),
            S0;
        true ->
            ?LOG_DEBUG("registering barrier for ~w", [TxId]),
            grb_propagation_vnode:register_red_uniform_barrier(Partition, Timestamp, self(), TxId),
            S0#state{pending_prepares=(S0#state.pending_prepares)#{TxId => Msg}}
    end,
    {noreply, S};

handle_cast({notify_barrier, TxId}, S=#state{socket=Socket, pending_prepares=PendingPrepares0}) ->
    {Msg, PendingPrepares} = maps:take(TxId, PendingPrepares0),
    ok = gen_tcp:send(Socket, Msg),
    {noreply, S#state{pending_prepares=PendingPrepares}};

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket,
                                              offload_pid=OffloadPid,
                                              pending_buffer=Buffer0}) ->

    Buffer = handle_messages(OffloadPid, <<Buffer0/binary, Data/binary>>),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{pending_buffer=Buffer}};

handle_info({tcp_closed, _Socket}, State) ->
    ?LOG_INFO("~p closed, removing reference", [?MODULE]),
    {stop, normal, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    ?LOG_INFO("~p got tcp_error", [?MODULE]),
    {stop, Reason, State};

handle_info(expand_snd_buf, S=#state{expand_buffer_timer=Timer}) ->
    ?CANCEL_TIMER_FAST(Timer),
    ok = expand_snd_buf(S#state.socket),
    {noreply, S#state{expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}};

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{socket=Socket, offload_pid=Pid}) ->
    Pid ! ?OFFLOAD_PID_KILL,
    ok = grb_redblue_manager:connection_closed(self()),
    ok = gen_tcp:close(Socket),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec establish_connection(gen_tcp:socket(), [partition_id()]) -> ok.
establish_connection(Socket, MyPartitions) ->
    gen_tcp:send(Socket, sequencer_messages:frame(?INTER_DC_SOCK_PACKET_OPT, sequencer_messages:ping(MyPartitions))).

%% Expand the send buffer size from time to time.
%% This will eventually settle in a stable state if the sndbuf stops changing.
-spec expand_snd_buf(gen_tcp:socket()) -> ok.
expand_snd_buf(Socket) ->
    case inet:getopts(Socket, [sndbuf]) of
        {error, _} ->
            %% Socket might have closed
            ok;

        {ok, [{sndbuf, SndBuf}]} ->
            case inet:setopts(Socket, [{sndbuf, SndBuf * 2}]) of
                {error, _} ->
                    %% Perhaps there's no room to continue to expand, keep it the way it was
                    inet:setopts(Socket, [{sndbuf, SndBuf}]);
                ok ->
                    ok
            end
    end.

-spec handle_messages(pid(), binary()) -> binary().
handle_messages(_, <<>>) ->
    <<>>;

handle_messages(OffloadPid, Buffer) ->
    case erlang:decode_packet(4, Buffer, []) of
        {ok, Msg, More} ->
            case Msg of
                ?SEQUENCER_MSG(Payload) ->
                    OffloadPid ! {incoming_msg, Payload};
                _ ->
                    %% drop unknown messages on the floor
                    ok
            end,
            handle_messages(OffloadPid, More);
        _ ->
            Buffer
    end.

%% This is an offload process to handle messages incoming from the connection,
%% so that we don't block when we receive.
-spec offload_process_start() -> ok.
offload_process_start() ->
    offload_process_loop().

offload_process_loop() ->
    receive
        {incoming_msg, Msg} ->
            ok = offload_handle_message(sequencer_messages:decode(Msg)),
            offload_process_loop();

        ?OFFLOAD_PID_KILL ->
            erlang:exit(normal)
    end.

-spec offload_handle_message(sequencer_message()) -> ok.
offload_handle_message(#redblue_prepare_response{tx_id=TxId, outcome=OutCome}) ->
    case grb_redblue_manager:take_transaction_promise(TxId) of
        {ok, Promise} ->
            grb_promise:resolve(OutCome, Promise);
        _ ->
            %% FIXME(borja, redblue): Missing coordinator for redblue decision, what do we do in this case?
            ok
    end;

offload_handle_message(#redblue_deliver{timestamp=Ts, transactions=[]}) ->
    %% No transactions, this is a heartbeat
    lists:foreach(
        fun(Partition) ->
            grb_oplog_vnode:handle_red_heartbeat(Partition, Ts)
        end,
        grb_dc_utils:my_partitions()
    );

offload_handle_message(#redblue_deliver{transactions=Transactions}) ->
    MyPartitions = grb_dc_utils:my_partitions(),
    %% Loop over the transaction ids, it has a mapping on partitions to writeset and commit vector
    lists:foreach(
        fun({TxId, PWS, CommitVC}) ->
            lists:foreach(
                fun(Partition) ->
                    case PWS of
                        #{ Partition := WS } ->
                            %% We know we can handle this transaction
                            grb_oplog_vnode:handle_red_transaction_redblue(Partition, TxId, WS, CommitVC);
                        _ ->
                            ok
                    end
                end,
                MyPartitions
            )
        end,
        Transactions
    );

offload_handle_message(_) ->
    %% We don't know how to handle the rest of the messages, forget about them
    ok.
