-module(grb_dc_connection_sender_socket).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/4]).
-ignore_xref([start_link/4]).

-export([start_connection/4,
         send_process/2,
         send_process_framed/2,
         close/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(raw_socket(S),
        {'$inet', gen_tcp_socket, {_, S}}).

-define(busy_socket(S),
    {?MODULE, S#state.connected_partition, S#state.connected_dc, busy_socket}).

-define(queue_data(S),
    {?MODULE, S#state.connected_partition, S#state.connected_dc, queue_data}).

-define(EXPAND_SND_BUF_INTERVAL, 100).

-ifndef(ENABLE_METRICS).
-define(report_busy_elapsed(S), S).
-define(report_pending_size(S), begin _ = S, ok end).
-else.
-define(report_busy_elapsed(__S),
    begin
        case __S#state.busy_ts of
            undefined ->
                __S;
            _ ->
                grb_measurements:log_stat(
                    {?MODULE, __S#state.connected_partition, __S#state.connected_dc, busy_elapsed},
                    grb_time:diff_native(grb_time:timestamp(), __S#state.busy_ts)
                ),
                __S#state{busy_ts=undefined}
        end
    end).
-define(report_pending_size(__S),
    begin
        grb_measurements:log_stat(
            {?MODULE, __S#state.connected_partition, __S#state.connected_dc, pending_queue_len},
            erlang:length(__S#state.pending_to_send)
        ),
        grb_measurements:log_stat(
            {?MODULE, __S#state.connected_partition, __S#state.connected_dc, pending_queue_bytes},
            erlang:iolist_size(__S#state.pending_to_send)
        )
    end).
-endif.

-ifdef(ENABLE_METRICS).
-record(state, {
    connected_dc :: replica_id(),
    connected_partition :: partition_id(),
    socket :: socket:socket(),
    gen_tcp_socket :: gen_tcp:socket(),
    busy = false :: false | {true, socket:select_ref()},
    busy_ts = undefined :: non_neg_integer() | undefined,
    pending_to_send = [] :: iodata(),
    expand_buffer_timer :: reference()
}).
-else.
-record(state, {
    connected_dc :: replica_id(),
    connected_partition :: partition_id(),
    socket :: socket:socket(),
    gen_tcp_socket :: gen_tcp:socket(),
    busy = false :: false | {true, socket:select_ref()},
    pending_to_send = [] :: iodata(),
    expand_buffer_timer :: reference()
}).
-endif.

-record(handle, { pid :: pid()  }).
-type t() :: #handle{}.
-export_type([t/0]).

start_link(TargetReplica, Partition, Ip, Port) ->
    gen_server:start_link(?MODULE, [TargetReplica, Partition, Ip, Port], []).

-spec start_connection(replica_id(), partition_id(), inet:ip_address(), inet:port_number()) -> {ok, t()}.
start_connection(TargetReplica, Partition, Ip, Port) ->
    {ok, Pid} = grb_dc_connection_sender_sup:start_connection(TargetReplica, Partition, Ip, Port),
    establish_connection(Pid, Partition).

-spec send_process(t(), iodata()) -> ok.
send_process(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send_needs_framing, Msg}).

-spec send_process_framed(t(), iolist()) -> ok.
send_process_framed(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send, Msg}).

-spec close(t()) -> ok.
close(#handle{pid=Pid}) ->
    gen_server:cast(Pid, close).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([TargetReplica, Partition, Ip, Port]) ->
    ok = grb_measurements:create_stat({?MODULE, Partition, TargetReplica, message_queue_len}),
    ok = grb_measurements:create_stat({?MODULE, Partition, TargetReplica, busy_elapsed}),
    ok = grb_measurements:create_stat({?MODULE, Partition, TargetReplica, pending_queue_len}),
    ok = grb_measurements:create_stat({?MODULE, Partition, TargetReplica, pending_queue_bytes}),
    Opts = lists:keyreplace(packet, 1, ?INTER_DC_SOCK_OPTS, {packet, raw}),
    case gen_tcp:connect(Ip, Port, [{inet_backend, socket} | Opts]) of
        {error, Reason} ->
            {stop, Reason};
        {ok, GenSocket=?raw_socket(Socket)} ->
            ok = expand_snd_buf(Socket),
            {ok, #state{connected_dc=TargetReplica,
                        connected_partition=Partition,
                        socket=Socket,
                        gen_tcp_socket=GenSocket,
                        expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}}
    end.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({send, Msg}, S=#state{busy={true, _}, pending_to_send=Pending}) ->
    ok = grb_measurements:log_counter(?queue_data(S)),
    {noreply, S#state{pending_to_send=[Pending, Msg]}};

handle_cast({send, Msg}, S0=#state{busy=false}) ->
    {ok, S} = socket_send(S0, Msg),
    {noreply, S};

handle_cast({send_needs_framing, Msg}, S=#state{busy={true, _}, pending_to_send=Pending}) ->
    ok = grb_measurements:log_counter(?queue_data(S)),
    {noreply, S#state{pending_to_send=[Pending, grb_dc_messages:frame(Msg)]}};

handle_cast({send_needs_framing, Msg}, S0=#state{busy=false}) ->
    {ok, S} = socket_send(S0, grb_dc_messages:frame(Msg)),
    {noreply, S};

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info({'$socket', Socket, select, Ref}, S=#state{socket=Socket, busy={true, Ref}, pending_to_send=[]}) ->
    S1 = ?report_busy_elapsed(S),
    {noreply, S1#state{busy=false}};

handle_info({'$socket', Socket, select, Ref}, S0=#state{socket=Socket, busy={true, Ref}, pending_to_send=Pending}) ->
    S1 = ?report_busy_elapsed(S0),
    ?report_pending_size(S1),
    %% Go through backlog first
    {ok, S2} = socket_send(S1#state{busy=false, pending_to_send=[]}, Pending),
    {noreply, S2};

handle_info({'$socket', Socket, abort, {Ref, closed}}, S=#state{socket=Socket, busy={true, Ref}}) ->
    {stop, normal, S#state{busy=false}};

handle_info({tcp, GenSocket, Data}, State=#state{gen_tcp_socket=GenSocket, connected_dc=Target}) ->
    ?LOG_INFO("~p: Received unexpected data ~p", [?MODULE, Target, Data]),
    ok = inet:setopts(GenSocket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S=#state{connected_partition=P, connected_dc=R}) ->
    ?LOG_INFO("~p to ~p:~p got tcp_error", [?MODULE, R, P]),
    {stop, Reason, S};

handle_info(expand_snd_buf, S=#state{expand_buffer_timer=Timer}) ->
    ?CANCEL_TIMER_FAST(Timer),
    ok = expand_snd_buf(S#state.socket),
    {noreply, S#state{expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}};

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{gen_tcp_socket=GenSocket, connected_dc=Replica, connected_partition=Partition}) ->
    ok = gen_tcp:close(GenSocket),
    ok = grb_dc_connection_manager:connection_closed(Replica, Partition),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec establish_connection(pid(), partition_id()) -> {ok, t()}.
establish_connection(Pid, Partition) ->
    Handle = #handle{pid=Pid},
    ok = send_process(Handle, grb_dc_messages:ping(grb_dc_manager:replica_id(), Partition)),
    {ok, Handle}.


-spec socket_send(#state{}, iodata()) -> {ok, #state{}} | {error, term()}.
-ifndef(ENABLE_METRICS).
socket_send(State=#state{socket=Socket,
                         pending_to_send=Pending}, Data) ->
    case socket:send(Socket, Data, [], nowait) of
        ok ->
            {ok, State};

        {ok, {More, {select_info, _, Ref}}} ->
            %% If we couldn't send everything, queue it in the front
            {ok, State#state{busy={true, Ref},
                             pending_to_send=[More | Pending]}};

        {select, {select_info, _, Ref}} ->
            %% Socket is busy, queue data and send later
            {ok, State#state{busy={true, Ref},
                             pending_to_send=[Data | Pending]}};

        {error, Reason} ->
            {error, Reason}
    end.
-else.
socket_send(State=#state{socket=Socket,
                         pending_to_send=Pending}, Data) ->
    case socket:send(Socket, Data, [], nowait) of
        ok ->
            {ok, State};

        {ok, {More, {select_info, _, Ref}}} ->
            ok = grb_measurements:log_counter(?busy_socket(State)),
            %% If we couldn't send everything, queue it in the front
            {ok, State#state{busy={true, Ref},
                             busy_ts=grb_time:timestamp(),
                             pending_to_send=[More | Pending]}};

        {select, {select_info, _, Ref}} ->
            ok = grb_measurements:log_counter(?busy_socket(State)),
            %% Socket is busy, queue data and send later
            {ok, State#state{busy={true, Ref},
                             busy_ts=grb_time:timestamp(),
                             pending_to_send=[Data | Pending]}};

        {error, Reason} ->
            {error, Reason}
    end.
-endif.

%% Expand the send buffer size from time to time.
%% This will eventually settle in a stable state if the sndbuf stops changing.
-spec expand_snd_buf(socket:socket()) -> ok.
expand_snd_buf(Socket) ->
    {ok, SndBuf} = socket:getopt(Socket, socket, sndbuf),
    case socket:setopt(Socket, socket, sndbuf, SndBuf * 2) of
        {error, _} ->
            %% Perhaps there's no room to continue to expand, keep it the way it was
            ok = socket:setopt(Socket, socket, sndbuf, SndBuf);
        ok ->
            ok
    end.
