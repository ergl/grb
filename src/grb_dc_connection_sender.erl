-module(grb_dc_connection_sender).
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

-ifndef(ENABLE_METRICS).
-define(report_busy_elapsed(S), begin _ = S, ok end).
-else.
-define(report_busy_elapsed(S),
    grb_measurements:log_stat(
        {?MODULE, S#state.connected_partition, S#state.connected_dc, busy_elapsed},
        erlang:convert_time_unit(erlang:monotonic_time() - (S#state.busy_ts), native, microsecond)
    )).
-endif.

-ifdef(ENABLE_METRICS).
-record(state, {
    connected_dc :: replica_id(),
    connected_partition :: partition_id(),
    socket :: socket:socket(),
    gen_tcp_socket :: gen_tcp:socket(),
    busy = false :: false | {true, reference()},
    busy_ts = undefined :: non_neg_integer() | undefined,
    pending_to_send = [] :: iodata()
}).
-else.
-record(state, {
    connected_dc :: replica_id(),
    connected_partition :: partition_id(),
    socket :: socket:socket(),
    gen_tcp_socket :: gen_tcp:socket(),
    busy = false :: false | {true, reference()},
    pending_to_send = [] :: iodata()
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
    gen_server:cast(Pid, {send, grb_dc_messages:frame(Msg)}).

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
    Opts = lists:keyreplace(packet, 1, ?INTER_DC_SOCK_OPTS, {packet, raw}),
    case gen_tcp:connect(Ip, Port, [{inet_backend, socket} | Opts]) of
        {error, Reason} ->
            {stop, Reason};
        {ok, GenSocket=?raw_socket(Socket)} ->
            {ok, #state{connected_dc=TargetReplica,
                        connected_partition=Partition,
                        socket=Socket,
                        gen_tcp_socket=GenSocket}}
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

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info({'$socket', Socket, select, Ref}, S=#state{socket=Socket, busy={true, Ref}, pending_to_send=[]}) ->
    ?report_busy_elapsed(S),
    {noreply, S#state{busy=false}};

handle_info({'$socket', Socket, select, Ref}, S0=#state{socket=Socket, busy={true, Ref}, pending_to_send=Pending}) ->
    ?report_busy_elapsed(S0),
    %% Go through backlog first
    {ok, S} = socket_send(S0#state{busy=false, pending_to_send=[]}, Pending),
    {noreply, S};

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
                             busy_ts=erlang:monotonic_time(),
                             pending_to_send=[More | Pending]}};

        {select, {select_info, _, Ref}} ->
            ok = grb_measurements:log_counter(?busy_socket(State)),
            %% Socket is busy, queue data and send later
            {ok, State#state{busy={true, Ref},
                             busy_ts=erlang:monotonic_time(),
                             pending_to_send=[Data | Pending]}};

        {error, Reason} ->
            {error, Reason}
    end.
-endif.
