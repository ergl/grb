-module(grb_dc_connection_sender).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/4]).
-ignore_xref([start_link/4]).

-export([start_connection/4,
         send/2,
         send_framed/2,
         send_process/2,
         send_process_framed/2,
         close/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(state, {
    connected_dc :: replica_id(),
    connected_partition :: partition_id(),
    socket :: gen_tcp:socket()
}).

-record(handle, { pid :: pid(), socket :: gen_tcp:socket() }).
-type t() :: #handle{}.
-export_type([t/0]).

start_link(TargetReplica, Partition, Ip, Port) ->
    gen_server:start_link(?MODULE, [TargetReplica, Partition, Ip, Port], []).

-spec start_connection(replica_id(), partition_id(), inet:ip_address(), inet:port_number()) -> {ok, t()}.
start_connection(TargetReplica, Partition, Ip, Port) ->
    {ok, Pid} = grb_dc_connection_sender_sup:start_connection(TargetReplica, Partition, Ip, Port),
    establish_connection(Pid, Partition).

-spec send(t(), iodata()) -> ok | {error, term()}.
send(#handle{socket=Socket}, Msg) ->
    gen_tcp:send(Socket, grb_dc_messages:frame(Msg)).

-spec send_framed(t(), iolist()) -> ok | {error, term()}.
send_framed(#handle{socket=Socket}, Msg) ->
    gen_tcp:send(Socket, Msg).

-spec send_process(t(), iodata()) -> ok.
send_process(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send, Msg}).

-spec send_process_framed(t(), iolist()) -> ok.
send_process_framed(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send_framed, Msg}).

-spec close(t()) -> ok.
close(#handle{pid=Pid}) ->
    gen_server:cast(Pid, close).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([TargetReplica, Partition, Ip, Port]) ->
    ok = grb_measurements:create_stat({?MODULE, Partition, TargetReplica, message_queue_len}),
    ok = grb_measurements:create_stat({?MODULE, Partition, TargetReplica, send_elapsed}),
    Opts = lists:keyreplace(packet, 1, ?INTER_DC_SOCK_OPTS, {packet, raw}),
    case gen_tcp:connect(Ip, Port, Opts) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            {ok, #state{connected_dc=TargetReplica,
                        connected_partition=Partition,
                        socket=Socket}}
    end.

handle_call(socket, _From, S=#state{socket=Socket}) ->
    {reply, {ok, Socket}, S};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({send, Msg}, S=#state{connected_dc=R, connected_partition=P}) ->
    grb_measurements:log_queue_length({?MODULE, P, R, message_queue_len}),
    ok = send_through_socket(S, grb_dc_messages:frame(Msg)),
    {noreply, S};

handle_cast({send_framed, Msg}, S=#state{connected_dc=R, connected_partition=P}) ->
    grb_measurements:log_queue_length({?MODULE, P, R, message_queue_len}),
    ok = send_through_socket(S, Msg),
    {noreply, S};

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

-ifndef(ENABLE_METRICS).
send_through_socket(#state{socket=Socket}, Data) ->
    ok = gen_tcp:send(Socket, Data).
-else.
send_through_socket(#state{socket=Socket, connected_dc=R, connected_partition=P}, Data) ->
    {Took, ok} = timer:tc(gen_tcp, send, [Socket, Data]),
    ok = grb_measurements:log_stat({?MODULE, P, R, send_elapsed}, Took),
    ok.
-endif.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket, connected_dc=Target}) ->
    ?LOG_INFO("~p: Received unexpected data ~p", [?MODULE, Target, Data]),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S=#state{connected_partition=P, connected_dc=R}) ->
    ?LOG_INFO("~p to ~p:~p got tcp_error", [?MODULE, R, P]),
    {stop, Reason, S};

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{socket=Socket, connected_dc=Replica, connected_partition=Partition}) ->
    ok = gen_tcp:close(Socket),
    ok = grb_dc_connection_manager:connection_closed(Replica, Partition),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec establish_connection(pid(), partition_id()) -> {ok, t()}.
establish_connection(Pid, Partition) ->
    Handle = make_handle(Pid),
    ok = send(Handle, grb_dc_messages:ping(grb_dc_manager:replica_id(), Partition)),
    {ok, Handle}.

-spec make_handle(pid()) -> t().
make_handle(Pid) ->
    {ok, Socket} = gen_server:call(Pid, socket, infinity),
    #handle{pid=Pid, socket=Socket}.
