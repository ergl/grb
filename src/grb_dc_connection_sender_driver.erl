-module(grb_dc_connection_sender_driver).
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

-define(EXPAND_SND_BUF_INTERVAL, 100).

-record(state, {
    connected_dc :: replica_id(),
    connected_partition :: partition_id(),
    socket :: gen_tcp:socket(),
    expand_buffer_timer :: reference()
}).

-record(handle, { pid :: pid()  }).
-type t() :: #handle{}.
-export_type([t/0]).

start_link(TargetReplica, Partition, Ip, Port) ->
    gen_server:start_link(?MODULE, [TargetReplica, Partition, Ip, Port], []).

-spec start_connection(replica_id(), partition_id(), inet:ip_address(), inet:port_number()) -> {ok, t()}.
start_connection(TargetReplica, Partition, Ip, Port) ->
    {ok, Pid} = grb_dc_connection_sender_sup:start_connection_child(TargetReplica, Partition, Ip, Port),
    establish_connection(Pid, Partition).

-spec send_process(t(), iodata()) -> ok.
send_process(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send_needs_framing, Msg}).

-spec send_process_framed(t(), iolist()) -> ok.
send_process_framed(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send, Msg}).

-spec close(t()) -> ok.
close(#handle{pid=Pid}) ->
    gen_server:cast(Pid, stop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([TargetReplica, Partition, Ip, Port]) ->
    Opts = lists:keyreplace(packet, 1, ?INTER_DC_SOCK_OPTS, {packet, raw}),
    case gen_tcp:connect(Ip, Port, Opts) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            ok = expand_snd_buf(Socket),
            {ok, #state{connected_dc=TargetReplica,
                        connected_partition=Partition,
                        socket=Socket,
                        expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}}
    end.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({send, Msg}, State=#state{socket=Socket}) ->
    ok = gen_tcp:send(Socket, Msg),
    {noreply, State};

handle_cast({send_needs_framing, Msg}, State=#state{socket=Socket}) ->
    ok = gen_tcp:send(Socket, grb_dc_messages:frame(Msg)),
    {noreply, State};

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket, connected_dc=Target}) ->
    ?LOG_INFO("~p: Received unexpected data ~p", [?MODULE, Target, Data]),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S=#state{connected_partition=P, connected_dc=R}) ->
    ?LOG_INFO("Connection lost to ~p (~p), removing reference", [R, P]),
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

terminate(_Reason, #state{socket=Socket, connected_dc=Replica, connected_partition=Partition}) ->
    ok = gen_tcp:close(Socket),
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

%% Expand the send buffer size from time to time.
%% This will eventually settle in a stable state if the sndbuf stops changing.
-spec expand_snd_buf(gen_tcp:socket()) -> ok.
expand_snd_buf(Socket) ->
    {ok, [{sndbuf, SndBuf}]} = inet:getopts(Socket, [sndbuf]),
    case inet:setopts(Socket, [{sndbuf, SndBuf * 2}]) of
        {error, _} ->
            %% Perhaps there's no room to continue to expand, keep it the way it was
            inet:setopts(Socket, [{sndbuf, SndBuf}]);
        ok ->
            ok
    end.
