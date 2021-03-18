-module(grb_causal_sequencer).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% supervision tree
-export([start_link/1]).
-ignore_xref([start_link/1]).

-export([sequence/3]).

%% internal / sys functions
-export([init/2,
         system_continue/3,
         system_terminate/4,
         system_get_state/1]).

%% called from proc_lib / sys
-ignore_xref([init/2,
              system_continue/3,
              system_terminate/4,
              system_get_state/1]).

%% A subset of replica_message/0
-type pending_message() :: #blue_heartbeat{}
                         | #replicate_tx{}
                         | #replicate_tx_4{}
                         | #replicate_tx_8{}
                         | #update_clocks{}
                         | #update_clocks_heartbeat{}
                         | #forward_heartbeat{}
                         | #forward_transaction{}
                         | #update_clocks_cure{}
                         | #update_clocks_cure_heartbeat{}.

-type replica_pending() :: #{ non_neg_integer() => pending_message() }.
-record(state, {
    partition :: partition_id(),
    next_replica_sequence = #{} :: #{ replica_id() => non_neg_integer() },
    replica_pending_messages = #{} :: #{ replica_id() => replica_pending() }
}).

start_link(Partition) ->
    proc_lib:start_link(?MODULE, init, [self(), Partition]).

-spec sequence(partition_id(), replica_id(), pending_message()) -> ok.
sequence(Partition, ConnReplica, Msg) ->
    Pid = persistent_term:get({?MODULE, Partition}),
    Pid ! {sequence, ConnReplica, Msg},
    ok.

-spec init(pid(), partition_id()) -> no_return().
init(Parent, Partition) ->
    process_flag(trap_exit, true),
    ok = persistent_term:put({?MODULE, Partition}, self()),
    Debug = sys:debug_options([]),
    proc_lib:init_ack(Parent, {ok, self()}),
    sequencer_loop(#state{partition=Partition}, Parent, Debug).

-spec sequencer_loop(#state{}, pid(), [term()]) -> no_return().
sequencer_loop(State, Parent, Debug) ->
    receive
        {sequence, FromReplica, Msg} ->
            sequencer_loop(sequence_internal(FromReplica, Msg, State), Parent, Debug);
        {'EXIT', Parent, Reason} ->
            exit(Reason);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, State);
        _ ->
            sequencer_loop(State, Parent, Debug)
    end.

%%%===================================================================
%%% convert incoming record to application message
%%%===================================================================

-spec sequence_internal(replica_id(), pending_message(), #state{}) -> #state{}.
sequence_internal(FromReplica, Msg, State=#state{partition=Partition,
                                                 next_replica_sequence=NextMap,
                                                 replica_pending_messages=PendingMessages}) ->
    SeqNumber = seq_number(Msg),
    case maps:get(FromReplica, NextMap, 0) of
        SeqNumber ->
            %% process message, advance seq, re-process pending
            ok = execute_command(FromReplica, Partition, Msg),
            reprocess_pending(
                FromReplica,
                State#state{next_replica_sequence=NextMap#{FromReplica => SeqNumber + 1}}
            );

        _ ->
            %% Not ready, buffer for later
            ReplicaPending =
                case PendingMessages of
                    #{ FromReplica := ReplicaPending0 } ->
                        ReplicaPending0#{SeqNumber => Msg};
                    #{} ->
                        #{SeqNumber => Msg}
                end,
            State#state{
                replica_pending_messages=PendingMessages#{FromReplica => ReplicaPending}
            }
    end.

%%%===================================================================
%%% util
%%%===================================================================

-spec seq_number(pending_message()) -> non_neg_integer().
seq_number(Rec) ->
    %% All matching records have the seq_number field in the first position, so we can acces it directly.
    element(2, Rec).

-spec execute_command(replica_id(), partition_id(), pending_message()) -> ok.
execute_command(ConnReplica, Partition, #blue_heartbeat{timestamp=Ts}) ->
    grb_vnode_proxy:async_blue_heartbeat(Partition, ConnReplica, Ts);

execute_command(ConnReplica, Partition, #replicate_tx{writeset=WS, commit_vc=VC}) ->
    grb_oplog_vnode:handle_replicate(Partition, ConnReplica, WS, VC);

execute_command(ConnReplica, Partition, #replicate_tx_4{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4}) ->
    grb_oplog_vnode:handle_replicate_array(Partition, ConnReplica, Tx1, Tx2, Tx3, Tx4);

execute_command(ConnReplica, Partition, #replicate_tx_8{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4,
                                                        tx_5=Tx5, tx_6=Tx6, tx_7=Tx7, tx_8=Tx8}) ->
    grb_oplog_vnode:handle_replicate_array(Partition, ConnReplica, Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8);

execute_command(ConnReplica, Partition, #update_clocks{known_vc=KnownVC, stable_vc=StableVC}) ->
    grb_propagation_vnode:handle_clock_update(Partition, ConnReplica, KnownVC, StableVC);

execute_command(ConnReplica, Partition, #update_clocks_heartbeat{known_vc=KnownVC, stable_vc=StableVC}) ->
    grb_propagation_vnode:handle_clock_heartbeat_update(Partition, ConnReplica, KnownVC, StableVC);

execute_command(_, Partition, #forward_heartbeat{replica=SourceReplica, timestamp=Ts}) ->
    grb_vnode_proxy:async_blue_heartbeat(Partition, SourceReplica, Ts);

execute_command(_, Partition, #forward_transaction{replica=SourceReplica, writeset=WS, commit_vc=VC}) ->
    grb_oplog_vnode:handle_replicate(Partition, SourceReplica, WS, VC);

execute_command(ConnReplica, Partition, #update_clocks_cure{known_vc=KnownVC}) ->
    grb_propagation_vnode:handle_clock_update(Partition, ConnReplica, KnownVC);

execute_command(ConnReplica, Partition, #update_clocks_cure_heartbeat{known_vc=KnownVC}) ->
    grb_propagation_vnode:handle_clock_heartbeat_update(Partition, ConnReplica, KnownVC).

-spec reprocess_pending(replica_id(), #state{}) -> #state{}.
reprocess_pending(FromReplica, S=#state{partition=Partition,
                                        next_replica_sequence=NextMap,
                                        replica_pending_messages=Pending}) ->

    case Pending of
        #{ FromReplica := PendingMessages } ->
            {N, ReplicaMessages} =
                reprocess_pending(
                    FromReplica,
                    Partition,
                    maps:get(FromReplica, NextMap),
                    PendingMessages
                ),
            S#state{
                next_replica_sequence=NextMap#{FromReplica => N},
                replica_pending_messages=Pending#{FromReplica => ReplicaMessages}
            };

        #{} ->
            S
    end.

-spec reprocess_pending(FromReplica :: replica_id(),
                        Partition :: partition_id(),
                        N0 :: non_neg_integer(),
                        Messages0 :: replica_pending()) -> {N :: non_neg_integer(), Messages :: replica_pending()}.
reprocess_pending(FromReplica, Partition, N, Messages0) ->
    case maps:take(N, Messages0) of
        error ->
            {N, Messages0};
        {Msg, Messages} ->
            ok = execute_command(FromReplica, Partition, Msg),
            reprocess_pending(FromReplica, Partition, N + 1, Messages)
    end.

%%%===================================================================
%%% stub proc_lib callbacks
%%%===================================================================

system_continue(Parent, Debug, State) ->
    sequencer_loop(State, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

system_get_state(State) ->
    {ok, State}.
