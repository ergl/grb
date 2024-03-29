-module(grb_red_coordinator).
-behavior(gen_server).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% Called by erpc / debug
-ignore_xref([start_link/1,
              accept_ack/6,
              already_decided/3]).

%% supervision tree
-export([start_link/1]).

-export([commit/7,
         commit_send/2,
         already_decided/3,
         already_decided/4,
         accept_ack/6,
         accept_ack/7]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-ifndef(ENABLE_METRICS).
-define(ADD_TARGET_PARTITION(__Id, __P), begin _ = __Id, _ = __P, ok end).
-define(ADD_SENT_TS(__Id), begin _ = __Id, ok end).
-define(ADD_ACK_TS(__Replica, __Partition, __Id),
    begin
        _ = __Replica,
        _ = __Partition,
        _ = __Id,
        ok
    end).
-define(ADD_DECISION_TS(__Id), begin _ = __Id, ok end).
-define(CLEANUP_TS(__Id), begin _ = __Id, ok end).
-else.
-define(ADD_TARGET_PARTITION(__Id, __P),
    begin
        ets:insert(grb_red_manager:metrics_table(), {{__Id, partition}, __P}),
        ok
    end).

-define(ADD_SENT_TS(__Id),
    begin
        ets:insert(grb_red_manager:metrics_table(), {{__Id, sent_ts}, grb_time:timestamp()}),
        ok
    end).

-define(ADD_ACK_TS(__Replica, __Partition, __Id),
    try
        __Now = grb_time:timestamp(),
        __TRef = grb_red_manager:metrics_table(),
        __PTS = ets:lookup_element(__TRef, {__Id, sent_ts}, 2),

        %% use insert_new so that it will never be overwritten afterwards
        case ets:insert_new(__TRef, {{__Id, first_ack}, __Now}) of
            true ->
                grb_measurements:log_stat({?MODULE, __Partition, sent_to_first_ack}, grb_time:diff_native(__Now, __PTS));
            false ->
                ok
        end,

        ok = grb_measurements:log_stat({?MODULE, __Partition, __Replica, sent_to_ack},
                                        grb_time:diff_native(__Now, __PTS)),
        ok
    catch _:_ ->
        ok
    end).

-define(ADD_DECISION_TS(__Id),
    try
        __Now = grb_time:timestamp(),
        __TRef = grb_red_manager:metrics_table(),
        __Partition = ets:lookup_element(__TRef, {__Id, partition}, 2),
        __PTS = ets:lookup_element(__TRef, {__Id, sent_ts}, 2),
        grb_measurements:log_stat({?MODULE, __Partition, sent_to_decision}, grb_time:diff_native(__Now, __PTS)),
        ok
    catch _:_ ->
        ok
    end).

-define(CLEANUP_TS(__Id),
    begin
        ets:select_delete(grb_red_manager:metrics_table(), [{ {{__Id, '_'}, '_'}, [], [true] } ]),
        ok
    end).

-endif.

-type partition_ballots() :: #{partition_id() => ballot()}.
-record(tx_acc, {
    promise :: grb_promise:t(),
    locations = [] :: [{partition_id(), leader_location()}],
    quorums_to_ack = #{} :: #{partition_id() => pos_integer()},
    ballots = #{} :: partition_ballots(),
    accumulator = #{} :: #{partition_id() => {red_vote(), grb_time:ts()}},
    snapshot_vc :: vclock(),

    %% pending data, hold the data while we wait for the uniform barrier to lift
    pending_label = undefined :: tx_label() | undefined,
    pending_prepares = undefined :: [{partition_id(), readset(), writeset()}] | undefined
}).

-record(state, {
    self_pid :: pid(),
    replica :: replica_id(),
    self_location :: red_coord_location(),
    quorum_size :: non_neg_integer(),
    accumulators = #{} :: #{term() => #tx_acc{}}
}).

-spec start_link(non_neg_integer()) -> {ok, pid()}.
start_link(Id) ->
    Name = {local, generate_coord_name(Id)},
    gen_server:start_link(Name, ?MODULE, [Id], []).

-spec generate_coord_name(non_neg_integer()) -> atom().
generate_coord_name(Id) ->
    BinId = integer_to_binary(Id),
    grb_dc_utils:safe_bin_to_atom(<<"grb_red_coordinator_", BinId/binary>>).

-spec commit(Coordinator :: red_coordinator(),
             Promise :: grb_promise:t(),
             TargetPartition :: partition_id(),
             TxId :: term(),
             Label :: tx_label(),
             SnapshotVC :: vclock(),
             Prepares :: [{partition_id(), readset(), writeset()}]) -> ok.

%% todo(borja, recovery): We should send the entire writeset to every partition.
commit(Coordinator, Promise, TargetPartition, TxId, Label, SnapshotVC, Prepares) ->
    gen_server:cast(Coordinator, {commit_init, Promise, TargetPartition, TxId, Label, SnapshotVC, Prepares}).

-spec commit_send(red_coordinator(), term()) -> ok.
commit_send(Coordinator, TxId) ->
    gen_server:cast(Coordinator, {commit_send, TxId}).

-spec already_decided(term(), red_vote(), vclock()) -> ok.
already_decided(TxId, Vote, VoteVC) ->
    grb_measurements:log_counter({?MODULE, ?FUNCTION_NAME}),
    case grb_red_manager:transaction_coordinator(TxId) of
        error -> ok;
        {ok, Coordinator} ->
            %% this might happen in multi-partition transactions. It is enough for
            %% one of the leaders to reply, so ignore the rest
            gen_server:cast(Coordinator, {already_decided, TxId, Vote, VoteVC})
    end.

-spec already_decided(node(), term(), red_vote(), vclock()) -> ok.
already_decided(Node, TxId, Vote, VoteVC) when Node =:= node() ->
    already_decided(TxId, Vote, VoteVC);

already_decided(Node, TxId, Vote, VoteVC) ->
    grb_dc_utils:send_cast(Node, ?MODULE, already_decided, [TxId, Vote, VoteVC]).

-spec accept_ack(replica_id(), partition_id(), ballot(), term(), red_vote(), grb_time:ts()) -> ok.
accept_ack(SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs) ->
    ?ADD_ACK_TS(SenderReplica, Partition, TxId),
    case grb_red_manager:transaction_coordinator(TxId) of
        error ->
            %% This only works for f=1
            ?CLEANUP_TS(TxId),
            ok;

        {ok, Coordinator} ->
            gen_server:cast(Coordinator, {accept_ack, Partition, Ballot, TxId, Vote, AcceptTs})
    end.

-spec accept_ack(node(), replica_id(), partition_id(), ballot(), term(), red_vote(), grb_time:ts()) -> ok.
-ifndef(ENABLE_METRICS).
accept_ack(Node, SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs) when Node =:= node() ->
    accept_ack(SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs);

accept_ack(Node, SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs) ->
    grb_dc_utils:send_cast(Node, ?MODULE, accept_ack, [SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs]).
-else.
accept_ack({SentTs, Node}, SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs) ->
    Elapsed = grb_time:diff_native(grb_time:timestamp(), SentTs),
    grb_measurements:log_stat({?MODULE, Partition, SenderReplica, ack_in_flight}, Elapsed),
    if
        Node =:= node() ->
            accept_ack(SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs);
        true ->
            ok = grb_measurements:log_counter({?MODULE, Partition, SenderReplica, fwd_accept_ack}),
            grb_dc_utils:send_cast(Node, ?MODULE, accept_ack, [SenderReplica, Partition, Ballot, TxId, Vote, AcceptTs])
    end.
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_WorkerArgs) ->
    process_flag(trap_exit, true),
    LocalId = grb_dc_manager:replica_id(),
    CoordId = {coord, LocalId, node()},
    QuorumSize = grb_red_manager:quorum_size(),
    {ok, #state{self_pid=self(),
                replica=LocalId,
                self_location=CoordId,
                quorum_size=QuorumSize}}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({commit_init, Promise, Partition, TxId, Label, SnapshotVC, Prepares}, S0=#state{self_pid=Pid, replica=LocalId}) ->
    ?ADD_TARGET_PARTITION(TxId, Partition),
    Timestamp = grb_vclock:get_time(LocalId, SnapshotVC),
    UniformTimestamp = grb_vclock:get_time(LocalId, grb_propagation_vnode:uniform_vc(Partition)),
    S = case Timestamp =< UniformTimestamp of
        true ->
            ?LOG_DEBUG("no need to register barrier for ~w", [TxId]),
            init_tx_and_send(Promise, TxId, Label, SnapshotVC, Prepares, S0);
        false ->
            ?LOG_DEBUG("registering barrier for ~w", [TxId]),
            ok = grb_measurements:log_counter({?MODULE, Partition, pre_commit_barrier}),
            grb_propagation_vnode:register_red_uniform_barrier(Partition, Timestamp, Pid, TxId),
            init_tx(Promise, TxId, Label, SnapshotVC, Prepares, S0)
    end,
    {noreply, S};

handle_cast({commit_send, TxId}, S) ->
    {noreply, send_tx_prepares(TxId, S)};

handle_cast({already_decided, TxId, Vote, VoteVC}, S0=#state{self_pid=Pid, accumulators=Acc0}) ->
    S = case maps:take(TxId, Acc0) of
        error ->
            ?LOG_DEBUG("missed ALREADY_DECIDED(~p, ~p)", [TxId, Vote]),
            S0;
        {#tx_acc{promise=Promise}, Acc} ->
            ?LOG_DEBUG("~p already decided", [TxId]),
            reply_to_client({Vote, VoteVC}, Promise),
            ok = grb_red_manager:unregister_coordinator(TxId, Pid),
            S0#state{accumulators=Acc}
    end,
    {noreply, S};

handle_cast({accept_ack, From, Ballot, TxId, Vote, AcceptTs}, S0=#state{self_pid=Pid,
                                                                        accumulators=TxAcc}) ->
    S = case maps:get(TxId, TxAcc, undefined) of
        undefined ->
            ?LOG_DEBUG("missed ACCEPT_ACK(~b, ~p, ~p) from ~p", [Ballot, TxId, Vote, From]),
            ?CLEANUP_TS(TxId),
            S0;
        TxState ->
            S0#state{accumulators=handle_ack(Pid, From, Ballot, TxId, Vote, AcceptTs, TxAcc, TxState)}
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_tx(Promise, TxId, Label, SnapshotVC, Prepares, S=#state{accumulators=Acc}) ->
    S#state{accumulators=Acc#{TxId => #tx_acc{promise=Promise,
                                              snapshot_vc=SnapshotVC,
                                              pending_label=Label,
                                              pending_prepares=Prepares}}}.

init_tx_and_send(Promise, TxId, Label, SnapshotVC, Prepares, S=#state{self_location=SelfCoord,
                                                                      quorum_size=QuorumSize,
                                                                      accumulators=Acc}) ->

    ?ADD_SENT_TS(TxId),
    {LeaderLocations, QuorumsToAck} = send_loop(SelfCoord, TxId, Label, SnapshotVC, QuorumSize, Prepares),
    S#state{accumulators=Acc#{TxId => #tx_acc{promise=Promise,
                                              snapshot_vc=SnapshotVC,
                                              locations=LeaderLocations,
                                              quorums_to_ack=QuorumsToAck}}}.

send_tx_prepares(TxId, S=#state{self_location=SelfCoord,
                                quorum_size=QuorumSize,
                                accumulators=Acc}) ->

    TxData = #tx_acc{pending_label=Label,
                     pending_prepares=Prepares,
                     snapshot_vc=SnapshotVC} = maps:get(TxId, Acc),

    ?ADD_SENT_TS(TxId),
    {LeaderLocations, QuorumsToAck} = send_loop(SelfCoord, TxId, Label, SnapshotVC, QuorumSize, Prepares),
    S#state{accumulators=Acc#{TxId => TxData#tx_acc{pending_label=undefined,
                                                    pending_prepares=undefined,
                                                    locations=LeaderLocations,
                                                    quorums_to_ack=QuorumsToAck}}}.

-spec send_loop(Coordinator :: red_coord_location(),
                TxId :: term(),
                _Label :: tx_label(),
                PrepareVC :: vclock(),
                QuorumSize :: non_neg_integer(),
                Prepares :: [{partition_id(), readset(), writeset()}]) -> { [{partition_id(), leader_location()}],
                                                                            #{partition_id() => pos_integer()} }.

send_loop(Coordinator, TxId, Label, PrepareVC, QuorumSize, Prepares) ->
    SendFun = fun({Partition, Readset, Writeset}, {LeaderAcc, QuorumAcc}) ->
        Location = send_prepare(Coordinator, Partition, TxId, Label, Readset, Writeset, PrepareVC),
        {
            [{Partition, Location} | LeaderAcc],
            QuorumAcc#{Partition => QuorumSize}
        }
    end,
    lists:foldl(SendFun, {[], #{}}, Prepares).

-spec check_ballot(partition_id(), ballot(), partition_ballots()) -> {ok, partition_ballots()} | error.
check_ballot(Partition, Ballot, Ballots) ->
    case maps:get(Partition, Ballots, undefined) of
        undefined -> {ok, Ballots#{Partition => Ballot}};
        Ballot -> {ok, Ballots};
        _ -> error
    end.

%% for each {partition, readset, writeset} in Prepares, check if the leader
%% for partition is in the local cluster.
%% - If it is, use riak_core to send a message to the vnode.
%% - If it isn't, use grb_dc_connection_manager to locate the leader.
%%   It might be that we don't have an active connection to the leader
%%   (for example, we're trying to commit at partition P0, but we don't
%%   own it, so we won't have an active connection to any node that owns P0).
%%   In that case, we will need to go through a proxy located at the local cluster
%%   node that owns P0.
-spec send_prepare(red_coord_location(), partition_id(), term(), tx_label(), readset(), writeset(), vclock()) -> leader_location().
send_prepare(Coordinator, Partition, TxId, Label, RS, WS, VC) ->
    LeaderLoc = grb_red_manager:leader_of(Partition),
    case LeaderLoc of
        {local, IndexNode} ->
            %% leader is in the local cluster, go through vnode directly
            ok = grb_paxos_vnode:prepare(IndexNode, TxId, Label, RS, WS, VC, Coordinator);
        {remote, RemoteReplica} ->
            %% leader is in another replica, and we have a direct inter_dc connection to it
            ok = grb_dc_connection_manager:send_red_prepare(RemoteReplica, Coordinator, Partition,
                                                            TxId, Label, RS, WS, VC);
        {proxy, LocalNode, RemoteReplica} ->
            ok = grb_measurements:log_counter({?MODULE, Partition, RemoteReplica, fwd_send_prepare}),
            %% leader is in another replica, but we don't have a direct inter_dc connection, have
            %% to go through a cluster-local proxy at `LocalNode`
            Msg = grb_dc_messages:frame(grb_dc_messages:red_prepare(Coordinator, TxId, Label, RS, WS, VC)),
            grb_dc_utils:send_cast(LocalNode, grb_dc_connection_manager, send_raw_framed, [RemoteReplica, Partition, Msg])
    end,
    LeaderLoc.

-spec handle_ack(SelfPid :: pid(),
                 FromPartition :: partition_id(),
                 Ballot :: ballot(),
                 TxId :: term(),
                 Vote :: red_vote(),
                 AcceptTs :: grb_time:ts(),
                 TxAcc0 :: #{term() => #tx_acc{}},
                 TxState :: #tx_acc{}) -> TxAcc :: #{term() => #tx_acc{}}.

handle_ack(SelfPid, FromPartition, Ballot, TxId, Vote, AcceptTs, TxAcc0, TxState) ->
    #tx_acc{ballots=Ballots0, quorums_to_ack=Quorums0, accumulator=Acc0} = TxState,

    {ok, Ballots} = check_ballot(FromPartition, Ballot, Ballots0),
    Acc = Acc0#{FromPartition => {Vote, AcceptTs}},
    ?LOG_DEBUG("ACCEPT_ACK(~b, ~p) from ~p", [Ballot, TxId, FromPartition]),
    Quorums = case maps:get(FromPartition, Quorums0, undefined) of
        %% we already received a quorum from this partition, and we removed it
        undefined -> Quorums0;
        1 -> maps:remove(FromPartition, Quorums0);
        ToAck when is_integer(ToAck) -> Quorums0#{FromPartition => ToAck - 1}
    end,
    case map_size(Quorums) of
        N when N > 0 ->
            TxAcc0#{TxId => TxState#tx_acc{quorums_to_ack=Quorums, accumulator=Acc, ballots=Ballots}};
        0 ->
            ?ADD_DECISION_TS(TxId),
            {Decision, CommitTs} = decide_transaction(Acc),
            reply_to_client(
                {Decision, grb_vclock:set_time(?RED_REPLICA, CommitTs, TxState#tx_acc.snapshot_vc)},
                TxState#tx_acc.promise
            ),

            lists:foreach(fun({Partition, Location}) ->
                Ballot = maps:get(Partition, Ballots),
                send_decision(Partition, Location, Ballot, TxId, Decision, CommitTs)
            end, TxState#tx_acc.locations),

            ok = grb_red_manager:unregister_coordinator(TxId, SelfPid),
            maps:remove(TxId, TxAcc0)
    end.

-spec decide_transaction(#{partition_id() => {red_vote(), grb_time:ts()}}) -> {red_vote(), grb_time:ts()}.
decide_transaction(VoteMap) ->
    maps:fold(
        fun
            (_, {Vote, Ts}, undefined) -> {Vote, Ts};
            (_, {Vote, Ts}, TsAcc) -> reduce_vote(Vote, Ts, TsAcc)
        end,
        undefined,
        VoteMap
    ).

-spec send_decision(partition_id(), leader_location(), ballot(), term(), red_vote(), grb_vclock:ts()) -> ok.
send_decision(Partition, LeaderLoc, Ballot, TxId, Decision, CommitTs) ->
    case LeaderLoc of
        {local, IndexNode} ->
            %% leader is in the local cluster, go through vnode directly
            ok = grb_paxos_vnode:decide(IndexNode, Ballot, TxId, Decision, CommitTs);
        {remote, RemoteReplica} ->
            %% leader is in another replica, and we have a direct inter_dc connection to it
            ok = grb_dc_connection_manager:send_red_decision(RemoteReplica, Partition, Ballot,
                                                             TxId, Decision, CommitTs);
        {proxy, LocalNode, RemoteReplica} ->
            ok = grb_measurements:log_counter({?MODULE, Partition, RemoteReplica, fwd_send_decision}),
            %% leader is in another replica, but we don't have a direct inter_dc connection, have
            %% to go through a cluster-local proxy at `LocalNode`
            Msg = grb_dc_messages:frame(grb_dc_messages:red_decision(Ballot, Decision, TxId, CommitTs)),
            grb_dc_utils:send_cast(LocalNode, grb_dc_connection_manager, send_raw_framed, [RemoteReplica, Partition, Msg])
    end.

-spec reduce_vote(red_vote(), grb_time:ts(), {red_vote(), grb_time:ts()}) -> {red_vote(), grb_time:ts()}.
reduce_vote(_, _, {{abort, _}, _}=Err) -> Err;
reduce_vote({abort, _}=Err, Ts, _) -> {Err, Ts};
reduce_vote(ok, Ts, {ok, AccTs}) -> {ok, max(Ts, AccTs)}.

-spec reply_to_client({red_vote(), vclock()}, grb_promise:t()) -> ok.
reply_to_client({ok, CommitVC}, Promise) ->
    grb_promise:resolve({ok, CommitVC}, Promise);

reply_to_client({{abort, _}=Abort, _}, Promise) ->
    grb_promise:resolve(Abort, Promise).
