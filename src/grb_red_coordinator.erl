-module(grb_red_coordinator).
-behavior(gen_server).
-behavior(poolboy_worker).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% supervision tree
-export([start_link/1]).

-export([commit/5,
         already_decided/3,
         accept_ack/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-type partition_ballots() :: #{partition_id() => ballot()}.
-record(certify_state, {
    replica :: replica_id(),
    promise :: grb_promise:t(),
    locations = [] :: [{partition_id(), leader_location()}],
    quorums_to_ack = #{} :: #{partition_id() => pos_integer()},
    ballots = #{} :: partition_ballots(),
    accumulator = #{} :: #{partition_id() => {red_vote(), vclock()}}
}).

-spec start_link(proplists:proplist()) -> {ok, pid()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec commit(red_coordinator(), grb_promise:t(), term(), vclock(), [{partition_id(), #{}, #{}}]) -> ok.
commit(Coordinator, Promise, TxId, SnapshotVC, Prepares) ->
    gen_server:cast(Coordinator, {commit, Promise, TxId, SnapshotVC, Prepares}).

-spec already_decided(term(), red_vote(), vclock()) -> ok.
already_decided(TxId, Vote, VoteVC) ->
    {ok, Coordinator} = grb_red_manager:transaction_coordinator(TxId),
    gen_server:cast(Coordinator, {already_decided, TxId, Vote, VoteVC}).

-spec accept_ack(partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
accept_ack(Partition, Ballot, TxId, Vote, AcceptVC) ->
    case grb_red_manager:transaction_coordinator(TxId) of
        error -> ok;
        {ok, Coordinator} ->
            gen_server:cast(Coordinator, {accept_ack, Partition, Ballot, TxId, Vote, AcceptVC})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_WorkerArgs) ->
    process_flag(trap_exit, true),
    {ok, undefined}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({commit, Promise, TxId, SnapshotVC, Prepares}, undefined) ->
    LocalId = grb_dc_manager:replica_id(),
    QuorumSize = grb_red_manager:quorum_size(),
    SendFun = fun({Partition, Readset, Writeset}, {LeaderAcc, QuorumAcc}) ->
        Location = send_prepare(LocalId, Partition, TxId, Readset, Writeset, SnapshotVC),
        {
            [{Partition, Location} | LeaderAcc],
            QuorumAcc#{Partition => QuorumSize }
        }
    end,
    {LeaderLocations, QuorumsToAck} = lists:foldl(SendFun, {[], #{}}, Prepares),
    {noreply, #certify_state{replica=LocalId,
                             promise=Promise,
                             locations=LeaderLocations,
                             quorums_to_ack=QuorumsToAck}};

handle_cast({already_decided, TxId, Vote, VoteVC}, #certify_state{promise=Promise}) ->
    ?LOG_DEBUG("~p already decided", [TxId]),
    grb_promise:resolve({Vote, VoteVC}, Promise),
    ok = grb_red_manager:unregister_coordinator(TxId),
    {noreply, undefined};

handle_cast({accept_ack, FromPartition, Ballot, TxId, Vote, AcceptVC},
            S0=#certify_state{ballots=Ballots0, quorums_to_ack=Quorums0, accumulator=Acc0}) ->

    {ok, Ballots} = check_ballot(FromPartition, Ballot, Ballots0),
    Acc = Acc0#{FromPartition => {Vote, AcceptVC}},
    ToAck = maps:get(FromPartition, Quorums0),
    ?LOG_DEBUG("~p ACCEPT_ACK(~p, ~b), ~b to go", [TxId, FromPartition, Ballot, ToAck - 1]),
    Quorums = case ToAck of
        1 -> maps:remove(FromPartition, Quorums0);
        _ -> Quorums0#{FromPartition => ToAck - 1}
    end,
    S = case map_size(Quorums) of
        N when N > 0 ->
            S0#certify_state{quorums_to_ack=Quorums, accumulator=Acc, ballots=Ballots};
        0 ->
            Outcome={Decision, CommitVC} = decide_transaction(Acc),
            grb_promise:resolve(Outcome, S0#certify_state.promise),

            LocalId = S0#certify_state.replica,
            lists:foreach(fun({Partition, Location}) ->
                Ballot = maps:get(Partition, Ballots),
                send_decision(LocalId, Partition, Location, Ballot, TxId, Decision, CommitVC)
            end, S0#certify_state.locations),

            ok = grb_red_manager:unregister_coordinator(TxId),
            undefined
    end,
    {noreply, S};

handle_cast({accept_ack, From, Ballot, TxId, Vote, _}, undefined) ->
    ?LOG_DEBUG("missed ACCEPT_ACK(~b, ~p, ~p) from ~p", [Ballot, TxId, Vote, From]),
    {noreply, undefined};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
-spec send_prepare(replica_id(), partition_id(), term(), #{}, #{}, vclock()) -> leader_location().
send_prepare(FromId, Partition, TxId, RS, WS, VC) ->
    LeaderLoc = grb_red_manager:leader_of(Partition),
    case LeaderLoc of
        {local, IndexNode} ->
            %% leader is in the local cluster, go through vnode directly
            ok = grb_paxos_vnode:prepare(IndexNode, TxId, RS, WS, VC, {coord, FromId, node()});
        {remote, RemoteReplica} ->
            %% leader is in another replica, and we have a direct inter_dc connection to it
            ok = grb_dc_connection_manager:send_red_prepare(RemoteReplica, {coord, FromId, node()}, Partition,
                                                            TxId, RS, WS, VC);
        {proxy, LocalNode, RemoteReplica} ->
            %% leader is in another replica, but we don't have a direct inter_dc connection, have
            %% to go through a cluster-local proxy at `LocalNode`
            %% todo(borja, red): Maybe pre-encode the message here, send a binary (look for more places to do this)
            ok = erpc:call(LocalNode,
                           grb_dc_connection_manager,
                           send_red_prepare,
                           [RemoteReplica, {coord, FromId, node()}, Partition, TxId, RS, WS, VC])
    end,
    LeaderLoc.

-spec decide_transaction(#{partition_id() => {red_vote(), vclock()}}) -> {red_vote(), vclock()}.
decide_transaction(VoteMap) ->
    maps:fold(fun
        (_, {Vote, VC}, undefined) -> {Vote, VC};
        (_, {Vote, VC}, VoteAcc) -> reduce_vote(Vote, VC, VoteAcc)
    end, undefined, VoteMap).

-spec send_decision(replica_id(), partition_id(), leader_location(), ballot(), term(), red_vote(), vclock()) -> ok.
send_decision(FromId, Partition, {proxy, LocalNode, _}, Ballot, TxId, Decision, CommitVC) ->
    remote_broadcast(LocalNode, FromId, Partition, Ballot, TxId, Decision, CommitVC);

send_decision(FromId, Partition, {local, {_, LocalNode}}, Ballot, TxId, Decision, CommitVC) ->
    MyNode = node(),
    case LocalNode of
        MyNode -> local_broadcast(FromId, Partition, Ballot, TxId, Decision, CommitVC);
        _ -> remote_broadcast(LocalNode, FromId, Partition, Ballot, TxId, Decision, CommitVC)
    end;

send_decision(FromId, Partition, {remote, _}, Ballot, TxId, Decision, CommitVC) ->
    local_broadcast(FromId, Partition, Ballot, TxId, Decision, CommitVC).

-spec local_broadcast(replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
local_broadcast(FromReplica, Partition, Ballot, TxId, Decision, CommitVC) ->
    grb_paxos_vnode:broadcast_decision(FromReplica, Partition, Ballot, TxId, Decision, CommitVC).

-spec remote_broadcast(node(), replica_id(), partition_id(), ballot(), term(), red_vote(), vclock()) -> ok.
remote_broadcast(Node, FromReplica, Partition, Ballot, TxId, Decision, CommitVC) ->
    erpc:call(Node, grb_paxos_vnode, broadcast_decision, [FromReplica, Partition, Ballot, TxId, Decision, CommitVC]).

-spec reduce_vote(red_vote(), vclock(), {red_vote(), vclock()}) -> {red_vote(), vclock()}.
reduce_vote(_, _, {{abort, _}, _}=Err) -> Err;
reduce_vote({abort, _}=Err, VC, _) -> {Err, VC};
reduce_vote(ok, CommitVC, {ok, AccCommitVC}) -> {ok, grb_vclock:max(CommitVC, AccCommitVC)}.
