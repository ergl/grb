-module(grb_red_coordinator).
-behavior(gen_server).
-behavior(poolboy_worker).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% supervision tree
-export([start_link/1]).

-export([commit/5,
         already_decided/2,
         accept_ack/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(certify_state, {
    promise :: grb_promise:t(),
    quorums_to_ack = #{} :: #{partition_id() => pos_integer()},
    accumulator = #{} :: #{partition_id() => red_vote()}
}).

-spec start_link(term()) -> {ok, pid()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec commit(red_coordinator(), grb_promise:t(), term(), vclock(), [{partition_id(), #{}, #{}}]) -> ok.
commit(Coordinator, Promise, TxId, SnapshotVC, Prepares) ->
    gen_server:cast(Coordinator, {commit, Promise, TxId, SnapshotVC, Prepares}).

-spec already_decided(term(), red_vote()) -> ok.
already_decided(TxId, Vote) ->
    {ok, Coordinator} = grb_red_manager:transaction_coordinator(TxId),
    gen_server:cast(Coordinator, {already_decided, TxId, Vote}).

-spec accept_ack(partition_id(), term(), red_vote()) -> ok.
accept_ack(Partition, TxId, Vote) ->
    {ok, Coordinator} = grb_red_manager:transaction_coordinator(TxId),
    gen_server:cast(Coordinator, {accept_ack, Partition, TxId, Vote}).

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
    %% for each {partition, readset, writeset} in Prepares, check if the leader
    %% for partition is in the local cluster.
    %% - If it is, use riak_core to send a message to the vnode.
    %% - If it isn't, use grb_dc_connection_manager to locate the leader.
    %%   It might be that we don't have an active connection to the leader
    %%   (for example, we're trying to commit at partition P0, but we don't
    %%   own it, so we won't have an active connection to any node that owns P0).
    %%   In that case, we will need to go through a proxy located at the local cluster
    %%   node that owns P0.
    ReplicaId = grb_dc_manager:replica_id(),
    QuorumSize = grb_red_manager:quorum_size(),
    QuorumsToAck = lists:foldl(fun({Partition, Readset, Writeset}, Acc) ->
        case grb_red_manager:leader_of(Partition) of
            {local, IndexNode} ->
                %% leader is in the local cluster, go through vnode directly
                ok = grb_paxos_vnode:local_prepare(IndexNode, TxId, Readset, Writeset, SnapshotVC);
            {remote, RemoteReplica} ->
                %% leader is in another replica, and we have a direct inter_dc connection to it
                ok = grb_dc_connection_manager:send_red_prepare(RemoteReplica, ReplicaId, Partition,
                                                                TxId, Readset, Writeset, SnapshotVC);
            {proxy, LocalNode, RemoteReplica} ->
                %% leader is in another replica, but we don't have a direct inter_dc connection, have
                %% to go through a cluster-local proxy at `LocalNode`
                %% todo(borja, red): LocalNode needs to be aware of how to route the messages back to this coordinator
                ok = erpc:call(LocalNode, grb_dc_connection_manager, send_red_prepare, [RemoteReplica, ReplicaId, Partition,
                                                                                        TxId, Readset, Writeset, SnapshotVC])
        end,
        Acc#{Partition => QuorumSize}
    end, #{}, Prepares),
    {noreply, #certify_state{promise=Promise, quorums_to_ack=QuorumsToAck}};

handle_cast({already_decided, TxId, Vote}, #certify_state{promise=Promise}) ->
    grb_promise:resolve(Vote, Promise),
    ok = grb_red_manager:unregister_coordinator(TxId),
    {noreply, undefined};

handle_cast({accept_ack, Partition, TxId, Vote}, S0=#certify_state{promise=Promise,
                                                                   quorums_to_ack=Quorums0,
                                                                   accumulator=Acc0}) ->
    Acc = Acc0#{Partition => Vote},
    ToAck = maps:get(Partition, Quorums0),
    Quorums = case ToAck of
        1 ->
            maps:remove(Partition, Quorums0);
        _ ->
            Quorums0#{Partition => ToAck - 1}
    end,

    S = case map_size(Quorums) of
        0 ->
            grb_promise:resolve(decide_transaction(Acc), Promise),
            ok = grb_red_manager:unregister_coordinator(TxId),
            undefined;
        _ ->
            S0#certify_state{quorums_to_ack=Quorums, accumulator=Acc}
    end,

    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(Info, State) ->
    ?LOG_INFO("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decide_transaction(#{partition_id() => red_vote()}) -> red_vote().
decide_transaction(VoteMap) ->
    maps:fold(fun
        (_, Vote, undefined) -> Vote;
        (_, Vote, VoteAcc) -> reduce_vote(Vote, VoteAcc)
    end, undefined, VoteMap).

-spec reduce_vote(red_vote(), red_vote()) -> red_vote().
reduce_vote(_, {abort, _}=Err) -> Err;
reduce_vote({abort, _}=Err, _) -> Err;
reduce_vote({ok, CommitVC}, {ok, AccCommitVC}) -> {ok, grb_vclock:max(CommitVC, AccCommitVC)}.
