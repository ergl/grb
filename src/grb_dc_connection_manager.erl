-module(grb_dc_connection_manager).
-behavior(gen_server).
-include("grb.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REPLICAS_TABLE, connected_replicas).
-define(REPLICAS_TABLE_KEY, replicas).
-define(CONN_PIDS_TABLE, connection_pids).

%% External API
-export([connect_to/1,
         connected_replicas/0,
         connections_for_replica/1,
         add_replica_id/1,
         broadcast_msg/1,
         broadcast_tx/3,
         broadcast_heartbeat/3,
         add_replica_connection/2]).

%% Supervisor
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {
    replicas :: cache(replicas, ordsets:ordset(replica_id())),
    connection_pids :: cache(replica_id(), pid())
}).

%% todo(borja): Right now connections are per node, maybe add info about partitions
%%
%%  Right now we're counting on a 1-to-1 correspondance between replica nodes and
%%  partitions, in the sense that, if a node is responsible for partitions A and B
%%  on one replica, then at all replicas, there's only a single node responsible
%%  for A and B.
%%
%%  As such, when a partition wants to route a message to its replica, it is enough
%%  to know the target replica id, since the partitioning is already implicit.
-spec connect_to(replica_descriptor()) -> ok | {error, term()}.
connect_to(#replica_descriptor{replica_id=ReplicaID, remote_addresses=RemoteNodes}) ->
    ?LOG_DEBUG("Node ~p connected succesfully to DC ~p", [ReplicaID]),
    %% RemoteNodes is a map, with Partitions as keys
    %% The value is a tuple {IP, Port}, to allow addressing
    %% that specific partition at the replica
    %%
    %% Get our local partitions (to our nodes),
    %% and get those from the remote addresses
    try
        lists:foldl(fun(Partition, AccSet) ->
            Entry={RemoteIP, RemotePort} = maps:get(Partition, RemoteNodes),
            case sets:is_element(Entry, AccSet) of
                true ->
                    AccSet;
                false ->
                    case grb_dc_connection_sender_sup:start_connection(ReplicaID, RemoteIP, RemotePort) of
                        {ok, _} ->
                            sets:add_element(Entry, AccSet);
                        Err ->
                            throw({error, {sender_connection, ReplicaID, RemoteIP, Err}})
                    end
            end
        end, sets:new(), grb_dc_utils:my_partitions()),
        ok
    catch Exn -> Exn
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Node API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_replica_id(replica_id()) -> ok.
add_replica_id(Id) ->
    gen_server:call(?MODULE, {add_replica_id, Id}).

-spec add_replica_connection(replica_id(), pid()) -> ok.
add_replica_connection(Id, Pid) ->
    gen_server:call(?MODULE, {add_connection_pid, Id, Pid}).

-spec connected_replicas() -> [replica_id()].
connected_replicas() ->
    ets:lookup_element(?REPLICAS_TABLE, ?REPLICAS_TABLE_KEY, 2).

-spec connections_for_replica(replica_id()) -> [pid()].
connections_for_replica(Id) ->
    ets:lookup_element(?CONN_PIDS_TABLE, Id, 2).

-spec broadcast_msg(any()) -> ok.
broadcast_msg(Msg) ->
    Pids = lists:flatten(ets:select(?CONN_PIDS_TABLE, [{{'_', '$1'}, [], ['$1']}])),
    %% fixme(borja): Avoid going through gen_server, send through socket directly
    lists:foreach(fun(P) ->
        ok = gen_server:cast(P, {send, Msg})
    end, Pids).

-spec broadcast_heartbeat(replica_id(), partition_id(), grb_time:ts()) -> ok.
broadcast_heartbeat(FromId, ToPartition, Time) ->
    broadcast_msg(heartbeat(FromId, ToPartition, Time)).

-spec broadcast_tx(replica_id(), partition_id(), {term(), #{}, vclock()}) -> ok.
broadcast_tx(FromId, ToPartition, Transaction) ->
    broadcast_msg(replicate_tx(FromId, ToPartition, Transaction)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    ReplicaTable = ets:new(?REPLICAS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:new()}),
    ConnPidTable = ets:new(?CONN_PIDS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{replicas=ReplicaTable,
                connection_pids=ConnPidTable}}.

handle_call({add_replica_id, ReplicaID}, _From, State) ->
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:add_element(ReplicaID, Replicas)}),
    {reply, ok, State};

handle_call({add_connection_pid, ReplicaID, Pid}, _From, State) ->
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:add_element(ReplicaID, Replicas)}),
    PidSet = case ets:lookup(?CONN_PIDS_TABLE, ReplicaID) of
        [] -> ordsets:new();
        [{ReplicaID, Set}] -> Set
    end,
    true = ets:insert(?CONN_PIDS_TABLE, {ReplicaID, ordsets:add_element(Pid, PidSet)}),
    {reply, ok, State};

handle_call(E, _From, S) ->
    ?LOG_WARNING("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec heartbeat(replica_id(), partition_id(), grb_time:ts()) -> binary().
heartbeat(FromId, ToPartition, Timestamp) ->
    dc_message(FromId, ToPartition, #blue_heartbeat{timestamp=Timestamp}).

-spec replicate_tx(replica_id(), partition_id(), {term(), #{}, vclock()}) -> binary().
replicate_tx(FromId, ToPartition, {TxId, WS, CommitVC}) ->
    dc_message(FromId, ToPartition, #replicate_tx{tx_id=TxId,
                                                  writeset=WS,
                                                  commit_vc=CommitVC}).

-spec dc_message(replica_id(), partition_id(), term()) -> binary().
dc_message(FromId, Partition, Payload) ->
    PBin = pad(?PARTITION_BYTES, binary:encode_unsigned(Partition)),
    Msg = #inter_dc_message{source_id=FromId, payload=Payload},
    <<?VERSION:?VERSION_BITS, PBin/binary, (term_to_binary(Msg))/binary>>.

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
    case Width - byte_size(Binary) of
        0 -> Binary;
        N when N > 0-> <<0:(N*8), Binary/binary>>
    end.
