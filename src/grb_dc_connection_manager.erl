-module(grb_dc_connection_manager).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REPLICAS_TABLE, connected_replicas).
-define(REPLICAS_TABLE_KEY, replicas).
-define(CONN_PIDS_TABLE, connection_pids).

%% External API
-export([connect_to/1,
         connected_replicas/0,
         connection_for_replica/1,
         add_replica_id/1,
         broadcast_msg/1,
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

-spec connection_for_replica(replica_id()) -> pid().
connection_for_replica(Id) ->
    ets:lookup_element(?CONN_PIDS_TABLE, Id, 2).

-spec broadcast_msg(any()) -> ok.
broadcast_msg(Msg) ->
    Pids = ets:select(?CONN_PIDS_TABLE, [{{'_', '$1'}, [], ['$1']}]),
    %% fixme(borja): Avoid going through gen_server, send through socket directly
    lists:foreach(fun(P) ->
        ok = gen_server:cast(P, {test, Msg})
    end, Pids).

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
    true = ets:insert(?CONN_PIDS_TABLE, {ReplicaID, Pid}),
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
