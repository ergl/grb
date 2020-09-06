-module(grb_red_manager).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).
-ignore_xref([start_link/0, persist_leader_info/0]).

%% pool api
-export([pool_spec/0]).

-export([persist_leader_info/0,
         persist_follower_info/1,
         leader_of/1,
         quorum_size/0,
         register_coordinator/1,
         transaction_coordinator/1,
         unregister_coordinator/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(POOL_NAME, red_coord_pool).
%% todo(borja, red): revisit
-define(POOL_SIZE, (1 * erlang:system_info(schedulers_online))).
-define(POOL_OVERFLOW, 5).

-define(LEADERS_TABLE, grb_red_manager_leaders).
-define(COORD_TABLE, grb_red_manager_coordinators).
-define(QUORUM_KEY, quorum_size).

-record(state, {
    pid_for_tx :: cache(term(), red_coordinator()),
    partition_leaders :: cache(partition_id(),
                                {local, index_node()} |
                                {remote, replica_id()})
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec pool_spec() -> supervisor:child_spec().
pool_spec() ->
    Args = [{name, {local, ?POOL_NAME}},
            {worker_module, grb_red_coordinator},
            {size, application:get_env(grb, red_pool_size, ?POOL_SIZE)},
            {max_overflow, ?POOL_OVERFLOW},
            {strategy, lifo}],

    %% todo(borja, red): Any args here?
    WorkerArgs = [],
    poolboy:child_spec(?POOL_NAME, Args, WorkerArgs).

-spec persist_leader_info() -> ok.
persist_leader_info() ->
    RemoteReplicas = grb_dc_manager:remote_replicas(),
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, length(RemoteReplicas)),
    ok = gen_server:call(?MODULE, set_leader).

-spec persist_follower_info(replica_id()) -> ok.
persist_follower_info(LeaderReplica) ->
    RemoteReplicas = grb_dc_manager:remote_replicas(),
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, length(RemoteReplicas)),
    ok = gen_server:call(?MODULE, {set_follower, LeaderReplica}).

-spec quorum_size() -> non_neg_integer().
quorum_size() ->
    persistent_term:get({?MODULE, ?QUORUM_KEY}).

-spec leader_of(partition_id()) -> {local, index_node()} | {remote, replica_id()}.
leader_of(Partition) ->
    ets:lookup_element(?LEADERS_TABLE, Partition, 2).

-spec register_coordinator(term()) -> red_coordinator().
register_coordinator(TxId) ->
    Pid = poolboy:checkout(?POOL_NAME),
    gen_server:cast(?MODULE, {register_coordinator, Pid, TxId}),
    Pid.

-spec transaction_coordinator(term()) -> {ok, red_coordinator()} | error.
transaction_coordinator(TxId) ->
    case ets:lookup(?COORD_TABLE, TxId) of
        [{TxId, Pid}] -> {ok, Pid};
        _ -> error
    end.

-spec unregister_coordinator(term()) -> ok.
unregister_coordinator(TxId) ->
    {ok, Pid} = transaction_coordinator(TxId),
    ok = poolboy:checkin(?POOL_NAME, Pid),
    gen_server:cast(?MODULE, {unregister_coordinator, TxId}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    LeaderTable = ets:new(?LEADERS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    CoordTable = ets:new(?COORD_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{pid_for_tx=CoordTable, partition_leaders=LeaderTable}}.

handle_call(set_leader, _From, S=#state{partition_leaders=Table}) ->
    Objects = [{P, {local, IndexNode}}
                || {P, _}=IndexNode <- grb_dc_utils:get_index_nodes()],
    true = ets:insert(Table, Objects),
    {reply, ok, S};

handle_call({set_follower, Leader}, _From, S=#state{partition_leaders=Table}) ->
    Objects = [{P, {remote, Leader}}
                || {P, _} <- grb_dc_utils:get_index_nodes()],
    true = ets:insert(Table, Objects),
    {reply, ok, S};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({register_coordinator, Pid, TxId}, S=#state{pid_for_tx=Table}) ->
    true = ets:insert(Table, {TxId, Pid}),
    {noreply, S};

handle_cast({unregister_coordinator, TxId}, S=#state{pid_for_tx=Table}) ->
    true = ets:delete(Table, TxId),
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("unexpected info: ~p~n", [E]),
    {noreply, S}.
