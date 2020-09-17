-module(grb_red_manager).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).
%% erpc
-ignore_xref([start_link/0,
              persist_unique_leader_info/0,
              persist_leader_info/0,
              persist_follower_info/1,
              start_red_coordinators/0]).

-export([persist_unique_leader_info/0,
         persist_leader_info/0,
         persist_follower_info/1,
         leader_of/1,
         quorum_size/0,
         start_red_coordinators/0,
         register_coordinator/1,
         transaction_coordinator/1,
         unregister_coordinator/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(LEADERS_TABLE, grb_red_manager_leaders).
-define(COORD_TABLE, grb_red_manager_coordinators).
-define(QUORUM_KEY, quorum_size).
-define(POOL_SIZE, pool_size).
-define(COORD_KEY, coord_key).

-record(state, {
    pid_for_tx :: cache(term(), red_coordinator()),
    partition_leaders :: cache(partition_id(), leader_location())
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec persist_unique_leader_info() -> ok.
persist_unique_leader_info() ->
    %% todo(borja, red): remove, redundant with calc_quorum_size/0
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, 1),
    ok = gen_server:call(?MODULE, set_leader).

-spec persist_leader_info() -> ok.
persist_leader_info() ->
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, calc_quorum_size()),
    ok = gen_server:call(?MODULE, set_leader).

-spec persist_follower_info(replica_id()) -> ok.
persist_follower_info(LeaderReplica) ->
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, calc_quorum_size()),
    ok = gen_server:call(?MODULE, {set_follower, LeaderReplica}).

-spec calc_quorum_size() -> non_neg_integer().
calc_quorum_size() ->
    AllLen = length(grb_dc_manager:all_replicas()),
    floor(AllLen / 2) + 1.

-spec quorum_size() -> non_neg_integer().
quorum_size() ->
    persistent_term:get({?MODULE, ?QUORUM_KEY}).

-spec leader_of(partition_id()) -> leader_location().
leader_of(Partition) ->
    ets:lookup_element(?LEADERS_TABLE, Partition, 2).

-spec start_red_coordinators() -> ok.
start_red_coordinators() ->
    {ok, PoolSize} = application:get_env(grb, red_coord_pool_size),
    ok = persistent_term:put({?MODULE, ?POOL_SIZE}, PoolSize),
    start_red_coordinators(PoolSize).

-spec start_red_coordinators(non_neg_integer()) -> ok.
start_red_coordinators(0) ->
    ok;
start_red_coordinators(N) ->
    {ok, Pid} = grb_red_coordinator_sup:start_coordinator(N),
    ok = persistent_term:put({?MODULE, ?COORD_KEY, N}, Pid),
    start_red_coordinators(N - 1).

-spec register_coordinator(term()) -> red_coordinator().
register_coordinator(TxId) ->
    PoolSize = persistent_term:get({?MODULE, ?POOL_SIZE}),
    WorkerPid = persistent_term:get({?MODULE, ?COORD_KEY, rand:uniform(PoolSize)}),
    true = ets:insert(?COORD_TABLE, {TxId, WorkerPid}),
    WorkerPid.

-spec transaction_coordinator(term()) -> {ok, red_coordinator()} | error.
transaction_coordinator(TxId) ->
    case ets:lookup(?COORD_TABLE, TxId) of
        [{TxId, Pid}] -> {ok, Pid};
        _ -> error
    end.

-spec unregister_coordinator(term(), pid()) -> ok.
unregister_coordinator(TxId, Pid) ->
    true = ets:delete_object(?COORD_TABLE, {TxId, Pid}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    LeaderTable = ets:new(?LEADERS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    CoordTable = ets:new(?COORD_TABLE, [set, public, named_table,
                                        {read_concurrency, true}, {write_concurrency, true}]),
    {ok, #state{pid_for_tx=CoordTable, partition_leaders=LeaderTable}}.

handle_call(set_leader, _From, S=#state{partition_leaders=Table}) ->
    Objects = [{P, {local, IndexNode}}
                || {P, _}=IndexNode <- grb_dc_utils:get_index_nodes()],
    true = ets:insert(Table, Objects),
    {reply, ok, S};

handle_call({set_follower, Leader}, _From, S=#state{partition_leaders=Table}) ->
    SelfNode = node(),
    Objects = lists:map(fun({Partition, Node}) ->
        case Node of
            SelfNode -> {Partition, {remote, Leader}};
            _ -> {Partition, {proxy, Node, Leader}}
        end
    end, grb_dc_utils:get_index_nodes()),
    true = ets:insert(Table, Objects),
    {reply, ok, S};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.
