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

-ifdef(ENABLE_METRICS).
-export([metrics_table/0]).
-endif.

-define(COORD_TABLE, grb_red_manager_coordinators).
-define(QUORUM_KEY, quorum_size).
-define(POOL_SIZE, pool_size).
-define(COORD_KEY, coord_key).
-define(LEADER_KEY, leader_key).

-record(state, { pid_for_tx :: cache(term(), red_coordinator()) }).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec persist_unique_leader_info() -> ok.
persist_unique_leader_info() ->
    persist_leader_info().

-spec persist_leader_info() -> ok.
persist_leader_info() ->
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, calc_quorum_size()),
    lists:foreach(fun({P, _}=IndexNode) ->
        persistent_term:put({?MODULE, ?LEADER_KEY, P}, {local, IndexNode})
    end, grb_dc_utils:get_index_nodes()).

-spec persist_follower_info(replica_id()) -> ok.
persist_follower_info(LeaderReplica) ->
    ok = persistent_term:put({?MODULE, ?QUORUM_KEY}, calc_quorum_size()),
    SelfNode = node(),
    lists:foreach(fun({Partition, Node}) ->
        Value = case Node of
            SelfNode -> {remote, LeaderReplica};
            _ -> {proxy, Node, LeaderReplica}
        end,
        ok = persistent_term:put({?MODULE, ?LEADER_KEY, Partition}, Value)
    end, grb_dc_utils:get_index_nodes()).

-spec calc_quorum_size() -> non_neg_integer().
calc_quorum_size() ->
    {ok, FaultToleranceFactor} = application:get_env(grb, fault_tolerance_factor),
    Factor = (FaultToleranceFactor + 1),
    %% peg between 1 and the size of all replicas to avoid bad usage
    max(1, min(Factor, length(grb_dc_manager:all_replicas()))).

-spec quorum_size() -> non_neg_integer().
quorum_size() ->
    persistent_term:get({?MODULE, ?QUORUM_KEY}).

-spec leader_of(partition_id()) -> leader_location().
leader_of(Partition) ->
    persistent_term:get({?MODULE, ?LEADER_KEY, Partition}).

-spec start_red_coordinators() -> ok.
start_red_coordinators() ->
    {ok, PoolSize} = application:get_env(grb, red_coord_pool_size),
    ok = persistent_term:put({?MODULE, ?POOL_SIZE}, PoolSize),

    AllReplicas = grb_dc_manager:all_replicas(),
    lists:foreach(
        fun(P) ->
            grb_measurements:create_stat({grb_red_coordinator, P, sent_to_first_ack}),
            grb_measurements:create_stat({grb_red_coordinator, P, sent_to_decision}),
            lists:foreach(
                fun(R) ->
                    grb_measurements:create_stat({grb_red_coordinator, P, R, sent_to_ack}),
                    grb_measurements:create_stat({grb_red_coordinator, P, R, ack_in_flight})
                end,
                AllReplicas
            )
        end,
        grb_dc_utils:my_partitions()
    ),

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

-ifdef(ENABLE_METRICS).
init([]) ->
    CoordTable = ets:new(?COORD_TABLE, [set, public, named_table,
                                        {read_concurrency, true}, {write_concurrency, true}]),
    Ref = ets:new(coord_metrics, [ordered_set, public,
                                  {read_concurrency, true}, {write_concurrency, true}]),
    ok = persistent_term:put({?MODULE, coord_metrics}, Ref),
    {ok, #state{pid_for_tx=CoordTable}}.

-spec metrics_table() -> reference().
metrics_table() ->
    persistent_term:get({?MODULE, coord_metrics}).
-else.
init([]) ->
    CoordTable = ets:new(?COORD_TABLE, [set, public, named_table,
                                        {read_concurrency, true}, {write_concurrency, true}]),
    {ok, #state{pid_for_tx=CoordTable}}.
-endif.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.
