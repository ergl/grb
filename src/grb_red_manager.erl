-module(grb_red_manager).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).
-ignore_xref([start_link/0]).

%% pool api
-export([pool_spec/0]).

-export([register_coordinator/1,
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

-record(state, {
    pid_for_tx :: cache(term(), red_coordinator())
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

-spec register_coordinator(term()) -> red_coordinator().
register_coordinator(TxId) ->
    Pid = poolboy:checkout(?POOL_NAME),
    gen_server:cast(?MODULE, {register_coordinator, Pid, TxId}),
    Pid.

-spec transaction_coordinator(term()) -> {ok, red_coordinator()} | error.
transaction_coordinator(TxId) ->
    case ets:lookup(?MODULE, TxId) of
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
    Table = ets:new(?MODULE, [set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{pid_for_tx=Table}}.

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
