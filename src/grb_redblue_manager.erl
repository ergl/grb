-module(grb_redblue_manager).
-behavior(gen_server).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).
-ignore_xref([start_link/0]).

-export([start_sequencer_connections/0,
         pool_connection/0,
         connection_closed/1]).

-export([register_promise/2,
         take_transaction_promise/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(TABLE, transaction_promises_table).
-define(CONN_TABLE, redblue_sequencer_connections).
-define(SEQ_POOL_SIZE, redblue_sequencer_pool_size).

-record(state, {
    promise_for_tx :: cache(term(), grb_promise:t()),
    connections :: cache(non_neg_integer(), grb_redblue_connection:t())
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start_sequencer_connections() -> ok | {error, atom()}.
start_sequencer_connections() ->
    {ok, SeqIp} = application:get_env(grb, redblue_sequencer_ip),
    {ok, SeqPort} = application:get_env(grb, redblue_sequencer_port),
    {ok, PoolSize} = application:get_env(grb, redblue_sequencer_connection_pool),
    persist_pool_size(PoolSize),
    try
        Ps = grb_dc_utils:my_partitions(),
        {ok, Connections} = start_sequencer_connections(SeqIp, SeqPort, Ps, PoolSize, []),
        gen_server:call(?MODULE, {add_sequencer_connections, PoolSize, Connections})
    catch Exn -> Exn
    end.

start_sequencer_connections(_, _, _, 0, Acc) ->
    {ok, Acc};
start_sequencer_connections(Ip, Port, Ps, N, Acc) ->
    {ok, Pid} = grb_redblue_connection_sup:start_connection(Ps, Ip, Port),
    start_sequencer_connections(Ip, Port, Ps, N - 1, [Pid | Acc]).

-spec persist_pool_size(non_neg_integer()) -> ok.
persist_pool_size(PoolSize) ->
    persistent_term:put({?MODULE, ?SEQ_POOL_SIZE}, PoolSize).

-spec conn_pool_size() -> non_neg_integer().
conn_pool_size() ->
    persistent_term:get({?MODULE, ?SEQ_POOL_SIZE}).

-spec pool_connection() -> grb_redblue_connection:t().
pool_connection() ->
    ets:lookup_element(?CONN_TABLE, rand:uniform(conn_pool_size()), 2).

-spec connection_closed(grb_redblue_connection:t()) -> ok.
connection_closed(Pid) ->
    gen_server:cast(?MODULE, {connection_closed, Pid}).

-spec register_promise(grb_promise:t(), term()) -> ok.
register_promise(Promise, TxId) ->
    true = ets:insert(?TABLE, {TxId, Promise}),
    ok.

-spec take_transaction_promise(term()) -> {ok, grb_promise:t()} | error.
take_transaction_promise(TxId) ->
    case ets:take(?TABLE, TxId) of
        [{TxId, Promise}] -> {ok, Promise};
        _ -> error
    end.

init([]) ->
    CoordTable = ets:new(?TABLE, [set, public, named_table,
                                  {read_concurrency, true}, {write_concurrency, true}]),
    ConnTable = ets:new(?CONN_TABLE, [set, named_table, {read_concurrency, true}]),
    {ok, #state{promise_for_tx=CoordTable, connections=ConnTable}}.

handle_call({add_sequencer_connections, PoolSize, Connections}, _From, S=#state{connections=Conn}) ->
    Objects = lists:zip(lists:seq(1, PoolSize), Connections),
    true = ets:insert(Conn, Objects),
    {reply, ok, S};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({connection_closed, Pid}, S=#state{connections=Conn}) ->
    lists:foreach(
        fun({_Idx, ConnPid}=Obj) ->
            true = ets:delete_object(Conn, Obj),
            if
                ConnPid =/= Pid ->
                    grb_redblue_connection:close(ConnPid);
                true ->
                    ok
            end
        end,
        ets:tab2list(Conn)
    ),
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.
