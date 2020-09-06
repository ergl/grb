-module(grb_red_coordinator).
-behavior(gen_server).
-behavior(poolboy_worker).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% supervision tree
-export([start_link/1]).

-export([commit/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {}).

-spec start_link(term()) -> {ok, pid()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec commit(red_coordinator(), grb_promise:t(), term(), vclock(), [{partition_id(), #{}, #{}}]) -> ok.
commit(Coordinator, Promise, TxId, SnapshotVC, Prepares) ->
    gen_server:cast(Coordinator, {commit, Promise, TxId, SnapshotVC, Prepares}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_WorkerArgs) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({commit, _Promise, _TxId, _SnapshotVC, _Prepares}, State) ->
    {noreply, State};

handle_cast(E, S) ->
    ?LOG_WARNING("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info(Info, State) ->
    ?LOG_INFO("Unhandled msg ~p", [Info]),
    {noreply, State}.
