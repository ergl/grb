-module(grb_replica_state).
-behavior(gen_server).
-include("grb.hrl").

%% Supervisor
-export([start_link/0]).

%% API
-export([uniform_vc/0,
         stable_vc/0,
         known_vc/0]).

-export([set_known_vc/1,
         set_known_vc/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {
    clock_cache :: cache(atom(), vclock())
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec uniform_vc() -> vclock().
uniform_vc() ->
    ets:lookup_element(?MODULE, uniform_vc, 2).

-spec stable_vc() -> vclock().
stable_vc() ->
    ets:lookup_element(?MODULE, stable_vc, 2).

-spec known_vc() -> vclock().
known_vc() ->
    ets:lookup_element(?MODULE, known_vc, 2).

-spec set_known_vc(grb_time:ts()) -> ok.
set_known_vc(Time) ->
    set_known_vc(grb_dc_utils:replica_id(), Time).

-spec set_known_vc(replica_id(), grb_time:ts()) -> ok.
set_known_vc(ReplicaID, Time) ->
    gen_server:cast(?MODULE, {set_known_vc, ReplicaID, Time}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    Cache = ets:new(?MODULE, [set, protected, named_table, {read_concurrency, true}]),
    true = ets:insert(?MODULE, [
        {uniform_vc, grb_vclock:new()},
        {stable_vc, grb_vclock:new()},
        {known_vc, grb_vclock:new()}
    ]),
    {ok, #state{clock_cache=Cache}}.

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast({set_known_vc, ReplicaID, Time}, State=#state{clock_cache=Cache}) ->
    Old = ets:lookup_element(Cache, known_vc, 2),
    New = grb_vclock:set_max_time(ReplicaID, Time, Old),
    true = ets:update_element(Cache, known_vc, {2, New}),
    {noreply, State};

%%handle_cast({set_global_known_matrix, AtId, FromId, Time}, State=#state{clock_cache=Cache}) ->
%%    Key = {AtId, FromId},
%%    OldTime = case ets:lookup(Cache, Key) of
%%        [] -> 0;
%%        [{Key, Ts}] -> Ts
%%    end,
%%    true = ets:insert(Cache, {Key, erlang:max(OldTime, Time)}),
%%    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info(_Arg0, _Arg1) ->
    erlang:error(not_implemented).
