-module(grb_red_coordinator_sup).
-behavior(supervisor).
-include("grb.hrl").

-export([start_link/0,
         start_coordinator/0]).

%% API
-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_coordinator() ->
    supervisor:start_child(?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_red_coordinator,
            {grb_red_coordinator, start_link, []},
            transient, 5000, worker, [grb_red_coordinator]}]
    }}.
