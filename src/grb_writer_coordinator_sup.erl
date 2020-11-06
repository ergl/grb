-module(grb_writer_coordinator_sup).
-behavior(supervisor).
-include("grb.hrl").

-export([start_coordinator/2,
         start_link/0]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_coordinator(partition_id(), non_neg_integer()) -> supervisor:startchild_ret().
start_coordinator(Partition, Id) ->
    supervisor:start_child(?MODULE, [Partition, Id]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_writer_coordinator,
            {grb_writer_coordinator, start_link, []},
            transient, 5000, worker, [grb_writer_coordinator]}]
    }}.
