-module(grb_partition_replica_sup).
-behavior(supervisor).

-export([start_replica/4,
         start_link/0]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_replica(Partition, Id, Val, Clock) ->
    supervisor:start_child(?MODULE, [Partition, Id, Val, Clock]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_partition_replica,
            {grb_partition_replica, start_link, []},
            transient, 5000, worker, [grb_partition_replica]}]
    }}.
