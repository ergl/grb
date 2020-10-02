-module(grb_partition_replica_sup).
-behavior(supervisor).
-include("grb.hrl").

-export([start_replica/2,
         start_link/0]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_replica(partition_id(), non_neg_integer()) -> supervisor:startchild_ret().
start_replica(Partition, Id) ->
    supervisor:start_child(?MODULE, [Partition, Id]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_partition_replica,
            {grb_partition_replica, start_link, []},
            transient, 5000, worker, [grb_partition_replica]}]
    }}.
