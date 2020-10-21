-module(grb_dc_connection_sender_sup).
-behavior(supervisor).

-export([start_link/0,
         start_connection/4]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_connection(TargetReplica, Partition, Ip, Port) ->
    supervisor:start_child(?MODULE, [TargetReplica, Partition, Ip, Port]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_dc_connection_sender,
            {grb_dc_connection_sender, start_link, []},
            transient, 5000, worker, [grb_dc_connection_sender]}]
    }}.
