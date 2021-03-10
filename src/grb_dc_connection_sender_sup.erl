-module(grb_dc_connection_sender_sup).
-behavior(supervisor).
-include("grb.hrl").

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
        [{?SENDER_MODULE,
            {?SENDER_MODULE, start_link, []},
            transient, 5000, worker, [?SENDER_MODULE]}]
    }}.
