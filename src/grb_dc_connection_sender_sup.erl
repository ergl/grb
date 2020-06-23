-module(grb_dc_connection_sender_sup).
-include("grb.hrl").
-behavior(supervisor).

%% Called by supervisor machinery
-ignore_xref([start_link/0]).

-export([start_connection/3,
         start_link/0]).

%% API
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_connection(replica_id(), inet:ip_address(), inet:port_number()) -> {ok, pid()} | {error, term()}.
start_connection(ReplicaId, IP, Port) ->
    supervisor:start_child(?MODULE, [ReplicaId, IP, Port]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_dc_connection_sender,
            {grb_dc_connection_sender, start_link, []},
            transient, 5000, worker, [grb_dc_connection_sender]}]
    }}.
