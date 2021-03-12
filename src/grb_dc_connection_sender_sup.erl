-module(grb_dc_connection_sender_sup).
-behavior(supervisor).
-include("grb.hrl").

-export([start_link/0,
         start_connection/4,
         start_connection_child/4]).

%% Debug API
-export([sender_module/0]).
-ignore_xref([sender_module/0]).

-export([init/1]).

%% Go through define to avoid intellij from complaining.
-define(type(Mod), Mod:t()).
-type handle() :: ?type(?SENDER_MODULE).
-export_type([handle/0]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_connection(TargetReplica :: replica_id(),
                       Partition :: partition_id(),
                       Ip :: inet:ip_address(),
                       Port :: inet:port_number()) -> {ok, [ handle(), ... ]}.

start_connection(TargetReplica, Partition, Ip, Port) ->
    ConnNum = grb_dc_connection_manager:sender_pool_size(),
    start_connection(TargetReplica, Partition, Ip, Port, ConnNum, []).

-spec start_connection(TargetReplica :: replica_id(),
                       Partition :: partition_id(),
                       Ip :: inet:ip_address(),
                       Port :: inet:port_number(),
                       N :: non_neg_integer(),
                       Acc :: [ handle() ]) -> {ok, [ handle(), ... ]}.

start_connection(_, _, _, _, 0, Acc) ->
    {ok, Acc};
start_connection(TargetReplica, Partition, Ip, Port, N, Acc) ->
    {ok, Handle} = ?SENDER_MODULE:start_connection(TargetReplica, Partition, Ip, Port),
    start_connection(TargetReplica, Partition, Ip, Port, N - 1, [Handle | Acc]).

start_connection_child(TargetReplica, Partition, Ip, Port) ->
    supervisor:start_child(?MODULE, [TargetReplica, Partition, Ip, Port]).

-spec sender_module() -> module().
sender_module() ->
    ?SENDER_MODULE.

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{?SENDER_MODULE,
            {?SENDER_MODULE, start_link, []},
            transient, 5000, worker, [?SENDER_MODULE]}]
    }}.
