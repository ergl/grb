-module(grb_redblue_connection_sup).
-behavior(supervisor).
-include("grb.hrl").

%% API
-export([start_link/0,
         start_connection/3,
         start_connection_child/3]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_connection(MyPartitions :: [partition_id()],
                       Ip :: inet:ip_address(),
                       Port :: inet:port_number()) -> {ok, grb_redblue_connection:t()}
                                                    | {error, term()}.

start_connection(MyPartitions, Ip, Port) ->
    grb_redblue_connection:start_connection(MyPartitions, Ip, Port).

start_connection_child(Ip, Port, Partitions) ->
    supervisor:start_child(?MODULE, [Ip, Port, Partitions]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_redblue_connection,
            {grb_redblue_connection, start_link, []},
            transient, 5000, worker, [grb_redblue_connection]}]
    }}.
