-module(grb_oplog_writer_sup).
-behavior(supervisor).
-include("grb.hrl").

-export([start_writer/2,
         start_link/0]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_writer(partition_id(), non_neg_integer()) -> supervisor:startchild_ret().
start_writer(Partition, Id) ->
    supervisor:start_child(?MODULE, [Partition, Id]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_oplog_writer,
            {grb_oplog_writer, start_link, []},
            transient, 5000, worker, [grb_oplog_writer]}]
    }}.
