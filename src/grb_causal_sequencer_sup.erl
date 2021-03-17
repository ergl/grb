-module(grb_causal_sequencer_sup).
-behavior(supervisor).
-include("grb.hrl").

%% API
-export([init/1,
         start_link/0,
         start_sequencer/2]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_sequencer(partition_id(), [replica_id()]) -> supervisor:startchild_ret().
start_sequencer(Partition, Replicas) ->
    supervisor:start_child(?MODULE, [Partition, Replicas]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_causal_sequencer,
            {grb_causal_sequencer, start_link, []},
            transient, 5000, worker, [grb_causal_sequencer]}]
    }}.
