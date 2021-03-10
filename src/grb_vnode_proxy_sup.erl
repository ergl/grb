-module(grb_vnode_proxy_sup).
-behavior(supervisor).
-include("grb.hrl").

-export([start_reader/2,
         start_link/0]).

-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_reader(partition_id(), non_neg_integer()) -> supervisor:startchild_ret().
start_reader(Partition, Id) ->
    supervisor:start_child(?MODULE, [Partition, Id]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{grb_vnode_proxy,
            {grb_vnode_proxy, start_link, []},
            transient, 5000, worker, [grb_vnode_proxy]}]
    }}.
