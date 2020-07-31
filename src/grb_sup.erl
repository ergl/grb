-module(grb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args),
    {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

-define(VNODE(I, M),
    {I, {riak_core_vnode_master, start_link, [M]},
     permanent, 5000, worker,
     [riak_core_vnode_master]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    BluePropMaster = ?VNODE(grb_propagation_vnode_master, grb_propagation_vnode),
    MainVNodeMaster = ?VNODE(grb_main_vnode_master, grb_main_vnode),

    ReplicaSup = ?CHILD(grb_partition_replica_sup, supervisor, []),
    InterDCConnManager = ?CHILD(grb_dc_connection_manager, worker, []),
    LocalBroadcast = ?CHILD(grb_local_broadcast, worker, []),

    {ok,
        {{one_for_one, 5, 10},
         [BluePropMaster,
          MainVNodeMaster,
          ReplicaSup,
          LocalBroadcast,
          InterDCConnManager]}}.
