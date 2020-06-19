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
    VMaster = ?VNODE(grb_vnode_master, grb_vnode),
    ReplicaSup = ?CHILD(grb_partition_replica_sup, supervisor, []),
    ReplicaState = ?CHILD(grb_replica_state, worker, []),
    InterDCSenderSup = ?CHILD(grb_dc_connection_sender_sup, supervisor, []),
    {ok,
        {{one_for_one, 5, 10},
         [VMaster,
          ReplicaSup,
          ReplicaState,
          InterDCSenderSup]}}.
