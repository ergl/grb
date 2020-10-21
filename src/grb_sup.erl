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
    ClockVNode = ?VNODE(grb_propagation_vnode_master, grb_propagation_vnode),
    BlueTxVnode = ?VNODE(grb_main_vnode_master, grb_main_vnode),

    BlueTxReplicaSup = ?CHILD(grb_partition_replica_sup, supervisor, []),
    InterDCSenderSup = ?CHILD(grb_dc_connection_sender_sup, supervisor, []),
    InterDCConnManager = ?CHILD(grb_dc_connection_manager, worker, []),
    LocalBroadcast = ?CHILD(grb_local_broadcast, worker, []),

    ChildSpecs = add_red_processes([ClockVNode,
                                    BlueTxVnode,
                                    BlueTxReplicaSup,
                                    LocalBroadcast,
                                    InterDCSenderSup,
                                    InterDCConnManager]),

    {ok, {{one_for_one, 5, 10}, ChildSpecs}}.

-spec add_red_processes([supervisor:child_spec()]) -> [supervisor:child_spec()].
-ifdef(BLUE_KNOWN_VC).
add_red_processes(ChildSpecs) -> ChildSpecs.
-else.
add_red_processes(ChildSpecs) ->
    RedCoordManager = ?CHILD(grb_red_manager, worker, []),
    RedCoordSup = ?CHILD(grb_red_coordinator_sup, supervisor, []),
    PaxosVnode = ?VNODE(grb_paxos_vnode_master, grb_paxos_vnode),
    RedTimer = ?CHILD(grb_red_timer, worker, []),
    [RedCoordManager, RedCoordSup, PaxosVnode, RedTimer | ChildSpecs].
-endif.
