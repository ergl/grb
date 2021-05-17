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
    RuntimeStats = ?CHILD(grb_measurements, worker, []),
    PropagationVnode = ?VNODE(grb_propagation_vnode_master, grb_propagation_vnode),
    OpLogVnode = ?VNODE(grb_oplog_vnode_master, grb_oplog_vnode),

    VnodeProxySup = ?CHILD(grb_vnode_proxy_sup, supervisor, []),
    CausalSequencer = ?CHILD(grb_causal_sequencer_sup, supervisor, []),

    InterDCSenderSup = ?CHILD(grb_dc_connection_sender_sup, supervisor, []),
    InterDCConnManager = ?CHILD(grb_dc_connection_manager, worker, []),
    LocalBroadcast = ?CHILD(grb_local_broadcast, worker, []),

    ChildSpecs = add_profile_specific_processes([RuntimeStats,
                                                 PropagationVnode,
                                                 OpLogVnode,
                                                 CausalSequencer,
                                                 VnodeProxySup,
                                                 LocalBroadcast,
                                                 InterDCSenderSup,
                                                 InterDCConnManager]),

    {ok, {{one_for_one, 5, 10}, ChildSpecs}}.

-spec add_profile_specific_processes([supervisor:child_spec()]) -> [supervisor:child_spec()].
-ifdef(NO_STRONG_ENTRY_VC).
add_profile_specific_processes(ChildSpecs) ->
    ChildSpecs.
-else.
-ifdef(USE_REDBLUE_SEQUENCER).
add_profile_specific_processes(ChildSpecs) ->
    RedBlueConnectionSup = ?CHILD(grb_redblue_connection_sup, supervisor, []),
    RedBlueManager = ?CHILD(grb_redblue_manager, worker, []),
    [ RedBlueManager, RedBlueConnectionSup | ChildSpecs ].
-else.
add_profile_specific_processes(ChildSpecs) ->
    RedCoordManager = ?CHILD(grb_red_manager, worker, []),
    RedCoordSup = ?CHILD(grb_red_coordinator_sup, supervisor, []),
    PaxosVnode = ?VNODE(grb_paxos_vnode_master, grb_paxos_vnode),
    [RedCoordManager, RedCoordSup, PaxosVnode | ChildSpecs].
-endif.
-endif.
