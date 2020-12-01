-module(grb_app).
-behaviour(application).
-include_lib("kernel/include/logger.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case grb_sup:start_link() of
        {error, Reason} ->
            {error, Reason};

        {ok, Pid} ->
            ok = register_vnodes(),
            ok = enable_debug_logs(),
            ok = grb_tcp_server:start_server(),
            ok = grb_dc_connection_receiver:start_service(),
            {ok, Pid}
    end.

-spec register_vnodes() -> ok.
-ifdef(BLUE_KNOWN_VC).
register_vnodes() ->
    ok = riak_core:register([{vnode_module, grb_propagation_vnode}]),
    ok = riak_core_node_watcher:service_up(grb_propagation, self()),

    ok = riak_core:register([{vnode_module, grb_oplog_vnode}]),
    ok = riak_core_node_watcher:service_up(grb_oplog, self()).
-else.
register_vnodes() ->
    ok = riak_core:register([{vnode_module, grb_paxos_vnode}]),
    ok = riak_core_node_watcher:service_up(grb_paxos, self()),

    ok = riak_core:register([{vnode_module, grb_propagation_vnode}]),
    ok = riak_core_node_watcher:service_up(grb_propagation, self()),

    ok = riak_core:register([{vnode_module, grb_oplog_vnode}]),
    ok = riak_core_node_watcher:service_up(grb_oplog, self()).
-endif.

-ifdef(debug_log).
-spec enable_debug_logs() -> ok.
enable_debug_logs() ->
    logger:add_handler(debug, logger_std_h, #{
        filters => [{debug, {fun logger_filters:level/2, {stop, neq, debug}}}],
        config => #{file => "log/debug.log"}
    }).
-else.
-spec enable_debug_logs() -> ok.
enable_debug_logs() -> ok.
-endif.

stop(_State) ->
    ok.
