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
            ok = riak_core:register([{vnode_module, grb_propagation_vnode}]),
            ok = riak_core_node_watcher:service_up(grb_propagation, self()),

            ok = riak_core:register([{vnode_module, grb_main_vnode}]),
            ok = riak_core_node_watcher:service_up(grb_main, self()),

            ok = enable_debug_logs(),
            ok = grb_tcp_server:start_server(),
            ok = grb_dc_connection_receiver:start_server(),

            %% this is only when we know there won't be more nodes or replicas joining
            case application:get_env(grb, auto_start_background_processes) of
                {ok, true} ->
                    ?LOG_INFO("Starting background processes"),
                    ok = grb_dc_manager:start_background_processes(),
                    ok = grb_local_broadcast:start_as_singleton(),
                    ok = grb_dc_manager:single_replica_processes();
                _ ->
                    ok
            end,
            {ok, Pid}
    end.

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
