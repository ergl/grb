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
            ok = riak_core:register([{vnode_module, grb_vnode}]),
            ok = riak_core_node_watcher:service_up(grb, self()),

            ok = enable_debug_logs(),
            ok = grb_tcp_server:start_server(),
            case application:get_env(grb, auto_start_background_processes) of
                {ok, true} ->
                    ?LOG_INFO("Starting background processes"),
                    grb_vnode:start_replicas();
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
