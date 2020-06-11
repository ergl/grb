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

stop(_State) ->
    ok.
