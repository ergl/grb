-module(grb_dc_manager).
-include_lib("kernel/include/logger.hrl").

%% API
-export([start_background_processes/0,
         stop_background_processes/0]).

start_background_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_propagate_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.

stop_background_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, stop_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res),
    Res1 = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_propagate_timer),
    ok = lists:foreach(fun({_, ok}) -> ok end, Res1),
    ?LOG_INFO("~p:~p", [?MODULE, ?FUNCTION_NAME]),
    ok.
