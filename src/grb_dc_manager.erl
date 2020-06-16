-module(grb_dc_manager).

%% API
-export([start_background_processes/0,
         stop_background_processes/0]).

start_background_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, start_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res).

stop_background_processes() ->
    Res = grb_dc_utils:bcast_vnode_sync(grb_vnode_master, stop_replicas),
    ok = lists:foreach(fun({_, true}) -> ok end, Res).
