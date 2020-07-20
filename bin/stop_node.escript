#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -hidden -name stop_node@127.0.0.1 -setcookie grb_cookie

-mode(compile).

%% API
-export([main/1]).

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "Usage: ~s 'node_1@host_1'~n", [Name]),
    halt(1).

main([NodeString]) ->
    case parse_node(NodeString) of
        {error, Reason} ->
            io:fwrite(standard_error, "Bad node ~s: ~p~n", [NodeString, Reason]),
            usage(),
            halt(1);
        {ok, Node} ->
            ok = erpc:call(Node, grb_dc_manager, stop_propagation_processes, []),
            ok = erpc:call(Node, grb_dc_manager, stop_background_processes, []),
            ok = erpc:call(Node, grb_local_broadcast, stop, [])
    end;

main(_) ->
    usage(),
    halt(1).

parse_node(Str) ->
    try
        {ok, list_to_atom(Str)}
    catch Err ->
        {error, Err}
    end.
