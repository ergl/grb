#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -hidden -name connect_dcs@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "Usage: ~s [-f config_file] | 'node_1@host_1' ... 'node_n@host_n' ~n", [Name]),
    halt(1).

main(Args) ->
    case parse_args(Args, []) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, #{config := ConfigFile}} ->
            prepare(validate(parse_node_config(ConfigFile)));
        {ok, #{rest := Nodes}} ->
            prepare(validate(parse_node_list(Nodes)))
    end.

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) -> {ok, [atom()]} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            Nodes = maps:fold(fun(_, #{servers := Servers}, Acc) ->
                [MainNode | _] = lists:usort(Servers),
                [build_erlang_node_name(MainNode) | Acc]
            end, [], ClusterMap),
            {ok, Nodes};
        _ ->
            error
    end.

-spec build_erlang_node_name(atom()) -> atom().
build_erlang_node_name(Node) ->
    {ok, Addr} = inet:getaddr(Node, inet),
    IPString = inet:ntoa(Addr),
    list_to_atom("grb@" ++ IPString).

parse_node_list([]) ->
    {error, emtpy_node_list};
parse_node_list([Node]) ->
    {ok, [list_to_atom(Node)]};
parse_node_list([_|_]=NodeListString) ->
    try
        Nodes = lists:foldl(fun(NodeString, Acc) ->
            [ list_to_atom(NodeString) | Acc]
        end, [], NodeListString),
        {ok, lists:reverse(Nodes)}
    catch Err -> {error, Err}
    end.

%% @doc Validate parsing, then proceed
-spec validate({ok, [node()]} | error | {error, term()}) -> ok | no_return().
validate(error) ->
    usage();

validate({error, Reason}) ->
    io:fwrite(standard_error, "Validate error: ~p~n", [Reason]),
    usage();

validate({ok, Nodes}) ->
    {ok, Nodes}.

-spec prepare({ok, [node()]}) -> ok | no_return().
prepare({ok, Nodes}) ->
    io:format("Starting clustering of nodes ~p~n", [Nodes]),
    Res = erpc:call(hd(Nodes), grb_dc_manager, create_replica_groups, [Nodes]),
    case Res of
        {error, Reason} ->
            io:fwrite(standard_error, "Error connecting clusters: ~p~n", [Reason]),
            halt(1);
        {ok, Descriptors} ->
            io:format("Joined clusters ~p~n", [Descriptors]),
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_args([], _) -> {error, noargs};
parse_args(Args, Required) ->
    case parse_args_inner(Args, #{}) of
        {ok, Opts} -> required(Required, Opts);
        Err -> Err
    end.

parse_args_inner([], Acc) -> {ok, Acc};
parse_args_inner([ [$- | Flag] | Args], Acc) ->
    case Flag of
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        "-file" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        [$h] ->
            usage(),
            halt(0);
        _ ->
            {error, {badarg, Flag}}
    end;

parse_args_inner(Words, Acc) ->
    {ok, Acc#{rest => Words}}.

parse_flag(Flag, Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args_inner(Rest, Fun(FlagArg));
        _ -> {error, {noarg, Flag}}
    end.

required(Required, Opts) ->
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        true -> {ok, Opts};
        false -> {error, "Missing required fields"}
    end.
