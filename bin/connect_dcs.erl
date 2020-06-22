#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name connect_dcs@127.0.0.1 -setcookie grb_cookie

-mode(compile).
-include("grb.hrl").

-export([main/1]).

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "Usage: ~s [-f config_file]~n", [Name]),
    halt(1).

main(Args) ->
    case parse_args(Args, [config]) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, #{config := ConfigFile}} ->
            prepare(validate(parse_node_config(ConfigFile)))
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

%% @doc Validate parsing, then proceed
-spec validate({ok, [node()]} | error | {error, term()}) -> ok | no_return().
validate(error) ->
    usage();

validate({error, Reason}) ->
    io:fwrite(standard_error, "Validate error: ~p~n", [Reason]),
    usage();

validate({ok, [_SingleNode]}) ->
    io:format("Single-node cluster, nothing to join"),
    halt();

validate({ok, Nodes}) ->
    {ok, Nodes}.

-spec prepare({ok, [node()]}) -> ok | no_return().
prepare({ok, Nodes}) ->
    io:format("Starting clustering of nodes ~p~n", [Nodes]),
    DescResult0 = erpc:multicall(Nodes, grb_dc_manager, replica_descriptor, []),
    DescResult1 = lists:foldl(fun
        (_, {error, Reason}) -> {error, Reason};
        ({ok, D}, {ok, Acc}) -> {ok, [D | Acc]};
        ({error, Reason}, _) -> {error, Reason};
        ({throw, Reason}, _) -> {error, Reason}
    end, {ok, []}, DescResult0),
    case DescResult1 of
        {error, Reason} ->
            io:fwrite(standard_error, "replica_descriptor error: ~p~n", [Reason]),
            halt(1);
        {ok, Descriptors} ->
            JoinResult0 = erpc:multicall(Nodes, grb_dc_manager, connect_to_replicas, [Descriptors]),
            JoinResult1 = lists:foldl(fun
                (_, {error, Reason}) -> {error, Reason};
                ({error, Reason}, _) -> {error, Reason};
                ({throw, Reason}, _) -> {error, Reason};
                ({ok, ok}, _) -> ok
            end, ok, JoinResult0),
            case JoinResult1 of
                {error, Reason} ->
                    io:fwrite(standard_error, "connect_to_replica error: ~p~n", [Reason]),
                    halt(1);
                ok ->
                    DescIds = [Id || {#replica_descriptor{replica_id=Id}} <- Descriptors],
                    io:format("succesfully joined dcs ~p~n", [DescIds]),
                    ok
            end
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
