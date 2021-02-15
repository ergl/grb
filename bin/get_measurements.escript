#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -hidden -name get_measurements@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).


-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(
        standard_error,
        "Usage: ~s [-c --cluster cluster_name] [-f config_file] [-o outpath] | 'node_1@host_1' ... 'node_n@host_n'~n",
        [Name]
    ),
    halt(1).

main(Args) ->
    case parse_args(Args, [outpath]) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, Opts=#{outpath := Path}} ->
            case maps:is_key(config, Opts) of
                true -> prepare_from_config(Opts);
                false -> prepare_from_args(maps:get(rest, Opts), Path)
            end
    end.

prepare_from_args(NodeListString, OutPath) ->
    prepare(validate(parse_node_list(NodeListString)), OutPath).

prepare_from_config(#{config := Config, cluster := ClusterName, outpath := OutPath}) ->
    prepare(validate(parse_node_config(ClusterName, Config)), OutPath);
prepare_from_config(#{config := Config, outpath := OutPath}) ->
    prepare(validate(parse_node_config(Config)), OutPath).

%% @doc Parse a literal node list passed as argument
-spec parse_node_list(list(string())) -> {ok, [node()], non_neg_integer()} | error.
parse_node_list([]) ->
    error;
parse_node_list([_ | _] = NodeListString) ->
    try
        Nodes = lists:foldl(
            fun(NodeString, Acc) ->
                Node = list_to_atom(NodeString),
                [Node | Acc]
            end,
            [],
            NodeListString
        ),
        {ok, lists:reverse(Nodes)}
    catch
        _:_ -> error
    end.

-spec parse_node_config(ClusterName :: atom(), ConfigFilePath :: string()) ->
    {ok, [node()], non_neg_integer()} | {error, term()}.
parse_node_config(ClusterName, ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {error, Reason} ->
            {error, Reason};
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            case maps:is_key(ClusterName, ClusterMap) of
                false ->
                    {error,
                        unicode:characters_to_list(
                            io_lib:format("No cluster named ~p", [ClusterName])
                        )};
                true ->
                    #{servers := Servers} = maps:get(ClusterName, ClusterMap),
                    {ok, build_erlang_node_names(lists:usort(Servers))}
            end
    end.

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) ->
    {ok, #{atom() => [node()]}} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            Nodes = maps:fold(
                fun(Cluster, #{servers := Servers}, Acc) ->
                    Acc#{Cluster => build_erlang_node_names(lists:usort(Servers))}
                end,
                #{},
                ClusterMap
            ),
            {ok, Nodes};
        _ ->
            error
    end.

-spec build_erlang_node_names([atom()]) -> [atom()].
build_erlang_node_names(NodeNames) ->
    [
        begin
            {ok, Addr} = inet:getaddr(Node, inet),
            IPString = inet:ntoa(Addr),
            list_to_atom("grb@" ++ IPString)
        end
        || Node <- NodeNames
    ].

%% @doc Validate parsing, then proceed
-spec validate({ok, [node()] | #{atom() => [node()]}} | error | {error, term()}) ->
    ok | no_return().
validate(error) ->
    usage();
validate({error, Reason}) ->
    io:fwrite(standard_error, "Validate error: ~p~n", [Reason]),
    usage();
validate({ok, Nodes}) ->
    {ok, Nodes}.

-spec prepare({ok, [node()] | #{atom() => [node()]}}, string()) -> ok.
prepare({ok, ClusterMap}, OutPath) when is_map(ClusterMap) ->
    maps:fold(
        fun(_ClusterName, NodeList, ok) ->
            ok = do_measurements(NodeList, OutPath)
        end,
        ok,
        ClusterMap
    );

prepare({ok, Nodes}, OutPath) ->
    do_measurements(Nodes, OutPath).

do_measurements(Nodes, Path) ->
    Results = maps:from_list([ {N, erpc:call(N, grb_measurements, report_stats, [])} || N <- Nodes ]),
    file:write_file(Path, term_to_binary(Results)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_args([], _) ->
    {error, noargs};
parse_args(Args, Required) ->
    case parse_args_inner(Args, #{}) of
        {ok, Opts} -> required(Required, Opts);
        Err -> Err
    end.

parse_args_inner([], Acc) ->
    {ok, Acc};
parse_args_inner([[$- | Flag] | Args], Acc) ->
    case Flag of
        [$c] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{cluster => list_to_atom(Arg)} end);
        "-cluster" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{cluster => list_to_atom(Arg)} end);
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        "-file" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        [$o] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{outpath => Arg} end);
        "-out" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{outpath => Arg} end);
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
