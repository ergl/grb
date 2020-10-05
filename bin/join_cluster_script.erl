#!/usr/bin/env escript

%% -*- erlang -*-
%%! -smp enable -hidden -name join_cluster@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).

-define(DEFAULT_TREE_FANOUT, 2).

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(
        standard_error,
        "Usage: ~s [-c --cluster cluster_name] [-f config_file] | 'node_1@host_1' ... 'node_n@host_n'~n",
        [Name]
    ),
    halt(1).

main(Args) ->
    case parse_args(Args, []) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, Opts} ->
            io:format("~p~n", [Opts]),
            case maps:is_key(config, Opts) of
                true -> prepare_from_config(Opts);
                false -> prepare_from_args(maps:get(rest, Opts))
            end
    end.

prepare_from_args(NodeListString) ->
    prepare(validate(parse_node_list(NodeListString))).

prepare_from_config(#{config := Config, cluster := ClusterName}) ->
    prepare(validate(parse_node_config(ClusterName, Config)));
prepare_from_config(#{config := Config}) ->
    prepare(validate(parse_node_config(Config))).

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
        {ok, lists:reverse(Nodes), ?DEFAULT_TREE_FANOUT}
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
            Fanout =
                case lists:keyfind(tree_fanout, 1, Terms) of
                    false -> ?DEFAULT_TREE_FANOUT;
                    {tree_fanout, TreeFanout} -> TreeFanout
                end,
            case maps:is_key(ClusterName, ClusterMap) of
                false ->
                    {error,
                        unicode:characters_to_list(
                            io_lib:format("No cluster named ~p", [ClusterName])
                        )};
                true ->
                    #{servers := Servers} = maps:get(ClusterName, ClusterMap),
                    {ok, build_erlang_node_names(lists:usort(Servers)), Fanout}
            end
    end.

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) ->
    {ok, #{atom() => [node()]}, non_neg_integer()} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            Fanout =
                case lists:keyfind(tree_fanout, 1, Terms) of
                    false -> ?DEFAULT_TREE_FANOUT;
                    {tree_fanout, TreeFanout} -> TreeFanout
                end,
            Nodes = maps:fold(
                fun(Cluster, #{servers := Servers}, Acc) ->
                    Acc#{Cluster => build_erlang_node_names(lists:usort(Servers))}
                end,
                #{},
                ClusterMap
            ),
            {ok, Nodes, Fanout};
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
validate({ok, Nodes, Fanout}) ->
    {ok, Nodes, Fanout}.

-spec prepare({ok, [node()] | #{atom() => [node()]}}) -> ok.
prepare({ok, ClusterMap, Fanout}) when is_map(ClusterMap) ->
    maps:fold(
        fun(ClusterName, NodeList, ok) ->
            io:format("Starting clustering at ~p (fanout ~p) of nodes ~p~n", [
                ClusterName,
                Fanout,
                NodeList
            ]),
            ok = do_join(NodeList, Fanout)
        end,
        ok,
        ClusterMap
    );
prepare({ok, Nodes, Fanout}) ->
    io:format("Starting clustering (fanout ~p) of nodes ~p~n", [Fanout, Nodes]),
    do_join(Nodes, Fanout).

do_join(Nodes, Fanout) ->
    ok = erpc:call(hd(Nodes), grb_cluster_manager, create_cluster, [Nodes, Fanout]).

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
