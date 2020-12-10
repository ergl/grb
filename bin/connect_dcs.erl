#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -hidden -name connect_dcs@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).

-type node_config() :: ip_address | node_name.

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(
        standard_error,
        "Usage: ~s [-d] [-i -p inter_dc_port] [-f config_file] | 'node_1@host_1' ... 'node_n@host_n' ~n",
        [Name]
    ),
    halt(1).

main(Args) ->
    case parse_args(Args, []) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, Map} ->
            erlang:put(dry_run, maps:get(dry_run, Map, false)),
            NodeConfig = case maps:get(use_public_ip, Map, false) of
                false ->
                    node_name;
                true ->
                    %% We know the key is defined, enforced in required/2
                    erlang:put(inter_dc_port, maps:get(inter_dc_port, Map)),
                    ip_address
            end,
            prepare(
                validate(
                    case Map of
                        #{config := ConfigFile} -> parse_node_config(NodeConfig, ConfigFile);
                        #{rest := Nodes} -> parse_node_list(NodeConfig, Nodes)
                    end
                )
            )
    end.

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(NodeConfig :: node_config(), ConfigFilePath :: string()) -> {ok, term()} | error.
parse_node_config(NodeConfig, ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            {red_leader_cluster, LeaderCluster} = lists:keyfind(red_leader_cluster, 1, Terms),
            {Leader, AllNodes} = maps:fold(
                fun(ClusterKey, #{servers := Servers}, {LeaderMarker, AllNodesAcc}) ->
                    [MainNode | _] = lists:usort(Servers),
                    NodeName = build_node_name(NodeConfig, MainNode),
                    case ClusterKey of
                        LeaderCluster ->
                            {NodeName, [NodeName | AllNodesAcc]};
                        _ ->
                            {LeaderMarker, [NodeName | AllNodesAcc]}
                    end
                end,
                {undefined, []},
                ClusterMap
            ),
            {ok, {config, NodeConfig, Leader, AllNodes}};
        _ ->
            error
    end.

-spec build_node_name(node_config(), term()) -> term().
build_node_name(node_name, Node) ->
    %% Assumes a route to the Addr
    {ok, Addr} = inet:getaddr(Node, inet),
    IPString = inet:ntoa(Addr),
    list_to_atom("grb@" ++ IPString);

build_node_name(ip_address, IP) ->
    IP.

-spec parse_node_list(node_config(), [term()]) -> {ok, {config | list, node_config(), [term()]}} | {error, atom()}.
parse_node_list(_, []) ->
    {error, emtpy_node_list};
parse_node_list(node_name=C, [Node]) ->
    {ok, {list, C, [list_to_atom(Node)]}};
parse_node_list(ip_address=C, [IP]) ->
    {ok, {list, C, [IP]}};
parse_node_list(Config, [_ | _] = NodeListString) ->
    try
        Nodes = lists:foldl(
            fun(NodeString, Acc) ->
                N = case Config of
                    node_name -> list_to_atom(NodeString);
                    ip_address -> NodeString
                end,
                [N | Acc]
            end,
            [],
            NodeListString
        ),
        {ok, {list, Config, lists:reverse(Nodes)}}
    catch
        Err -> {error, Err}
    end.

%% @doc Validate parsing, then proceed
-spec validate({ok, term()} | error | {error, term()}) -> {ok, term()} | no_return().
validate(error) ->
    usage();
validate({error, Reason}) ->
    io:fwrite(standard_error, "Validate error: ~p~n", [Reason]),
    usage();
validate({ok, Payload}) ->
    {ok, Payload}.

-spec prepare({ok, {config | list, node_config(), [term()]}}) -> ok | no_return().
prepare({ok, {config, NodeConfig, undefined, All}}) -> prepare(NodeConfig, hd(All), All);
prepare({ok, {config, NodeConfig, Leader, All}}) -> prepare(NodeConfig, Leader, All);
prepare({ok, {list, NodeConfig, Nodes}}) -> prepare(NodeConfig, hd(Nodes), Nodes).

prepare(Config, Leader, AllNodes) ->
    io:format("[~p] Starting clustering at leader ~p of nodes ~p~n", [Config, Leader, AllNodes]),
    Res =
        case erlang:get(dry_run) of
            false -> do_connect(Config, Leader, AllNodes);
            true -> {error, dry_run}
        end,
    case Res of
        {error, Reason} ->
            io:fwrite(standard_error, "Error connecting clusters: ~p~n", [Reason]),
            halt(1);
        {ok, Descriptors} ->
            io:format("Joined clusters ~p~n", [Descriptors]),
            ok
    end.

do_connect(node_name, Leader, AllNodes) ->
    erpc:call(Leader, grb_dc_manager, create_replica_groups, [AllNodes]);
do_connect(ip_address, LeaderIP, AllIPs) ->
    Port = erlang:get(inter_dc_port),
    {ok, Socket} = gen_tcp:connect(LeaderIP, Port, [binary, {active, false}, {nodelay, true}, {packet, 4}]),
    ok = gen_tcp:send(Socket, <<0:8, 17:8, (term_to_binary(AllIPs))/binary>>),
    Resp = case gen_tcp:recv(Socket, 0) of
        {ok, Bin} ->
            binary_to_term(Bin);
        {error, Reason} ->
            {error, Reason}
    end,
    ok = gen_tcp:close(Socket),
    Resp.

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
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        "-file" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        [$d] ->
            parse_args_inner(Args, Acc#{dry_run => true});
        [$i] ->
            parse_args_inner(Args, Acc#{use_public_ip => true});
        [$p] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{inter_dc_port => list_to_integer(Arg)} end);
        "-port" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{inter_dc_port => list_to_integer(Arg)} end);
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
        true ->
            case maps:get(use_public_ip, Opts, false) of
                false ->
                    {ok, Opts};
                true ->
                    if
                        is_map_key(inter_dc_port, Opts) ->
                            {ok, Opts};
                        true ->
                            {error, "IP addresses, but no id len of port specified"}
                    end
            end;
        false -> {error, "Missing required fields"}
    end.
