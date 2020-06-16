#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name join_cluster@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).

main([NodeNaneListConfig]) ->
    prepare(validate(parse_node_config(NodeNaneListConfig)));

main(NodesListString) ->
    prepare(validate(parse_node_list(NodesListString))).

%% @doc Parse a literal node list passed as argument
-spec parse_node_list(list(string())) -> {ok, [node()]} | error.
parse_node_list([]) ->
    error;

parse_node_list([_|_]=NodeListString) ->
    try
        Nodes = lists:foldl(fun(NodeString, Acc) ->
            Node = list_to_atom(NodeString),
            [Node | Acc]
                            end, [], NodeListString),
        {ok, lists:reverse(Nodes)}
    catch
        _:_ -> error
    end.

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) -> {ok, [atom()]} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            NodeNames = lists:usort(lists:flatten([N || #{servers := N} <- maps:values(ClusterMap)])),
            {ok, build_erlang_node_names(NodeNames)};
        _ ->
            error
    end.

-spec build_erlang_node_names([atom()]) -> [atom()].
build_erlang_node_names(NodeNames) ->
    [begin
         {ok, Addr} = inet:getaddr(Node, inet),
         IPString = inet:ntoa(Addr),
         list_to_atom("antidote@" ++ IPString)
     end || Node <- NodeNames].

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "~s <config_file> | 'node_1@host_1' ... 'node_n@host_n'~n", [Name]),
    halt(1).

%% @doc Validate parsing, then proceed
-spec validate({ok, [node()]} | error) -> ok | no_return().
validate(error) ->
    usage();

validate({ok, [_SingleNode]}) ->
    io:format("Single-node cluster, nothing to join"),
    halt();

validate({ok, Nodes}) ->
    io:format("Starting clustering of nodes ~p~n", [Nodes]),
    {ok, Nodes}.

-spec prepare({ok, [node()]}) -> ok.
prepare({ok, [_MainNode | _] = Nodes}) ->
    lists:foreach(fun(N) -> erlang:set_cookie(N, grb_cookie) end, Nodes),
    ok = join_nodes(Nodes).
%%    {_, BadNodes} = rpc:multicall(Nodes, inter_dc_manager, start_bg_processes, [stable], infinity),
%%    case BadNodes of
%%        [] ->
%%            ok = wait_until_master_ready(MainNode),
%%            io:format("Successfully joined nodes ~p~n", [Nodes]);
%%        _ ->
%%            io:fwrite(standard_error, "start_bg_processes failed on ~p, aborting~n", [BadNodes]),
%%            halt(1)
%%    end.

join_nodes([MainNode | OtherNodes]=Nodes) ->
    ok = case check_node_owners(Nodes) of
        {error, FaultyNode} ->
            io:fwrite(standard_error, "Bad ownership check on ~p", [FaultyNode]),
            halt(1);
        ok ->
            ok
    end,
    io:format("join request~n"),
    Res0 = erpc:multicall(OtherNodes, riak_core, join, [MainNode]),
    true = lists:all(fun({ok, ok}) -> true; (_) -> false end, Res0),

    %% todo(borja): commit always returns with no plan
    true = wait_plan_ready(MainNode, 5),
    timer:sleep(timer:seconds(20)),
    io:format("commit plan~n"),
    case erpc:call(MainNode, riak_core_claimant, commit, []) of
        ok -> ok;
        Ret ->
            io:fwrite(standard_error, "Bad commit return ~p", [Ret]),
            halt(1)
    end,
    true = wait_changes_done(Nodes, 30),
    true = wait_cluster_ready(MainNode, Nodes, 5),
    true = wait_ring_ready(Nodes, 5).

check_node_owners(Nodes) ->
    Result =  erpc:multicall(Nodes, grb_dc_utils, is_ring_owner, []),
    lists:foldl(fun
        (_, {error, Node}) ->
            {error, Node};
        ({Node, NodeRes}, ok) ->
            case NodeRes of
                {ok, true} -> ok;
                _ -> {error, Node}
            end
    end, ok, lists:zip(Nodes, Result)).

wait_plan_ready(_, 0) -> {error, timeout};
wait_plan_ready(MainNode, Retries) ->
    io:format("check plan ready~n"),
    case erpc:call(MainNode, riak_core_claimant, plan, []) of
        {ok, _, _} -> true;
        _ ->
            io:format("plan not ready, retying (~b)~n", [Retries]),
            timer:sleep(timer:seconds(1)),
            wait_plan_ready(MainNode, Retries - 1)
    end.

wait_changes_done(_, 0) -> {error, timeout};
wait_changes_done(AllNodes, Retries) ->
    io:format("check changes done~n"),
    Res = erpc:multicall(AllNodes, grb_dc_utils, pending_ring_changes, []),
    case lists:all(fun({ok, true}) -> true; (_) -> false end, Res) of
        true -> true;
        false ->
            io:format("changes not done, retrying (~b)~n", [Retries]),
            timer:sleep(timer:seconds(1)),
            wait_changes_done(AllNodes, Retries - 1)
    end.

wait_cluster_ready(_, _, 0) -> {error, timeout};
wait_cluster_ready(MainNode, AllNodes, Retries) ->
    io:format("check cluster ready~n"),
    Members = erpc:call(MainNode, grb_dc_utils, ready_ring_members, []),
    SortedMembers = lists:usort(Members),
    SortedNodes = lists:usort(AllNodes),
    case SortedMembers =:= SortedNodes of
        true -> true;
        false ->
            io:format("cluster not ready, retrying (~b)~n", [Retries]),
            timer:sleep(timer:seconds(1)),
            wait_cluster_ready(MainNode, AllNodes, Retries - 1)
    end.

wait_ring_ready(_, 0) -> {error, timeout};
wait_ring_ready(Nodes, Retries) ->
    io:format("check ring ready~n"),
    Res = erpc:multicall(Nodes, riak_core_ring, ring_ready, []),
    case lists:all(fun({ok, true}) -> true; (_) -> false end, Res) of
        true -> true;
        false ->
            io:format("rings not ready, retrying (~b)~n", [Retries]),
            timer:sleep(timer:seconds(1)),
            wait_changes_done(Nodes, Retries - 1)
    end.
