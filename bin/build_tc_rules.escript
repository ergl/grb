#!/usr/bin/env escript

-mode(compile).

-export([main/1]).

-define(IFACE, enp1s0).
-define(ROOT_CMD_STR, "sudo tc qdisc add dev ~p root handle 1: prio bands ~b priomap ~s").
-define(NETEM_CMD_STR, "sudo tc qdisc add dev ~p parent 1:~b handle ~b: netem delay ~bms").
-define(FILTER_CMD_STR, "sudo tc filter add dev ~p parent 1:0 protocol ip prio 1 u32 match ip dst ~s/32 flowid 1:~b").

%% Example configuration file:
%% ```cluster.config:
%% {latencies, #{
%%     nancy => [{rennes, 10}, {tolouse, 20}],
%%     rennes => [{nancy, 10}, {tolouse, 15}],
%%     tolouse => [{rennes, 15}, {nancy, 20}]
%% }}.
%%
%% {clusters, #{
%%     nancy => #{servers => ['apollo-1-1.imdea', ...],
%%                clients => ['apollo-2-1.imdea', ...]},
%%     rennes => #{servers => ['apollo-1-4.imdea', ...],
%%                 clients => ['apollo-2-4.imdea', ...]},
%%     tolouse => #{servers => ['apollo-1-7.imdea', ...],
%%                  clients => ['apollo-2-7.imdea', ...]}
%% }}.
%% ```

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite("Wrong option, reason: ~p~n", [Reason]),
            usage();
        {ok, PropList} ->
            DryRun = proplists:get_value(dry_run, PropList, false),
            _ = erlang:put(dry_run, DryRun),
            {ok, SelfCluster} = safe_get_value(cluster, PropList),
            {ok, ConfigFile} = safe_get_value(config, PropList),
            run(SelfCluster, ConfigFile)
    end.

-spec usage() -> ok.
usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(standard_error, "~s [-d] -c <cluster-name> -f <config_file>~n", [Name]).

run(Self, ConfigFile) ->
    {ok, Terms} = file:consult(ConfigFile),
    {latencies, LatencyMap} = lists:keyfind(latencies, 1, Terms),
    {clusters, ClusterDef} = lists:keyfind(clusters, 1, Terms),
    case maps:get(Self, LatencyMap, empty) of
        empty ->
            %% If we don't have a latency map for ourselves, it means we shouldn't
            %% touch the latencies. We may only have a single cluster
            ok;
        [_|_]=Latencies ->
            true = reset_tc_rules(),
            Latencies = maps:get(Self, LatencyMap),
            ok = build_tc_rules(Latencies, ClusterDef)
    end.

-spec reset_tc_rules() -> boolean().
reset_tc_rules() ->
    already_default() orelse begin
        Cmd = io_lib:format("sudo tc qdisc del dev ~p root", [default_iface()]),
        _ = safe_cmd(Cmd),
        true
    end.

-spec already_default() -> boolean().
already_default() ->
    TCRules = binary:split(list_to_binary(safe_cmd("tc qdisc ls")), [<<"\n">>, <<" \n">>], [global, trim_all]),
    DefaultRule = list_to_binary(io_lib:format("qdisc mq 0: dev ~p root", [default_iface()])),
    lists:member(DefaultRule, TCRules).

build_tc_rules(Latencies, ClusterDefs) ->
    %% Get all the different target latencies. We might have duplicates, so remove them.
    Millis = lists:usort([Latency || {_, Latency} <- Latencies]),
    ok = setup_tc_root(length(Millis)),
    HandleIds = setup_tc_qdiscs(Millis),
    ok = setup_tc_filters(HandleIds, Latencies, ClusterDefs).

setup_tc_qdiscs(Millis) ->
    lists:zipwith(fun(DelayMs, RootMinor) ->
        HandleId = RootMinor * 10,
        RuleCmd = io_lib:format(?NETEM_CMD_STR, [default_iface(), RootMinor, HandleId, DelayMs]),
        _ = safe_cmd(RuleCmd),
        {DelayMs, RootMinor}
    end, Millis, lists:seq(1, length(Millis))).

%% Each qdisc must have the parent number, and a handle
%% Handles must be globally unique, so pick a multiple of the parent minor number
%% Parent 1:1 -> handle 10:
%% Parent 1:2 -> handle 20:
%% We made sure beforehand that there are enough parent bands to do this
setup_tc_filters(HandleIds, Latencies, ClusterDefs) ->
    [begin
        RootMinor = proplists:get_value(Delay, HandleIds),
        #{servers := TargetNodes} = maps:get(TargetCluster, ClusterDefs),
        lists:foreach(fun(Node) ->
            NodeIP = get_ip(Node),
            FilterCmd = io_lib:format(?FILTER_CMD_STR, [default_iface(), NodeIP, RootMinor]),
            _ = safe_cmd(FilterCmd)
        end, TargetNodes)
     end || {TargetCluster, Delay} <- Latencies],
    ok.

get_ip(NodeName) ->
     {ok, Addr} = inet:getaddr(NodeName, inet),
     list_to_atom(inet:ntoa(Addr)).

%% Set up root qdisc, of type prio, with N+1 bands, and a priomap redirecting all
%% traffic to the highes band (lowest priority). This way, only packets matching
%% filters will go through netem qdiscs.
-spec setup_tc_root(non_neg_integer()) -> ok.
setup_tc_root(Lanes) ->
    RootCmd = io_lib:format(?ROOT_CMD_STR, [default_iface(), Lanes + 1, priomap(Lanes)]),
    _ = safe_cmd(RootCmd),
    ok.

-spec priomap(non_neg_integer()) -> string().
priomap(Lanes) when is_integer(Lanes) ->
    lists:join(" ", lists:duplicate(16, integer_to_list(Lanes))).


%% Safety functions and wrappers

safe_cmd(Cmd) ->
    case get_default(dry_run, false) of
        true ->
            ok = io:format("~s~n", [Cmd]),
            "";
        false ->
            os:cmd(Cmd)
    end.

safe_get_value(Key, PropList) ->
    case proplists:get_value(Key, PropList) of
        undefined ->
            error;
        Val -> {ok, Val}
    end.

get_default(Key, Default) ->
    case erlang:get(Key) of
        undefined ->
            Default;
        Val ->
            Val
    end.

%% Hacked-together getopt

-spec parse_args([term()]) -> {ok, proplists:proplist()} | {error, Reason :: atom()}.
parse_args([]) ->
    {error, empty};

parse_args(Args) ->
    parse_args(Args, []).

parse_args([], Acc) ->
    {ok, Acc};

parse_args([ [$- | Flag] | Args], Acc) ->
    case Flag of
        [$d] ->
            parse_args(Args, [{dry_run, true} | Acc]);
        [$c] ->
            case Args of
                [FlagArg | Rest] ->
                    parse_args(Rest, [{cluster, list_to_atom(FlagArg)} | Acc]);
                _ ->
                    {error, {noarg, Flag}}
            end;
        [$f] ->
            case Args of
                [FlagArg | Rest] ->
                    parse_args(Rest, [{config, FlagArg} | Acc]);
                _ ->
                    {error, {noarg, Flag}}
            end;
        _ ->
            {error, {option, Flag}}
    end;

parse_args(_, _) ->
    {error, noarg}.

-spec default_iface() -> atom().
default_iface() ->
    case inet:gethostname() of
        {ok, "apollo-2-4"} ->
            eth0;
        _ ->
            ?IFACE
    end.
