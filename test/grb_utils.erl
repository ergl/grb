-module(grb_utils).
-include_lib("common_test/include/ct.hrl").

-define(RING_SIZE, 8).

%% API
-export([ring_size/0,
         init_single_node_dc/2,
         init_single_dc/3,
         init_multi_dc/3,
         stop_node/1,
         stop_clusters/1,
         kill_node/1]).

%% Logger API
-export([log/2]).

log(#{msg := Msg}, _Config) ->
    Master = application:get_env(grb, ct_master, undefined),
    Arg = case Msg of
        {report, Report} ->
            io_lib:format("log report: ~p", [Report]);
        {string, Str} ->
            io_lib:format("string: ~s", [Str]);
        {Format, Terms} ->
            io_lib:format(Format, Terms)
    end,
    _ = erpc:call(Master, ct, log, [Arg]).

ring_size() -> ?RING_SIZE.

-spec init_single_node_dc(term(), proplists:proplist()) -> proplists:proplist().
init_single_node_dc(Suite, Config) ->
    ct:pal("[~p]", [Suite]),
    ok = at_init_testsuite(),

    {ready, Node} =  start_node(dev1, Config),
    ok = erpc:call(Node, grb_cluster_manager, create_cluster, [[Node], 'testing', 2]),
    {ok, _} = erpc:call(Node, grb_dc_manager, create_replica_groups, [[Node]]),
    ReplicaId = erpc:call(Node, grb_dc_manager, replica_id, []),
    Info = #{ReplicaId => #{nodes => [Node], main_node => Node}},
    [ {cluster_info, Info} | Config ].

-spec init_single_dc(term(), [atom()], proplists:proplist()) -> proplists:proplist().
init_single_dc(Suite, NodeNames, Config) ->
    ct:pal("[~p]", [Suite]),
    ok = at_init_testsuite(),

    [Main | _] = Nodes = pmap(fun(NodeName) ->
        {ready, Node} = start_node(NodeName, Config),
        Node
    end, NodeNames),

    ok = erpc:call(Main, grb_cluster_manager, create_cluster, [Nodes, 'testing', 2]),
    {ok, _} = erpc:call(Main, grb_dc_manager, create_replica_groups, [[Main]]),
    ReplicaId = erpc:call(Main, grb_dc_manager, replica_id, []),
    Info = #{ReplicaId => #{nodes => Nodes, main_node => Main}},
    [ {cluster_info, Info} | Config ].

-spec init_multi_dc(term(), [[atom()]], proplists:proplist()) -> proplists:proplist().
init_multi_dc(Suite, ClusterSpec, Config) ->
    ct:pal("[~p]", [Suite]),
    ok = at_init_testsuite(),
    Tuples = pmap(fun(Names) ->
        [Main | _] = ClusterNodes = pmap(fun(Name) ->
            {ready, Node} = start_node(Name, Config),
            Node
        end, Names),
        ok = erpc:call(Main, grb_cluster_manager, create_cluster, [ClusterNodes, Main, 2]),
        ReplicaId = erpc:call(Main, grb_dc_manager, replica_id, []),
        {ReplicaId, #{nodes => ClusterNodes, main_node => Main}}
    end, ClusterSpec),

    MainNodes = [ Main || {_, #{main_node := Main}} <- Tuples ],
    {ok, _} = erpc:call(hd(MainNodes), grb_dc_manager, create_replica_groups, [MainNodes]),
    Info = maps:from_list(Tuples),
    [ {cluster_info, Info} | Config ].

start_node(Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeDir = filename:join([PrivDir, Name]) ++ "/",
    ok = filelib:ensure_dir(NodeDir),

    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    NodeConfig = [{monitor_master, true},
                  {startup_functions, [ {code, set_path, [CodePath]}]},
                  {boot_timeout, 5}],

    case ct_slave:start(Name, NodeConfig) of
        {error, already_started, Node} ->
            {ready, Node};

        {error, Reason, Node} ->
            ct:pal("Error starting node ~p, reason ~p", [Node, Reason]),
            ct_slave:stop(Name),
            {error, Node};

        {ok, Node} ->
            ok = erpc:call(Node, application, load, [riak_core]),
            ok = erpc:call(Node, application, load, [syntax_tools]),
            ok = erpc:call(Node, application, load, [compiler]),
            ok = erpc:call(Node, application, load, [ranch]),
            ok = erpc:call(Node, application, load, [grb]),

            Port = initial_port(Name),
            {ok, NodeCWD} = erpc:call(Node, file, get_cwd, []),
            TS = integer_to_list(os:system_time()),

            %% Riak Config
            ok = erpc:call(Node, application, set_env, [riak_core, ring_state_dir, filename:join([NodeCWD, Node, TS, "data"])]),
            ok = erpc:call(Node, application, set_env, [riak_core, platform_data_dir, filename:join([NodeCWD, Node, TS, "data"])]),
            ok = erpc:call(Node, application, set_env, [riak_core, ring_creation_size, ?RING_SIZE]),
            ok = erpc:call(Node, application, set_env, [riak_core, handoff_port, Port]),

            %% Log config
            ok = erpc:call(Node, logger, set_primary_config, [level, all]),
            ok = erpc:call(Node, application, set_env, [grb, ct_master, node()]),
            ConfLog = #{level => info,
                        formatter => {logger_formatter, #{single_line => true,
                                                          max_size => 2048}},
                        config => #{type => standard_io}},

            ok = erpc:call(Node, logger, add_handler, [grb_ct_redirect, ?MODULE, ConfLog]),

            %% GRB Config
            {ok, Addrs} = inet:getif(),
            IP = element(1, hd(Addrs)),
            ok = erpc:call(Node, application, set_env, [grb, inter_dc_ip, inet:ntoa(IP)]),
            ok = erpc:call(Node, application, set_env, [grb, tcp_port, Port + 1]),
            ok = erpc:call(Node, application, set_env, [grb, tcp_id_len_bits, 16]),

            ok = erpc:call(Node, application, set_env, [grb, inter_dc_port, Port + 2]),
            ok = erpc:call(Node, application, set_env, [grb, inter_dc_pool_size, 16]),

            ok = erpc:call(Node, application, set_env, [grb, version_log_size, 10]),

            ok = erpc:call(Node, application, set_env, [grb, partition_ready_wait_ms, 1]),
            ok = erpc:call(Node, application, set_env, [grb, local_broadcast_interval, 1]),
            ok = erpc:call(Node, application, set_env, [grb, self_blue_heartbeat_interval, 1]),

            ok = erpc:call(Node, application, set_env, [grb, basic_replication_interval, 1]),
            ok = erpc:call(Node, application, set_env, [grb, uniform_replication_interval, 5000]),
            ok = erpc:call(Node, application, set_env, [grb, remote_clock_broadcast_interval, 10000]),

            ok = erpc:call(Node, application, set_env, [grb, prune_committed_blue_interval, 50]),
            ok = erpc:call(Node, application, set_env, [grb, prepared_blue_stale_check_ms, 0]),

            ok = erpc:call(Node, application, set_env, [grb, fault_tolerance_factor, 1]),
            ok = erpc:call(Node, application, set_env, [grb, red_coord_pool_size, 16]),
            ok = erpc:call(Node, application, set_env, [grb, red_heartbeat_schedule_ms, 5]),
            ok = erpc:call(Node, application, set_env, [grb, red_delivery_interval, 1]),
            ok = erpc:call(Node, application, set_env, [grb, red_prune_interval, 20]),
            ok = erpc:call(Node, application, set_env, [grb, red_abort_interval_ms, 100]),

            {ok, _} = erpc:call(Node, grb, start, []),
            ct:pal("Node ~p started", [Node]),

            {ready, Node}
    end.

-spec stop_node(node()) -> ok.
stop_node(Node) ->
    ok = erpc:call(Node, grb_dc_manager, stop_propagation_processes, []),
    ok = erpc:call(Node, grb_dc_manager, stop_background_processes, []),
    {ok, Node} = kill_node(Node),
    ok.

stop_clusters(ClusterMap) ->
    Outer = pmap(fun(#{nodes := Nodes}) ->
        Inner = pmap(fun stop_node/1, Nodes),
        ok = lists:foreach(fun(ok) -> ok end, Inner)
    end, maps:values(ClusterMap)),
    ok = lists:foreach(fun(ok) -> ok end, Outer).

kill_node(Node) ->
    ct_slave:stop(get_node_name(Node)).

-spec at_init_testsuite() -> ok | {error, term()}.
at_init_testsuite() ->
    {ok, N} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ N), shortnames]) of
        {ok, _Pid} ->
            ok;

        {error, {already_started, _}} ->
            ok;

        {error, Trace} ->
            case element(1, Trace) of
                {already_started, _} ->
                    ok;
                _ -> {error, Trace}
            end
    end.

%% @doc Convert node to node atom
-spec get_node_name(node()) -> atom().
get_node_name(NodeAtom) ->
    Node = atom_to_list(NodeAtom),
    {match, [{Pos, _Len}]} = re:run(Node, "@"),
    list_to_atom(string:substr(Node, 1, Pos)).

initial_port(dev1) -> 10015;
initial_port(dev2) -> 10025;
initial_port(dev3) -> 10035;
initial_port(dev4) -> 10045;
initial_port(clusterdev1) -> 10115;
initial_port(clusterdev2) -> 10125;
initial_port(clusterdev3) -> 10135;
initial_port(clusterdev4) -> 10145;
initial_port(clusterdev5) -> 10155;
initial_port(clusterdev6) -> 10165.

-spec pmap(fun(), list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(fun() ->
                           Parent ! {pmap, N, F(X)}
                       end),
            N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N, R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.
