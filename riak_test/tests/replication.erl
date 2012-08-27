-module(replication).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

replication() ->
    %% TODO: Don't hardcode # of nodes
    NumNodes = 6,
    ClusterASize = list_to_integer(get_os_env("CLUSTER_A_SIZE", "3")),
    %% ClusterBSize = NumNodes - ClusterASize,
    %% ClusterBSize = list_to_integer(get_os_env("CLUSTER_B_SIZE"), "2"),

    %% Nodes = rt:nodes(NumNodes),
    %% lager:info("Create dirs"),
    %% create_dirs(Nodes),

    Backend = list_to_atom(get_os_env("RIAK_BACKEND",
            "riak_kv_bitcask_backend")),

    lager:info("Deploy ~p nodes using ~p backend", [NumNodes, Backend]),
    Conf = [
            {riak_kv,
             [
                {storage_backend, Backend}
             ]},
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    Nodes = deploy_nodes(NumNodes, Conf),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    make_cluster(ANodes),

    lager:info("Build cluster B"),
    make_cluster(BNodes),

    replication(ANodes, BNodes, false).

replication([AFirst|_] = ANodes, [BFirst|_] = BNodes, Connected) ->

    TestHash = erlang:md5(term_to_binary(os:timestamp())),
    TestBucket = <<TestHash/binary, "-systest_a">>,
    FullsyncOnly = <<TestHash/binary, "-fullsync_only">>,
    RealtimeOnly = <<TestHash/binary, "-realtime_only">>,
    NoRepl = <<TestHash/binary, "-no_repl">>,

    case Connected of
        false ->
            %% clusters are not connected, connect them

            %% write some initial data to A
            lager:info("Writing 100 keys to ~p", [AFirst]),
            ?assertEqual([], do_write(AFirst, 1, 100, TestBucket, 2)),

            %% setup servers/listeners on A
            Listeners = add_listeners(ANodes),

            %% verify servers are visible on all nodes
            verify_listeners(Listeners),

            %% get the leader for the first cluster
            wait_until_leader(AFirst),
            LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),

            %% list of listeners not on the leader node
            NonLeaderListeners = lists:keydelete(LeaderA, 3, Listeners),

            %% setup sites on B
            %% TODO: make `NumSites' an argument
            NumSites = 4,
            {Ip, Port, _} = hd(NonLeaderListeners),
            add_site(hd(BNodes), {Ip, Port, "site1"}),
            FakeListeners = gen_fake_listeners(NumSites-1),
            add_fake_sites(BNodes, FakeListeners),

            %% verify sites are distributed on B
            verify_sites_balanced(NumSites, BNodes),

            %% check the listener IPs were all imported into the site
            verify_site_ips(BFirst, "site1", Listeners);
        _ ->
            lager:info("waiting for leader to converge on cluster A"),
            ?assertEqual(ok, wait_until_leader_converge(ANodes)),
            lager:info("waiting for leader to converge on cluster B"),
            ?assertEqual(ok, wait_until_leader_converge(BNodes)),
            %% get the leader for the first cluster
            LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),
            lager:info("Leader on cluster A is ~p", [LeaderA]),
            [{Ip, Port, _}|_] = get_listeners(LeaderA)
    end,

    %% write some data on A
    ?assertEqual(ok, wait_until_connection(LeaderA)),
    %io:format("~p~n", [rpc:call(LeaderA, riak_repl_console, status, [quiet])]),
    lager:info("Writing 100 more keys to ~p", [LeaderA]),
    ?assertEqual([], do_write(LeaderA, 101, 200, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [LeaderA, BFirst]),
    ?assertEqual(0, wait_for_reads(BFirst, 101, 200, TestBucket, 2)),

    case Connected of
        false ->
            %% check that the keys we wrote initially aren't replicated yet, because
            %% we've disabled fullsync_on_connect
            lager:info("Check keys written before repl was connected are not present"),
            Res2 = rt:systest_read(BFirst, 1, 100, TestBucket, 2),
            ?assertEqual(100, length(Res2)),

            start_and_wait_until_fullsync_complete(LeaderA),

            lager:info("Check keys written before repl was connected are present"),
            ?assertEqual(0, wait_for_reads(BFirst, 1, 200, TestBucket, 2));
        _ ->
            ok
    end,

    %%
    %% Failover tests
    %%

    lager:info("Testing master failover: stopping ~p", [LeaderA]),
    rt:stop(LeaderA),
    rt:wait_until_unpingable(LeaderA),
    ASecond = hd(ANodes -- [LeaderA]),
    wait_until_leader(ASecond),

    LeaderA2 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

    lager:info("New leader is ~p", [LeaderA2]),

    ?assertEqual(ok, wait_until_connection(LeaderA2)),

    lager:info("Writing 100 more keys to ~p now that the old leader is down",
        [ASecond]),

    ?assertEqual([], do_write(ASecond, 201, 300, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 100 keys written to ~p from ~p", [ASecond, BFirst]),
    ?assertEqual(0, wait_for_reads(BFirst, 201, 300, TestBucket, 2)),

    %% get the leader for the first cluster
    LeaderB = rpc:call(BFirst, riak_repl_leader, leader_node, []),

    lager:info("Testing client failover: stopping ~p", [LeaderB]),
    rt:stop(LeaderB),
    rt:wait_until_unpingable(LeaderB),
    BSecond = hd(BNodes -- [LeaderB]),
    wait_until_leader(BSecond),

    LeaderB2 = rpc:call(BSecond, riak_repl_leader, leader_node, []),

    lager:info("New leader is ~p", [LeaderB2]),

    ?assertEqual(ok, wait_until_connection(LeaderA2)),

    lager:info("Writing 100 more keys to ~p now that the old leader is down",
        [ASecond]),

    ?assertEqual([], do_write(ASecond, 301, 400, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading 101 keys written to ~p from ~p", [ASecond, BSecond]),
    ?assertEqual(0, wait_for_reads(BSecond, 301, 400, TestBucket, 2)),

    %% Testing fullsync with downed nodes
    lager:info("Re-running fullsync with ~p and ~p down", [LeaderA, LeaderB]),

    start_and_wait_until_fullsync_complete(LeaderA2),

    %%
    %% Per-bucket repl settings tests
    %%

    lager:info("Restarting down node ~p", [LeaderA]),
    rt:start(LeaderA),
    rt:wait_until_pingable(LeaderA),
    start_and_wait_until_fullsync_complete(LeaderA2),

    lager:info("Restarting down node ~p", [LeaderB]),
    rt:start(LeaderB),
    rt:wait_until_pingable(LeaderB),

    lager:info("Nodes restarted"),

    case nodes_all_have_version(ANodes, "1.1.0") of
        true ->

            make_bucket(LeaderA, NoRepl, [{repl, false}]),

            case nodes_all_have_version(ANodes, "1.2.0") of
                true ->
                    make_bucket(LeaderA, RealtimeOnly, [{repl, realtime}]),
                    make_bucket(LeaderA, FullsyncOnly, [{repl, fullsync}]),

                    %% disconnect the other cluster, so realtime doesn't happen
                    lager:info("disconnect the 2 clusters"),
                    del_site(LeaderB, "site1"),

                    lager:info("write 100 keys to a realtime only bucket"),
                    ?assertEqual([], do_write(ASecond, 1, 100,
                            RealtimeOnly, 2)),

                    lager:info("reconnect the 2 clusters"),
                    add_site(LeaderB, {Ip, Port, "site1"}),
                    ?assertEqual(ok, wait_until_connection(LeaderA));
                _ ->
                    timer:sleep(1000)
            end,

            LeaderA3 = rpc:call(ASecond, riak_repl_leader, leader_node, []),

            lager:info("write 100 keys to a {repl, false} bucket"),
            ?assertEqual([], do_write(ASecond, 1, 100, NoRepl, 2)),

            case nodes_all_have_version(ANodes, "1.2.0") of
                true ->
                    lager:info("write 100 keys to a fullsync only bucket"),
                    ?assertEqual([], do_write(ASecond, 1, 100,
                            FullsyncOnly, 2)),

                    lager:info("Check the fullsync only bucket didn't replicate the writes"),
                    Res6 = rt:systest_read(BSecond, 1, 100, FullsyncOnly, 2),
                    ?assertEqual(100, length(Res6)),

                    lager:info("Check the realtime only bucket that was written to offline "
                        "isn't replicated"),
                    Res7 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
                    ?assertEqual(100, length(Res7));
                _ ->
                    timer:sleep(1000)
            end,

            lager:info("Check the {repl, false} bucket didn't replicate"),
            Res8 = rt:systest_read(BSecond, 1, 100, NoRepl, 2),
            ?assertEqual(100, length(Res8)),

            %% do a fullsync, make sure that fullsync_only is replicated, but
            %% realtime_only and no_repl aren't
            start_and_wait_until_fullsync_complete(LeaderA3),

            case nodes_all_have_version(ANodes, "1.2.0") of
                true ->
                    lager:info("Check fullsync only bucket is now replicated"),
                    ?assertEqual(0, wait_for_reads(BSecond, 1, 100,
                            FullsyncOnly, 2)),

                    lager:info("Check realtime only bucket didn't replicate"),
                    Res10 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
                    ?assertEqual(100, length(Res10)),


                    lager:info("Write 100 more keys into realtime only bucket"),
                    ?assertEqual([], do_write(ASecond, 101, 200,
                            RealtimeOnly, 2)),

                    timer:sleep(5000),

                    lager:info("Check the realtime keys replicated"),
                    ?assertEqual(0, wait_for_reads(BSecond, 101, 200,
                                RealtimeOnly, 2)),

                    lager:info("Check the older keys in the realtime bucket did not replicate"),
                    Res12 = rt:systest_read(BSecond, 1, 100, RealtimeOnly, 2),
                    ?assertEqual(100, length(Res12));
                _ ->
                    ok
            end,

            lager:info("Check {repl, false} bucket didn't replicate"),
            Res13 = rt:systest_read(BSecond, 1, 100, NoRepl, 2),
            ?assertEqual(100, length(Res13));
        _ ->
            ok
    end,

    lager:info("Test passed"),
    fin.

verify_sites_balanced(NumSites, BNodes0) ->
    Leader = rpc:call(hd(BNodes0), riak_repl_leader, leader_node, []),
    case node_has_version(Leader, "1.2.0") of
        true ->
            BNodes = nodes_with_version(BNodes0, "1.2.0") -- [Leader],
            NumNodes = length(BNodes),
            case NumNodes of
                0 ->
                    %% only leader is upgraded, runs clients locally
                    ?assertEqual(NumSites, client_count(Leader));
                _ ->
                    NodeCounts = [{Node, client_count(Node)} || Node <- BNodes],
                    lager:notice("nodecounts ~p", [NodeCounts]),
                    lager:notice("leader ~p", [Leader]),
                    Min = NumSites div NumNodes,
                    [?assert(Count >= Min) || {_Node, Count} <- NodeCounts]
            end;
        false ->
            ok
    end.

make_cluster(Nodes) ->
    [First|Rest] = Nodes,
    [join(Node, First) || Node <- Rest],
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)).

%% does the node meet the version requirement?
node_has_version(Node, Version) ->
    NodeVersion =  rtdev:node_version(rtdev:node_id(Node)),
    case NodeVersion of
        current ->
            %% current always satisfies any version check
            true;
        _ ->
            NodeVersion >= Version
    end.

nodes_with_version(Nodes, Version) ->
    [Node || Node <- Nodes, node_has_version(Node, Version)].

nodes_all_have_version(Nodes, Version) ->
    Nodes == nodes_with_version(Nodes, Version).

client_count(Node) ->
    Clients = rpc:call(Node, supervisor, which_children, [riak_repl_client_sup]),
    length(Clients).

gen_fake_listeners(Num) ->
    Ports = gen_ports(11000, Num),
    IPs = lists:duplicate(Num, "127.0.0.1"),
    Nodes = [fake_node(N) || N <- lists:seq(1, Num)],
    lists:zip3(IPs, Ports, Nodes).

fake_node(Num) ->
    lists:flatten(io_lib:format("fake~p@127.0.0.1", [Num])).

add_fake_sites([Node|_], Listeners) ->
    [add_site(Node, {IP, Port, fake_site(Port)})
     || {IP, Port, _} <- Listeners].

add_site(Node, {IP, Port, Name}) ->
    lager:info("Add site ~p ~p:~p at node ~p", [Name, IP, Port, Node]),
    Args = [IP, integer_to_list(Port), Name],
    Res = rpc:call(Node, riak_repl_console, add_site, [Args]),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(5)).

del_site(Node, Name) ->
    lager:info("Del site ~p at ~p", [Name, Node]),
    Res = rpc:call(Node, riak_repl_console, del_site, [[Name]]),
    ?assertEqual(ok, Res),
    timer:sleep(timer:seconds(5)).

fake_site(Port) ->
    lists:flatten(io_lib:format("fake_site_~p", [Port])).

verify_listeners(Listeners) ->
    Strs = [IP ++ ":" ++ integer_to_list(Port) || {IP, Port, _} <- Listeners],
    [?assertEqual(ok, verify_listener(Node, Strs)) || {_, _, Node} <- Listeners].

verify_listener(Node, Strs) ->
    lager:info("Verify listeners ~p ~p", [Node, Strs]),
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                lists:all(fun(Str) ->
                            lists:keymember(Str, 2, Status)
                    end, Strs)
        end).

add_listeners(Nodes=[FirstNode|_]) ->
    Ports = gen_ports(9010, length(Nodes)),
    IPs = lists:duplicate(length(Nodes), "127.0.0.1"),
    PN = lists:zip3(IPs, Ports, Nodes),
    [add_listener(FirstNode, Node, IP, Port) || {IP, Port, Node} <- PN],
    timer:sleep(timer:seconds(5)),
    PN.

add_listener(N, Node, IP, Port) ->
    lager:info("Adding repl listener to ~p ~s:~p", [Node, IP, Port]),
    Args = [[atom_to_list(Node), IP, integer_to_list(Port)]],
    Res = rpc:call(N, riak_repl_console, add_listener, Args),
    ?assertEqual(ok, Res).

del_listeners(Node) ->
    Listeners = get_listeners(Node),
    lists:foreach(fun(Listener={IP, Port, N}) ->
                lager:info("deleting listener ~p on ~p", [Listener, Node]),
                Res = rpc:call(Node, riak_repl_console, del_listener,
                    [[atom_to_list(N), IP, integer_to_list(Port)]]),
                ?assertEqual(ok, Res)
        end, Listeners).

get_listeners(Node) ->
    Status = rpc:call(Node, riak_repl_console, status, [quiet]),
    %% *sigh*
    [
        begin
                NodeName = list_to_atom(string:substr(K, 10)),
                [IP, Port] = string:tokens(V, ":"),
                {IP, list_to_integer(Port), NodeName}
        end || {K, V} <- Status, is_list(K), string:substr(K, 1, 9) == "listener_"
    ].

gen_ports(Start, Len) ->
    lists:seq(Start, Start + Len - 1).

get_os_env(Var) ->
    case get_os_env(Var, undefined) of
        undefined -> exit({os_env_var_undefined, Var});
        Value -> Value
    end.

get_os_env(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> Value
    end.

verify_site_ips(Leader, Site, Listeners) ->
    Status = rpc:call(Leader, riak_repl_console, status, [quiet]),
    Key = lists:flatten([Site, "_ips"]),
    IPStr = proplists:get_value(Key, Status),
    IPs = lists:sort(re:split(IPStr, ", ")),
    ExpectedIPs = lists:sort(
        [list_to_binary([IP, ":", integer_to_list(Port)]) || {IP, Port, _Node} <-
            Listeners]),
    ?assertEqual(ExpectedIPs, IPs).

make_bucket(Node, Name, Args) ->
    Res = rpc:call(Node, riak_core_bucket, set_bucket, [Name, Args]),
    ?assertEqual(ok, Res).

start_and_wait_until_fullsync_complete(Node) ->
    Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    Count = proplists:get_value(server_fullsyncs, Status0) + 1,
    lager:info("waiting for fullsync count to be ~p", [Count]),

    lager:info("Starting fullsync on ~p (~p)", [Node,
            rtdev:node_version(rtdev:node_id(Node))]),
    rpc:call(Node, riak_repl_console, start_fullsync, [[]]),
    %% sleep because of the old bug where stats will crash if you call it too
    %% soon after starting a fullsync
    timer:sleep(500),

    Res = rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(server_fullsyncs, Status) of
                    C when C >= Count ->
                        true;
                    _ ->
                        false
                end
        end),
    case node_has_version(Node, "1.2.0") of
        true ->
            ?assertEqual(ok, Res);
        _ ->
            case Res of
                ok ->
                    ok;
                _ ->
                    ?assertEqual(ok, wait_until_connection(Node)),
                    lager:warning("Pre 1.2.0 node failed to fullsync, retrying"),
                    start_and_wait_until_fullsync_complete(Node)
            end
    end,

    lager:info("Fullsync on ~p complete", [Node]).

wait_until_leader(Node) ->
    Res = rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case Status of
                    {badrpc, _} ->
                        false;
                    _ ->
                        case proplists:get_value(leader, Status) of
                            undefined ->
                                false;
                            _ ->
                                true
                        end
                end
        end),
    ?assertEqual(ok, Res).

wait_until_leader_converge([Node|_] = Nodes) ->
    rt:wait_until(Node,
        fun(_) ->
                length(lists:usort([begin
                        Status = rpc:call(N, riak_repl_console, status, [quiet]),
                        case Status of
                            {badrpc, _} ->
                                false;
                            _ ->
                                case proplists:get_value(leader, Status) of
                                    undefined ->
                                        false;
                                    L ->
                                        %lager:info("Leader for ~p is ~p",
                                            %[N,L]),
                                        L
                                end
                        end
                end || N <- Nodes])) == 1
        end).

wait_until_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(server_stats, Status) of
                    [] ->
                        false;
                    [_C] ->
                        true;
                    Conns ->
                        lager:warning("multiple connections detected: ~p",
                            [Conns]),
                        true
                end
        end, 80, 500). %% 40 seconds is enough for repl

wait_for_reads(Node, Start, End, Bucket, R) ->
    rt:wait_until(Node,
        fun(_) ->
                rt:systest_read(Node, Start, End, Bucket, R) == []
        end),
    length(rt:systest_read(Node, Start, End, Bucket, R)).

do_write(Node, Start, End, Bucket, W) ->
    case rt:systest_write(Node, Start, End, Bucket, W) of
        [] ->
            [];
        Errors ->
            lager:warning("~p errors while writing: ~p",
                [length(Errors), Errors]),
            timer:sleep(1000),
            lists:flatten([rt:systest_write(Node, S, S, Bucket, W) ||
                    {S, _Error} <- Errors])
    end.

