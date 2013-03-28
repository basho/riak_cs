-module(repl_helpers).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

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

%% The functions below are for 1.3 repl (aka Advanced Mode MDC)
connect_cluster(Node, IP, Port) ->
    Res = rpc:call(Node, riak_repl_console, connect,
        [[IP, integer_to_list(Port)]]),
    ?assertEqual(ok, Res).

disconnect_cluster(Node, Name) ->
    Res = rpc:call(Node, riak_repl_console, disconnect,
        [[Name]]),
    ?assertEqual(ok, Res).

wait_for_connection(Node, Name) ->
    rt:wait_until(Node,
        fun(_) ->
                {ok, Connections} = rpc:call(Node, riak_core_cluster_mgr,
                    get_connections, []),
                lists:any(fun({{cluster_by_name, N}, _}) when N == Name -> true;
                        (_) -> false
                    end, Connections)
        end).

wait_until_no_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                case proplists:get_value(connected_clusters, Status) of
                    [] ->
                        true;
                    _ ->
                        false
                end
        end). %% 40 seconds is enough for repl

enable_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["enable", Cluster]]),
    ?assertEqual(ok, Res).

disable_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["disable", Cluster]]),
    ?assertEqual(ok, Res).

enable_fullsync(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, fullsync, [["enable", Cluster]]),
    ?assertEqual(ok, Res).

start_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["start", Cluster]]),
    ?assertEqual(ok, Res).

stop_realtime(Node, Cluster) ->
    Res = rpc:call(Node, riak_repl_console, realtime, [["stop", Cluster]]),
    ?assertEqual(ok, Res).

name_cluster(Node, Name) ->
    lager:info("Naming cluster ~p",[Name]),
    Res = rpc:call(Node, riak_repl_console, clustername, [[Name]]),
    ?assertEqual(ok, Res).

