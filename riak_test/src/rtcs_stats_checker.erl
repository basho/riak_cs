-module(rtcs_stats_checker).

-compile(export_all).


start_link(Nodes, Fun) ->
    Pid = spawn_link(fun() -> stats_checker(Nodes, Fun, 5) end),
    {ok, Pid}.

stop(Pid) ->
    Pid ! {stop, self()},
    receive Reply -> Reply end.

stats_checker(Nodes, Fun, IntervalSec) ->
    check_stats(Nodes),
    catch Fun(),
    receive
        {stop, From} ->
            catch Fun(),
            From ! check_stats(Nodes)
    after IntervalSec * 1000 ->
            stats_checker(Nodes, Fun, IntervalSec)
    end.

check_stats(Nodes) ->
    Stats = collect_stats(Nodes),
    MaxSib = pp_siblings(Stats),
    pp_objsize(Stats),
    pp_time(Stats),
    MaxSib.

collect_stats(Nodes) ->
    RiakNodes = Nodes,
    {Stats, _} = rpc:multicall(RiakNodes, riak_kv_stat, get_stats, []),
    Stats.

pp_siblings(Stats) -> pp(siblings, Stats).

pp_objsize(Stats) -> pp(objsize, Stats).

pp_time(Stats) ->    pp(time, Stats).

pp(Target, Stats) ->
    AtomMeans = list_to_atom(lists:flatten(["node_get_fsm_", atom_to_list(Target), "_mean"])),
    AtomMaxs = list_to_atom(lists:flatten(["node_get_fsm_", atom_to_list(Target), "_100"])),
    Means = [ proplists:get_value(AtomMeans, Stat) || Stat <- Stats ],
    Maxs = [ proplists:get_value(AtomMaxs, Stat)   || Stat <- Stats ],
    MeansStr = [ "\t" ++ integer_to_list(Mean) || Mean <- Means ],
    MaxsStr = [ "\t" ++ integer_to_list(Max) || Max <- Maxs ],
    lager:info("~s Mean: ~s", [Target, MeansStr]),
    lager:info("~s Max: ~s", [Target, MaxsStr]),
    Max = lists:foldl(fun erlang:max/2, 0, Maxs),
    {ok, Max}.
