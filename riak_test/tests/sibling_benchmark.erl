%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

-module(sibling_benchmark).

%% @doc Microbenchmark of sibling convergence. Verifies the number of
%% siblings are in the same magnitude with number of update
%% concurrency. To test with previous version, add a configuration
%% like this to your harness::
%%
%%      {sibling_benchmark,
%%       [{write_concurrency, 2},
%%        {duration_sec, 30},
%%        {leave_and_join, 0}, %% number of cluster membership change during the benchmark
%%        {dvv_enabled, true},
%%        {version, previous}]
%%        %%{version, current}]
%%      },


-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET,        "riak-test-bucket").
-define(KEY_SINGLE_BLOCK,   "riak_test_key1").

confirm() ->
    RTConfig = rt_config:get(sibling_benchmark, []),
    Concurrency = proplists:get_value(write_concurrency, RTConfig, 4),
    ?assert(is_integer(Concurrency) andalso Concurrency >= 0),
    %% msec
    Interval = proplists:get_value(write_interval, RTConfig, 100),
    ?assert(is_integer(Interval) andalso Interval >= 0),

    Duration = proplists:get_value(duration_sec, RTConfig, 16),
    ?assert(is_integer(Duration) andalso Duration >= 0),

    DVVEnabled = proplists:get_value(dvv_enabled, RTConfig, true),
    ?assert(is_boolean(DVVEnabled)),

    Config = [{cs,
               [{riak_cs,
                 [{leeway_seconds, 5},
                  {gc_interval, 10},
                  {connection_pools, [{request_pool, {Concurrency*2 + 5, 0}}]}
                 ]}]},
              {riak,
               [{riak_core,
                 [{default_bucket_props,
                   [{allow_mult, true}, {dvv_enabled, DVVEnabled}]}]}
               ]}],
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} =
        case proplists:get_value(version, RTConfig, current) of
            current ->
                put(version, current),
                rtcs:setup(4, Config);
            previous ->
                put(version, previous),
                rtcs:setup(4, Config, previous)
        end,

    %% setting up the stage
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),
    %% The first object
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, <<"boom!">>, UserConfig),

    get_counts(RiakNodes, ?TEST_BUCKET, ?KEY_SINGLE_BLOCK),
    lager:info("====================== run benchmark ====================="),
    {ok, Pid} = start_stats_checker(RiakNodes),

    lager:info("write_concurrency: ~p, write_interval: ~p msec, DVV: ~p",
               [Concurrency, Interval, DVVEnabled]),

    Writers = [start_object_writer(UserConfig, Interval) || _ <- lists:seq(1, Concurrency)],
    {ok, Reader} = start_object_reader(UserConfig),

    %% Do your madness or evil here
    timer:sleep(Duration * 1000),
    LeaveAndJoin = proplists:get_value(leave_and_join, RTConfig, 0),
    leave_and_join_node(RiakNodes, LeaveAndJoin),

    ok = stop_object_reader(Reader),
    [stop_object_writer(Writer) || {ok, Writer} <- Writers],
    lager:info("====================== benchmark done ===================="),
    MaxSib = stop_stats_checker(Pid),
    %% Max number of siblings should not exceed number of upload concurrency
    %% according to DVVset implementation
    lager:info("MaxSib:Concurrency = ~p:~p", [MaxSib, Concurrency]),
    %% Real concurrency is, added by GC workers
    ?assert(MaxSib =< Concurrency + 5),

    get_counts(RiakNodes, ?TEST_BUCKET, ?KEY_SINGLE_BLOCK),

    %% Just test sibling resolver, for a major case of manifests
    RawManifestBucket = rc_helper:to_riak_bucket(objects, ?TEST_BUCKET),
    RawKey = ?KEY_SINGLE_BLOCK,
    case rpc:call(hd(CSNodes), riak_cs_console, resolve_siblings,
                  [RawManifestBucket, RawKey]) of
        ok -> ok;
        {error, not_supported} ->
            _ = lager:info("resolve_siblings does not suport multibag yet, skip it"),
            ok
    end,

    %% tearing down the stage
    erlcloud_s3:delete_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, UserConfig),
    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    rtcs:pass().

start_object_reader(UserConfig) ->
    Pid = spawn_link(fun() -> object_reader(UserConfig, 1) end),
    {ok, Pid}.

stop_object_reader(Pid) ->
    Pid ! {stop, self()},
    receive Reply -> Reply end.

object_reader(UserConfig, IntervalSec)->
    erlcloud_s3:get_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, UserConfig),
    receive
        {stop, From} -> From ! ok
    after IntervalSec * 1000 ->
            object_reader(UserConfig, IntervalSec)
    end.

start_object_writer(UserConfig, IntervalMilliSec) ->
    timer:sleep(IntervalMilliSec),
    Pid = spawn_link(fun() -> object_writer(UserConfig, IntervalMilliSec) end),
    {ok, Pid}.

stop_object_writer(Pid) ->
    Pid ! {stop, self()},
    receive Reply -> Reply end.

object_writer(UserConfig, IntervalMilliSec)->
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),
    receive
        {stop, From} -> From ! ok
    after IntervalMilliSec ->
            object_writer(UserConfig, IntervalMilliSec)
    end.

start_stats_checker(Nodes) ->
    Pid = spawn_link(fun() -> stats_checker(Nodes, 5, -1) end),
    {ok, Pid}.

stop_stats_checker(Pid) ->
    Pid ! {stop, self()},
    receive Reply -> Reply end.

stats_checker(Nodes, IntervalSec, MaxSib0) ->
    NewMaxSib = check_stats(Nodes, MaxSib0),
    receive
        {stop, From} -> From ! check_stats(Nodes, NewMaxSib)
    after IntervalSec * 1000 ->
            stats_checker(Nodes, IntervalSec, NewMaxSib)
    end.

check_stats(Nodes, MaxSib0) ->
    Stats = collect_stats(Nodes),
    MaxSib = pp_siblings(Stats),
    pp_objsize(Stats),
    pp_time(Stats),
    get_counts(Nodes, ?TEST_BUCKET, ?KEY_SINGLE_BLOCK),
    case MaxSib of
        _ when is_integer(MaxSib) andalso MaxSib0 < MaxSib ->
            MaxSib;
        _ ->
            MaxSib0
    end.

collect_stats(Nodes) ->
    RiakNodes = Nodes,
    {Stats, _} = rpc:multicall(RiakNodes, riak_kv_stat, get_stats, []),
    Stats.

pp_siblings(Stats) -> pp(siblings, Stats).

pp_objsize(Stats) ->  pp(objsize, Stats).

pp_time(Stats) ->     pp(time, Stats).

pp(Target, Stats) ->
    AtomMeans = list_to_atom(lists:flatten(["node_get_fsm_", atom_to_list(Target), "_mean"])),
    AtomMaxs = list_to_atom(lists:flatten(["node_get_fsm_", atom_to_list(Target), "_100"])),
    Means = [ safe_get_value(AtomMeans, Stat) || Stat <- Stats ],
    Maxs = [ safe_get_value(AtomMaxs, Stat)   || Stat <- Stats ],
    MeansStr = [ "\t" ++ safe_integer_to_list(Mean) || Mean <- Means ],
    MaxsStr = [ "\t" ++ safe_integer_to_list(Max) || Max <- Maxs ],
    lager:info("~s Mean: ~s", [Target, MeansStr]),
    lager:info("~s Max: ~s", [Target, MaxsStr]),
    Max = lists:foldl(fun erlang:max/2, 0,
                      lists:filter(fun is_integer/1, Maxs)),
    %% lager:debug("Max ~p: ~p <= ~p", [Target, Max, Maxs]),
    Max.

safe_get_value(_AtomKey, {badrpc, _}) -> undefined;
safe_get_value(AtomKey, Stat) when is_list(Stat) ->  proplists:get_value(AtomKey, Stat);
safe_get_value(_, _) -> undefined.

safe_integer_to_list(I) when is_integer(I) ->
    integer_to_list(I);
safe_integer_to_list(_) ->
    " - ".

-spec get_counts(list(), string(), string()) -> {ok, non_neg_integer(), [non_neg_integer()]}.
get_counts(RiakNodes, Bucket, Key) ->
    {ok, RiakObj} = rc_helper:get_riakc_obj(RiakNodes, objects, Bucket, Key),
    SiblingCount = riakc_obj:value_count(RiakObj),
    Histories = [ binary_to_term(V) ||
                    V <- riakc_obj:get_values(RiakObj),
                    V /= <<>>],
    %% [lager:info("History Length: ~p", [length(H)]) || H <- Histories],
    HistoryCounts = [ length(H) || H <- Histories ],
    lager:info("SiblingCount: ~p, HistoryCounts: ~w", [SiblingCount, HistoryCounts]),
    {ok, SiblingCount, HistoryCounts}.

leave_and_join_node(_RiakNodes, 0) -> ok;
leave_and_join_node(RiakNodes, N) ->
    lager:info("leaving node2 (~p)", [N]),
    Node2 = hd(tl(RiakNodes)),
    rt:leave(Node2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node2)),
    timer:sleep(1000),

    lager:info("joining node2 again (~p)", [N]),
    Node1 = hd(RiakNodes),
    rt:start_and_wait(Node2),
    rt:staged_join(Node2, Node1),
    rt:plan_and_commit(Node2),
    timer:sleep(1000),
    leave_and_join_node(RiakNodes, N-1).
