%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------

-module(repl_v3_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

confirm() ->
    case rt_config:get(build_type, oss) of
        ee ->
            confirm_ee();
        _ ->
            lager:info("~s test is only valid on riak_ee, skipping", [?MODULE]),
            pass
    end.

confirm_ee() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup2x2(),
    lager:info("UserConfig = ~p", [UserConfig]),
    [A,B,C,D] = RiakNodes,

    ANodes = [A,B],
    BNodes = [C,D],

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    %% User 1, Cluster 1 config
    U1C1Config = rtcs_admin:create_user(AFirst, 1),
    %% User 1, Cluster 2 config
    U1C2Config = rtcs_admin:aws_config(U1C1Config, [{port, rtcs_config:cs_port(BFirst)}]),

    %% User 2, Cluster 2 config
    U2C2Config = rtcs_admin:create_user(BFirst, 2),
    %% User 2, Cluster 1 config
    U2C1Config = rtcs_admin:aws_config(U2C2Config, [{port, rtcs_config:cs_port(AFirst)}]),

    lager:info("User 1 IS valid on the primary cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(U1C1Config)),

    lager:info("User 2 IS valid on the primary cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(U2C1Config)),

    lager:info("User 2 is NOT valid on the secondary cluster"),
    ?assertError({aws_error, _}, erlcloud_s3:list_buckets(U2C2Config)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, U1C1Config)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
        erlcloud_s3:list_buckets(U1C1Config)),

    ObjList1= erlcloud_s3:list_objects(?TEST_BUCKET, U1C1Config),
    ?assertEqual([], proplists:get_value(contents, ObjList1)),

    Object1 = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_one", Object1, U1C1Config),

    ObjList2 = erlcloud_s3:list_objects(?TEST_BUCKET, U1C1Config),
    ?assertEqual(["object_one"],
        [proplists:get_value(key, O) ||
            O <- proplists:get_value(contents, ObjList2)]),

    Obj = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C1Config),
    ?assertEqual(Object1, proplists:get_value(content, Obj)),

    lager:info("set up v3 replication between clusters"),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Name and connect Riak Enterprise V3 replication between A and B
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    repl_helpers:name_cluster(AFirst,"A"),
    repl_helpers:name_cluster(BFirst,"B"),


    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    repl_helpers:wait_until_13_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),
    LeaderB = rpc:call(BFirst, riak_core_cluster_mgr, get_leader, []),

    ModeResA = rpc:call(LeaderA, riak_repl_console, modes, [["mode_repl13"]]),
    ModeResB = rpc:call(LeaderB, riak_repl_console, modes, [["mode_repl13"]]),
    lager:info("Replication Modes = ~p", [ModeResA]),
    lager:info("Replication Modes = ~p", [ModeResB]),


    {ok, {_IP, BPort}} = rpc:call(BFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_helpers:connect_clusters13(LeaderA, ANodes, BPort, "B"),

    ?assertEqual(ok, repl_helpers:wait_for_connection13(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),

    repl_helpers:enable_realtime(LeaderA, "B"),
    repl_helpers:start_realtime(LeaderA, "B"),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Done connection replication
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    %% run an initial fullsync
    repl_helpers:start_and_wait_until_fullsync_complete13(LeaderA),

    lager:info("User 2 is valid on secondary cluster after fullsync,"
               " still no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(U2C2Config)),

    lager:info("User 1 has the test bucket on the secondary cluster now"),
    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
        erlcloud_s3:list_buckets(U1C2Config)),

    lager:info("Object written on primary cluster is readable from secondary "
        "cluster"),
    Obj2 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    ?assertEqual(Object1, proplists:get_value(content, Obj2)),

    lager:info("write 2 more objects to the primary cluster"),

    Object2 = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_two", Object2, U1C1Config),

    Object3 = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_three", Object3, U1C1Config),

    lager:info("disable proxy_get"),
    disable_pg(LeaderA, "B", ANodes, BNodes, BPort),

    lager:info("check we can still read the fullsynced object"),
    Obj3 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    ?assertEqual(Object1, proplists:get_value(content,Obj3)),

    lager:info("check all 3 objects are listed on the sink cluster"),
    ?assertEqual(["object_one", "object_three", "object_two"],
        [proplists:get_value(key, O) || O <- proplists:get_value(contents,
                erlcloud_s3:list_objects(?TEST_BUCKET, U1C2Config))]),

    lager:info("check that the 2 other objects can't be read"),
    %% We expect errors here since proxy_get will fail due to the
    %% clusters being disconnected.
    ?assertError({aws_error, _}, erlcloud_s3:get_object(?TEST_BUCKET,
                                                        "object_two", U1C2Config)),
    ?assertError({aws_error, _}, erlcloud_s3:get_object(?TEST_BUCKET,
                                                        "object_three", U1C2Config)),

    lager:info("enable proxy_get"),
    enable_pg(LeaderA, "B", ANodes, BNodes, BPort),

    lager:info("check we can read object_two via proxy get"),
    Obj6 = erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config),
    ?assertEqual(Object2, proplists:get_value(content, Obj6)),

    lager:info("disable proxy_get again"),
    disable_pg(LeaderA, "B", ANodes, BNodes, BPort),

    lager:info("check we still can't read object_three"),
    ?assertError({aws_error, _}, erlcloud_s3:get_object(?TEST_BUCKET,
            "object_three", U1C2Config)),

    lager:info("check that proxy getting object_two wrote it locally, so we"
        " can read it"),
    Obj8 = erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config),
    ?assertEqual(Object2, proplists:get_value(content, Obj8)),

    lager:info("delete object_one while clusters are disconnected"),
    erlcloud_s3:delete_object(?TEST_BUCKET, "object_one", U1C1Config),

    lager:info("enable proxy_get"),
    enable_pg(LeaderA, "B", ANodes, BNodes, BPort),

    lager:info("delete object_two while clusters are connected"),
    erlcloud_s3:delete_object(?TEST_BUCKET, "object_two", U1C1Config),

    lager:info("object_one is still visible on secondary cluster"),
    Obj9 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    ?assertEqual(Object1, proplists:get_value(content, Obj9)),

    lager:info("object_two is deleted"),
    ?assertError({aws_error, _},
                 erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config)),

    repl_helpers:start_and_wait_until_fullsync_complete13(LeaderA),

    lager:info("object_one is deleted after fullsync"),
    ?assertError({aws_error, _},
                 erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config)),

    lager:info("disable proxy_get again"),
    disable_pg(LeaderA, "B", ANodes, BNodes, BPort),

    Object3A = crypto:rand_bytes(4194304),
    ?assert(Object3 /= Object3A),

    lager:info("write a new version of object_three"),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_three", Object3A, U1C1Config),

    lager:info("Independently write different object_four and object_five to bolth clusters"),

    Object4A = crypto:rand_bytes(4194304),
    Object4B = crypto:rand_bytes(4194304),

    Object5A = crypto:rand_bytes(4194304),
    Object5B = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_four", Object4A, U1C1Config),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_four", Object4B, U1C2Config),
    erlcloud_s3:put_object(?TEST_BUCKET, "object_five", Object5B, U1C2Config),

    lager:info("delay writing object 5 on primary cluster 1 second after "
        "writing to secondary cluster"),
    timer:sleep(1000),
    erlcloud_s3:put_object(?TEST_BUCKET, "object_five", Object5A, U1C1Config),

    lager:info("enable proxy_get"),
    enable_pg(LeaderA, "B", ANodes, BNodes, BPort),

    lager:info("secondary cluster has old version of object three"),
    Obj10 = erlcloud_s3:get_object(?TEST_BUCKET, "object_three", U1C2Config),
    ?assertEqual(Object3, proplists:get_value(content, Obj10)),

    lager:info("secondary cluster has 'B' version of object four"),
    Obj11 = erlcloud_s3:get_object(?TEST_BUCKET, "object_four", U1C2Config),
    ?assertEqual(Object4B, proplists:get_value(content, Obj11)),

    repl_helpers:start_and_wait_until_fullsync_complete13(LeaderA),

    lager:info("secondary cluster has new version of object three"),
    Obj12 = erlcloud_s3:get_object(?TEST_BUCKET, "object_three", U1C2Config),
    ?assertEqual(Object3A, proplists:get_value(content, Obj12)),

    lager:info("secondary cluster has 'B' version of object four"),
    Obj13 = erlcloud_s3:get_object(?TEST_BUCKET, "object_four", U1C2Config),
    ?assertEqual(Object4B, proplists:get_value(content, Obj13)),

    lager:info("secondary cluster has 'A' version of object five, because it "
        "was written later"),
    Obj14 = erlcloud_s3:get_object(?TEST_BUCKET, "object_five", U1C2Config),
    ?assertEqual(Object5A, proplists:get_value(content, Obj14)),

    lager:info("write 'A' version of object four again on primary cluster"),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_four", Object4A, U1C1Config),

    lager:info("secondary cluster now has 'A' version of object four"),

    Obj15 = erlcloud_s3:get_object(?TEST_BUCKET, "object_four", U1C2Config),
    ?assertEqual(Object4A, proplists:get_value(content,Obj15)),

    lager:info("Disable proxy_get (not disconnect) "
               "and enable realtime block replication"),
    set_proxy_get(LeaderA, "disable", "B", ANodes, BNodes),
    set_block_rt(RiakNodes),

    Object6 = crypto:rand_bytes(4194304),
    erlcloud_s3:put_object(?TEST_BUCKET, "object_six", Object6, U1C1Config),
    repl_helpers:wait_until_realtime_sync_complete(ANodes),
    lager:info("The object can be downloaded from sink cluster"),
    Obj16 = erlcloud_s3:get_object(?TEST_BUCKET, "object_six", U1C2Config),
    ?assertEqual(Object6, proplists:get_value(content, Obj16)),

    rtcs:pass().

enable_pg(SourceLeader, SinkName, ANodes, BNodes, BPort) ->
    repl_helpers:connect_clusters13(SourceLeader, ANodes, BPort, SinkName),
    set_proxy_get(SourceLeader, "enable", SinkName, ANodes, BNodes).

disable_pg(SourceLeader, SinkName, ANodes, BNodes, _BPort) ->
    set_proxy_get(SourceLeader, "disable", SinkName, ANodes, BNodes),
    repl_helpers:disconnect_clusters13(SourceLeader, ANodes, SinkName).

set_proxy_get(SourceLeader, EnableOrDisable, SinkName, ANodes, BNodes) ->
    PGEnableResult = rpc:call(SourceLeader, riak_repl_console, proxy_get,
                              [[EnableOrDisable,SinkName]]),

    lager:info("Enabled pg: ~p", [PGEnableResult]),
    Status = rpc:call(SourceLeader, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    ok.

set_block_rt(RiakNodes) ->
    rpc:multicall(RiakNodes, application, set_env,
                  [riak_repl, replicate_cs_blocks_realtime, true]).
