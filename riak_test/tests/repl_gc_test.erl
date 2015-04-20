-module(repl_gc_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

%% @doc This test demonstrates race condition of CS garbage collection
%% and fullsync, which causes blocks leaking space... in uni-direction replication
%%
%% Sink <- Source
%%
%% - create object_one
%% - run fullsync
%% - delete object_one
%% - run GC on sink
%% - run fullsync
%% - and then leaks!

confirm() ->
    %% With delete_mode keep, this test works fine in 2.0. But for
    %% production environment, keeping keys of tombstone harms memory
    %% usage as blocks are stored in bitcask.
    DeleteModeKV = [], %% Fail in 2.0
    %% DeleteModeKV = [{riak_kv, [{delete_mode, keep}]}], %% Success in 2.0

    NonGCConfig = [{riak, rtcs:riak_config(DeleteModeKV)},
                   {stanchion, rtcs:stanchion_config()},
                   {cs, rtcs:cs_config([{gc_interval, infinity},
                                        {leeway_seconds, 1}])}],
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup2x2(NonGCConfig),
    lager:info("UserConfig = ~p", [UserConfig]),
    [A,B,C,D] = RiakNodes,
    [CSSource, _, CSSink, _] = CSNodes,
    rt:setup_log_capture(CSSink),
    rt:setup_log_capture(CSSource),

    SourceNodes = [A,B],
    SinkNodes = [C,D],
    lager:info("SourceNodes: ~p", [SourceNodes]),
    lager:info("SinkNodes: ~p", [SinkNodes]),

    SourceFirst = hd(SourceNodes),
    SinkFirst = hd(SinkNodes),

    {AccessKeyId, SecretAccessKey} = rtcs:create_user(SinkFirst, 1),

    %% User 1, Source config
    U1C1Config = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(SourceFirst)),
    %% User 1, Sink config
    U1C2Config = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(SinkFirst)),

    lager:info("User 1 IS valid on the primary cluster, and has no buckets"),
    lager:info("UserConfig ~p", [U1C1Config]),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(U1C1Config)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, U1C1Config)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(U1C1Config)),

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
    %% Name and connect Riak Enterprise V3 replication Source -> Sink
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    repl_helpers:name_cluster(SourceFirst,"source"),
    repl_helpers:name_cluster(SinkFirst,"sink"),

    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),

    repl_helpers:wait_until_13_leader(SourceFirst),
    LeaderA = rpc:call(SourceFirst, riak_core_cluster_mgr, get_leader, []),
    LeaderB = rpc:call(SinkFirst, riak_core_cluster_mgr, get_leader, []),

    ModeResA = rpc:call(LeaderA, riak_repl_console, modes, [["mode_repl13"]]),
    ModeResB = rpc:call(LeaderB, riak_repl_console, modes, [["mode_repl13"]]),
    lager:info("Replication Modes = ~p", [ModeResA]),
    lager:info("Replication Modes = ~p", [ModeResB]),

    %% Replication from Source to Sink: source connecting BPort
    {ok, {_IP, BPort}} = rpc:call(SinkFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_helpers:connect_clusters13(LeaderA, SourceNodes, BPort, "sink"),

    %% ?assertEqual(ok, repl_helpers:wait_for_connection13(LeaderA, "source")),
    rt:wait_until_ring_converged(SourceNodes),

    PGEnableResult = rpc:call(LeaderB, riak_repl_console, proxy_get, [["enable","source"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult]),
    Status = rpc:call(LeaderB, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,
    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Done connection replication
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    %% run an initial fullsync, 
    repl_helpers:start_and_wait_until_fullsync_complete13(LeaderA),

    lager:info("User 1 has the test bucket on the secondary cluster now"),
    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(U1C2Config)),

    lager:info("Object written on primary cluster is readable from Source"),
    Obj2 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C1Config),
    ?assertEqual(Object1, proplists:get_value(content, Obj2)),

    lager:info("check we can still read the fullsynced object"),
    Obj3 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    ?assertEqual(Object1, proplists:get_value(content, Obj3)),

    lager:info("delete object_one in Source"),
    erlcloud_s3:delete_object(?TEST_BUCKET, "object_one", U1C1Config),

    %%lager:info("object_one is still visible on secondary cluster"),
    %%Obj9 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    %%?assertEqual(Object1, proplists:get_value(content, Obj9)),

    %% wait for leeway
    timer:sleep(5000),
    %% Run GC in Sink side
    start_and_wait_for_gc(CSSink),

    %% Propagate deleted manifests
    repl_helpers:start_and_wait_until_fullsync_complete13(LeaderA),

    lager:info("object_one is invisible in Sink"),
    ?assertError({aws_error, _},
                 erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config)),

    lager:info("object_one is invisible in Source"),
    ?assertError({aws_error, _},
                 erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C1Config)),

    lager:info("secondary cluster now has 'A' version of object four"),

    %% Run GC in Source side
    start_and_wait_for_gc(CSSource),
    
    %% Make sure no blocks are in Source side
    {ok, Keys0} = rc_helper:list_keys(SourceNodes, blocks, ?TEST_BUCKET),
    ?assertEqual([],
                 rc_helper:filter_tombstones(SourceNodes, blocks,
                                             ?TEST_BUCKET, Keys0)),

    %% Boom!!!!
    %% Make sure no blocks are in Sink side: will fail in Riak CS 2.0
    %% or older. Or turn on {delete_mode, keep}
    {ok, Keys1} = rc_helper:list_keys(SinkNodes, blocks, ?TEST_BUCKET),
    ?assertEqual([],
                 rc_helper:filter_tombstones(SinkNodes, blocks,
                                             ?TEST_BUCKET, Keys1)),

    %% Verify no blocks are in sink and source
    rtcs:pass().

start_and_wait_for_gc(CSNode) ->
    rtcs:gc(rtcs:node_id(CSNode), "batch 1"),
    true = rt:expect_in_log(CSNode,
                            "Finished garbage collection: \\d+ seconds, "
                            "1 batch_count, 0 batch_skips, "
                            "1 manif_count, 4 block_count").
