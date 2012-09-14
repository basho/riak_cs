-module(repl_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

cs_port(Node) ->
    8370 + rtdev:node_id(Node).

confirm() ->
    Path = case os:getenv("RT_TARGET_CURRENT") of
        false -> lager:info("RT_TARGET_CURRENT not defined."),
                 lager:info("Defaulting to /tmp/rt/current"),
            "/tmp/rt/current";
        Value -> Value
    end,
    rt:set_config(csroot,Path),

    EEConfig =
    [
        lager_config(),
        {riak_core,
            [{default_bucket_props, [{allow_mult, true}]}]},
        {riak_kv,
            [
                {add_paths, [Path ++ "/cs/rtdev1/lib/riak_cs-1.1.0/ebin"]},
                {storage_backend, riak_cs_kv_multi_backend},
                {multi_backend_prefix_list, [{<<"0b:">>, be_blocks}]},
                {multi_backend_default, be_default},
                {multi_backend,
                    [{be_default, riak_kv_eleveldb_backend,
                            [
                                {max_open_files, 50},
                                {data_root, "./leveldb"}
                            ]},
                        {be_blocks, riak_kv_bitcask_backend,
                            [
                                {data_root, "./bitcask"}
                            ]}
                    ]}
            ]},
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {proxy_get, enabled}
            ]}
    ],


    %% Note that riak_pb_port points to the first devrel
    StanchionConfig =
    [
        lager_config(),
        {stanchion,
            [
                {stanchion_port, 9095},
                {riak_pb_port, 8081}
            ]}],

    {RiakNodes, _CSNodes, _Stanchion} = rtcs:deploy_nodes(6, [{riak, EEConfig}, {stanchion, StanchionConfig},
            {cs, cs_config(1)}]),

    timer:sleep(1000),

    {ANodes, BNodes} = lists:split(3, RiakNodes),

    %{ACSNodes, BCSNodes} = lists:split(3, RiakNodes),

    lager:info("Build cluster A"),
    replication:make_cluster(ANodes),

    lager:info("Build cluster B"),
    replication:make_cluster(BNodes),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    erlcloud:start(),
    timer:sleep(3000),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    {A,B,C} = erlang:now(),
    EmailAddr = lists:flatten(io_lib:format("~p~p~p@basho.com", [A,B,C])),
    {AccessKeyId, SecretAccessKey, _Id} =
        rtcs:create_admin_user(cs_port(AFirst), EmailAddr,"Test User"),
    lager:info("Created user ~p with keys ~p ~p", [EmailAddr, AccessKeyId,
            SecretAccessKey]),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(ANodes)),"http"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets()),

    lager:info("creating bucket riak_test_bucket"),
    ?assertEqual(ok, erlcloud_s3:create_bucket("riak_test_bucket")),

    ?assertMatch([{buckets, [[{name, "riak_test_bucket"}, _]]}],
        erlcloud_s3:list_buckets()),


    ?assertEqual([],
        proplists:get_value(contents,
            erlcloud_s3:list_objects("riak_test_bucket"))),

    Object1 = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object("riak_test_bucket", "object_one", Object1),

    ?assertEqual(["object_one"],
        [proplists:get_value(key, O) || O <- proplists:get_value(contents,
                erlcloud_s3:list_objects("riak_test_bucket"))]),


    Obj = erlcloud_s3:get_object("riak_test_bucket", "object_one"),
    ?assertEqual(Object1, proplists:get_value(content,Obj)),

    {D,E,F} = erlang:now(),
    EmailAddr2 = lists:flatten(io_lib:format("~p~p~p@basho.com", [D,E,F])),
    {AccessKeyId2, SecretAccessKey2, _Id2} =
        rtcs:create_admin_user(cs_port(BFirst), EmailAddr2,"Test User2"),
    lager:info("Created user ~p with keys ~p ~p", [EmailAddr2, AccessKeyId2,
            SecretAccessKey2]),

    erlcloud_s3:configure(AccessKeyId2, SecretAccessKey2,
        "localhost",cs_port(hd(BNodes)),"http"),

    lager:info("new user isn't valid on secondary cluster, even though we "
        "created it there"),
    ?assertError({aws_error, _}, erlcloud_s3:list_buckets()),

    erlcloud_s3:configure(AccessKeyId2, SecretAccessKey2,
        "localhost",cs_port(hd(ANodes)),"http"),

    lager:info("new user IS valid on the primary cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets()),

    lager:info("set up replication between clusters"),

    %% setup servers/listeners on A
    Listeners = replication:add_listeners(ANodes),

    %% verify servers are visible on all nodes
    replication:verify_listeners(Listeners),

    %% get the leader for the first cluster
    replication:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),

    %% list of listeners not on the leader node
    NonLeaderListeners = lists:keydelete(LeaderA, 3, Listeners),

    %% setup sites on B
    %% TODO: make `NumSites' an argument
    NumSites = 1,
    {Ip, Port, _} = hd(NonLeaderListeners),
    replication:add_site(hd(BNodes), {Ip, Port, "site1"}),

    %% verify sites are distributed on B
    replication:verify_sites_balanced(NumSites, BNodes),

    %% check the listener IPs were all imported into the site
    replication:verify_site_ips(BFirst, "site1", Listeners),

    replication:start_and_wait_until_fullsync_complete(LeaderA),

    erlcloud_s3:configure(AccessKeyId2, SecretAccessKey2,
        "localhost",cs_port(hd(BNodes)),"http"),

    lager:info("new user is valid on secondary cluster after fullsync, still"
        " no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets()),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),

    lager:info("original user has the test bucket on the secondary cluster"
        " now"),
    ?assertMatch([{buckets, [[{name, "riak_test_bucket"}, _]]}],
        erlcloud_s3:list_buckets()),

    lager:info("Object written on primary cluster is readable from secondary "
        "cluster"),
    Obj2 = erlcloud_s3:get_object("riak_test_bucket", "object_one"),
    ?assertEqual(Object1, proplists:get_value(content,Obj2)),

    lager:info("write 2 more objects to the primary cluster"),
    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(ANodes)),"http"),

    Object2 = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object("riak_test_bucket", "object_two", Object2),

    Object3 = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object("riak_test_bucket", "object_three", Object3),

    lager:info("disconnect the clusters"),
    LeaderB = rpc:call(hd(BNodes), riak_repl_leader, leader_node, []),
    replication:del_site(LeaderB, "site1"),

    lager:info("check we can still read the fullsynced object"),
    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),

    Obj3 = erlcloud_s3:get_object("riak_test_bucket", "object_one"),
    ?assertEqual(Object1, proplists:get_value(content,Obj3)),

    lager:info("check all 3 objects are listed on the secondary cluster"),
    ?assertEqual(["object_one", "object_three", "object_two"],
        [proplists:get_value(key, O) || O <- proplists:get_value(contents,
                erlcloud_s3:list_objects("riak_test_bucket"))]),

    lager:info("check that the 2 other objects can't be read"),
    %% XXX I expect errors here, but I get successful objects containing <<>>
    %?assertError({aws_error, _}, erlcloud_s3:get_object("riak_test_bucket",
            %"object_two")),
    %?assertError({aws_error, _}, erlcloud_s3:get_object("riak_test_bucket",
            %"object_three")),
    Obj4 = erlcloud_s3:get_object("riak_test_bucket", "object_two"),
    ?assertEqual(<<>>, proplists:get_value(content,Obj4)),
    ?assertEqual(integer_to_list(byte_size(Object2)),
        proplists:get_value(content_length,Obj4)),
    Obj5 = erlcloud_s3:get_object("riak_test_bucket", "object_three"),
    ?assertEqual(<<>>, proplists:get_value(content,Obj5)),
    ?assertEqual(integer_to_list(byte_size(Object3)),
        proplists:get_value(content_length,Obj5)),

    lager:info("reconnect clusters"),
    replication:add_site(hd(BNodes), {Ip, Port, "site1"}),

    lager:info("check we can read object_two via proxy get"),
    Obj6 = erlcloud_s3:get_object("riak_test_bucket", "object_two"),
    ?assertEqual(Object2, proplists:get_value(content,Obj6)),

    lager:info("disconnect the clusters again"),
    replication:del_site(LeaderB, "site1"),

    lager:info("check we still can't read object_three"),
    Obj7 = erlcloud_s3:get_object("riak_test_bucket", "object_three"),
    ?assertEqual(<<>>, proplists:get_value(content,Obj7)),

    lager:info("check that proxy getting object_two wrote it locally, so we"
        " can read it"),
    Obj8 = erlcloud_s3:get_object("riak_test_bucket", "object_two"),
    ?assertEqual(Object2, proplists:get_value(content,Obj8)),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(ANodes)),"http"),

    lager:info("delete object_one while clusters are disconnected"),
    erlcloud_s3:delete_object("riak_test_bucket", "object_one"),

    lager:info("reconnect clusters"),
    replication:add_site(hd(BNodes), {Ip, Port, "site1"}),

    lager:info("delete object_two while clusters are connected"),
    erlcloud_s3:delete_object("riak_test_bucket", "object_two"),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),

    lager:info("object_one is still visible on secondary cluster"),
    Obj9 = erlcloud_s3:get_object("riak_test_bucket", "object_one"),
    ?assertEqual(Object1, proplists:get_value(content,Obj9)),

    lager:info("object_two is deleted"),
    ?assertError({aws_error, _}, erlcloud_s3:get_object("riak_test_bucket",
            "object_two")),

    replication:start_and_wait_until_fullsync_complete(LeaderA),
 
    lager:info("object_one is deleted after fullsync"),
    ?assertError({aws_error, _}, erlcloud_s3:get_object("riak_test_bucket",
            "object_one")),

    lager:info("disconnect the clusters again"),
    replication:del_site(LeaderB, "site1"),

    Object3A = crypto:rand_bytes(4194304),
    ?assert(Object3 /= Object3A),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(ANodes)),"http"),

    lager:info("write a new version of object_three"),

    erlcloud_s3:put_object("riak_test_bucket", "object_three", Object3A),

    lager:info("Independantly write different object_four and object_five to bolth clusters"),

    Object4A = crypto:rand_bytes(4194304),
    Object4B = crypto:rand_bytes(4194304),

    Object5A = crypto:rand_bytes(4194304),
    Object5B = crypto:rand_bytes(4194304),

    erlcloud_s3:put_object("riak_test_bucket", "object_four", Object4A),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),

    erlcloud_s3:put_object("riak_test_bucket", "object_four", Object4B),
    erlcloud_s3:put_object("riak_test_bucket", "object_five", Object5B),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(ANodes)),"http"),

    lager:info("delay writing object 5 on primary cluster 1 second after "
        "writing to secondary cluster"),
    timer:sleep(1000),
    erlcloud_s3:put_object("riak_test_bucket", "object_five", Object5A),

    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),

    lager:info("reconnect clusters"),
    replication:add_site(hd(BNodes), {Ip, Port, "site1"}),

    lager:info("secondary cluster has old version of object three"),
    Obj10 = erlcloud_s3:get_object("riak_test_bucket", "object_three"),
    ?assertEqual(Object3, proplists:get_value(content,Obj10)),

    lager:info("secondary cluster has 'B' version of object four"),
    Obj11 = erlcloud_s3:get_object("riak_test_bucket", "object_four"),
    ?assertEqual(Object4B, proplists:get_value(content,Obj11)),

    replication:start_and_wait_until_fullsync_complete(LeaderA),

    lager:info("secondary cluster has new version of object three"),
    Obj12 = erlcloud_s3:get_object("riak_test_bucket", "object_three"),
    ?assertEqual(Object3A, proplists:get_value(content,Obj12)),

    lager:info("secondary cluster has 'B' version of object four"),
    Obj13 = erlcloud_s3:get_object("riak_test_bucket", "object_four"),
    ?assertEqual(Object4B, proplists:get_value(content,Obj13)),

    lager:info("secondary cluster has 'A' version of object five, because it "
        "was written later"),
    Obj14 = erlcloud_s3:get_object("riak_test_bucket", "object_five"),
    ?assertEqual(Object5A, proplists:get_value(content,Obj14)),

    lager:info("write 'A' version of object four again on primary cluster"),
    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),
    erlcloud_s3:put_object("riak_test_bucket", "object_four", Object4A),

    lager:info("secondary cluster now has 'A' version of object four"),
    erlcloud_s3:configure(AccessKeyId, SecretAccessKey,
        "localhost",cs_port(hd(BNodes)),"http"),

    Obj15 = erlcloud_s3:get_object("riak_test_bucket", "object_four"),
    ?assertEqual(Object4A, proplists:get_value(content,Obj15)),
    pass.


cs_config(_N) ->
    [
        lager_config(),
        {riak_cs,
            [
                {proxy_get, enabled},
                {anonymous_user_creation, true},
                {stanchion_port, 9095},
                {cs_root_host, "localhost"}
            ]
        }].

lager_config() ->
    {lager, [
            {handlers, [
                    {lager_console_backend, debug},
                    {lager_file_backend, [
                            {"./log/error.log", error, 10485760, "$D0",5},
                            {"./log/console.log", debug, 10485760, "$D0", 5}
                        ]}
                ]}
        ]}.


