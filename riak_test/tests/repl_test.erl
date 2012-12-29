-module(repl_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(PROXY_HOST, "localhost").
-define(S3_HOST, "s3.amazonaws.com").
-define(S3_PORT, 80).
-define(DEFAULT_PROTO, "http").
-define(TEST_BUCKET, "riak_test_bucket").

config(Key, Secret, Port) ->
    erlcloud_s3:new(Key,
                    Secret,
                    ?S3_HOST,
                    Port, % inets issue precludes using ?S3_PORT
                    ?DEFAULT_PROTO,
                    ?PROXY_HOST,
                    Port).

create_user(Node, UserIndex) ->
    {A, B, C} = erlang:now(),
    User = "Test User" ++ integer_to_list(UserIndex),
    Email = lists:flatten(io_lib:format("~p~p~p@basho.com", [A, B, C])),
    {KeyId, Secret, _Id} =
        rtcs:create_admin_user(cs_port(Node), Email, User),
    lager:info("Created user ~p with keys ~p ~p", [Email, KeyId, Secret]),
    {KeyId, Secret}.

confirm() ->
    {RiakNodes, _CSNodes, _Stanchion} =
        rtcs:deploy_nodes(4, [{riak, ee_config()},
                              {stanchion, stanchion_config()},
                              {cs, cs_config()}]),

    rt:wait_until_nodes_ready(RiakNodes),

    {ANodes, BNodes} = lists:split(2, RiakNodes),

    lager:info("Build cluster A"),
    repl_helpers:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_helpers:make_cluster(BNodes),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    erlcloud:start(),

    AFirst = hd(ANodes),
    BFirst = hd(BNodes),

    {AccessKeyId, SecretAccessKey} = create_user(AFirst, 1),
    {AccessKeyId2, SecretAccessKey2} = create_user(BFirst, 2),

    %% User 1, Cluster 1 config
    U1C1Config = config(AccessKeyId, SecretAccessKey, cs_port(hd(ANodes))),
    %% User 2, Cluster 1 config
    U2C1Config = config(AccessKeyId2, SecretAccessKey2, cs_port(hd(ANodes))),
    %% User 1, Cluster 2 config
    U1C2Config = config(AccessKeyId, SecretAccessKey, cs_port(hd(BNodes))),
    %% User 2, Cluster 2 config
    U2C2Config = config(AccessKeyId2, SecretAccessKey2, cs_port(hd(BNodes))),

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

    lager:info("set up replication between clusters"),

    %% setup servers/listeners on A
    Listeners = repl_helpers:add_listeners(ANodes),

    %% verify servers are visible on all nodes
    repl_helpers:verify_listeners(Listeners),

    %% get the leader for the first cluster
    repl_helpers:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_repl_leader, leader_node, []),

    %% list of listeners not on the leader node
    NonLeaderListeners = lists:keydelete(LeaderA, 3, Listeners),

    %% setup sites on B
    %% TODO: make `NumSites' an argument
    NumSites = 1,
    {Ip, Port, _} = hd(NonLeaderListeners),
    repl_helpers:add_site(hd(BNodes), {Ip, Port, "site1"}),

    %% verify sites are distributed on B
    repl_helpers:verify_sites_balanced(NumSites, BNodes),

    %% check the listener IPs were all imported into the site
    repl_helpers:verify_site_ips(BFirst, "site1", Listeners),

    repl_helpers:start_and_wait_until_fullsync_complete(LeaderA),

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

    %% Object2 = crypto:rand_bytes(4194304),
    Object2 = crypto:rand_bytes(500),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_two", Object2, U1C1Config),

    %% Object3 = crypto:rand_bytes(4194304),
    Object3 = crypto:rand_bytes(500),

    erlcloud_s3:put_object(?TEST_BUCKET, "object_three", Object3, U1C1Config),

    lager:info("disconnect the clusters"),
    LeaderB = rpc:call(hd(BNodes), riak_repl_leader, leader_node, []),
    repl_helpers:del_site(LeaderB, "site1"),

    timer:sleep(5000),

    lager:info("check we can still read the fullsynced object"),

    Obj3 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    ?assertEqual(Object1, proplists:get_value(content,Obj3)),

    lager:info("check all 3 objects are listed on the secondary cluster"),
    ?assertEqual(["object_one", "object_three", "object_two"],
        [proplists:get_value(key, O) || O <- proplists:get_value(contents,
                erlcloud_s3:list_objects(?TEST_BUCKET, U1C2Config))]),

    lager:info("check that the 2 other objects can't be read"),
    %% XXX I expect errors here, but I get successful objects containing <<>>
    %?assertError({aws_error, _}, erlcloud_s3:get_object(?TEST_BUCKET,
            %"object_two")),
    %?assertError({aws_error, _}, erlcloud_s3:get_object(?TEST_BUCKET,
            %"object_three")),

    Obj4 = erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config),

    %% Check content of Obj4
    ?assertEqual(<<>>, proplists:get_value(content, Obj4)),
    %% Check content_length of Obj4
    ?assertEqual(integer_to_list(byte_size(Object2)),
        proplists:get_value(content_length, Obj4)),

    Obj5 = erlcloud_s3:get_object(?TEST_BUCKET, "object_three", U1C2Config),

    %% Check content of Obj5
    ?assertEqual(<<>>, proplists:get_value(content, Obj5)),
    %% Check content_length of Obj5
    ?assertEqual(integer_to_list(byte_size(Object3)),
        proplists:get_value(content_length, Obj5)),

    lager:info("reconnect clusters"),
    repl_helpers:add_site(hd(BNodes), {Ip, Port, "site1"}),

    lager:info("check we can read object_two via proxy get"),
    Obj6 = erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config),
    ?assertEqual(Object2, proplists:get_value(content, Obj6)),

    lager:info("disconnect the clusters again"),
    repl_helpers:del_site(LeaderB, "site1"),

    lager:info("check we still can't read object_three"),
    Obj7 = erlcloud_s3:get_object(?TEST_BUCKET, "object_three", U1C2Config),
    ?assertEqual(<<>>, proplists:get_value(content, Obj7)),

    lager:info("check that proxy getting object_two wrote it locally, so we"
        " can read it"),
    Obj8 = erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config),
    ?assertEqual(Object2, proplists:get_value(content, Obj8)),

    lager:info("delete object_one while clusters are disconnected"),
    erlcloud_s3:delete_object(?TEST_BUCKET, "object_one", U1C1Config),

    lager:info("reconnect clusters"),
    repl_helpers:add_site(hd(BNodes), {Ip, Port, "site1"}),

    lager:info("delete object_two while clusters are connected"),
    erlcloud_s3:delete_object(?TEST_BUCKET, "object_two", U1C1Config),

    lager:info("object_one is still visible on secondary cluster"),
    Obj9 = erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config),
    ?assertEqual(Object1, proplists:get_value(content, Obj9)),

    lager:info("object_two is deleted"),
    ?assertError({aws_error, _},
                 erlcloud_s3:get_object(?TEST_BUCKET, "object_two", U1C2Config)),

    repl_helpers:start_and_wait_until_fullsync_complete(LeaderA),

    lager:info("object_one is deleted after fullsync"),
    ?assertError({aws_error, _},
                 erlcloud_s3:get_object(?TEST_BUCKET, "object_one", U1C2Config)),

    lager:info("disconnect the clusters again"),
    repl_helpers:del_site(LeaderB, "site1"),

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

    lager:info("reconnect clusters"),
    repl_helpers:add_site(hd(BNodes), {Ip, Port, "site1"}),

    lager:info("secondary cluster has old version of object three"),
    Obj10 = erlcloud_s3:get_object(?TEST_BUCKET, "object_three", U1C2Config),
    ?assertEqual(Object3, proplists:get_value(content, Obj10)),

    lager:info("secondary cluster has 'B' version of object four"),
    Obj11 = erlcloud_s3:get_object(?TEST_BUCKET, "object_four", U1C2Config),
    ?assertEqual(Object4B, proplists:get_value(content, Obj11)),

    repl_helpers:start_and_wait_until_fullsync_complete(LeaderA),

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
    pass.


cs_port(Node) ->
    8070 + rtdev:node_id(Node).

ee_config() ->
    CSCurrent = rt:config(rtdev_path.cs_current),
    [
     lager_config(),
     {riak_core,
      [{default_bucket_props, [{allow_mult, true}]}]},
     {riak_kv,
      [
       {add_paths, [CSCurrent ++ "/dev/dev1/lib/riak_cs/ebin"]},
       {storage_backend, riak_cs_kv_multi_backend},
       {multi_backend_prefix_list, [{<<"0b:">>, be_blocks}]},
       {multi_backend_default, be_default},
       {multi_backend,
        [{be_default, riak_kv_eleveldb_backend,
          [
           {max_open_files, 20},
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
    ].

cs_config() ->
    [
     lager_config(),
     {riak_cs,
      [
       {proxy_get, enabled},
       {anonymous_user_creation, true},
       {riak_pb_port, 10017},
       {stanchion_port, 9095}
      ]
     }].

stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {stanchion_port, 9095},
       {riak_pb_port, 10017}
      ]}].

lager_config() ->
    {lager,
     [
      {handlers,
       [
        {lager_console_backend, debug},
        {lager_file_backend,
         [
          {"./log/error.log", error, 10485760, "$D0",5},
          {"./log/console.log", debug, 10485760, "$D0", 5}
         ]}
       ]}
     ]}.
