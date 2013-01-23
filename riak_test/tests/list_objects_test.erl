-module(list_objects_test).

%% @doc Integration test for list the contents of a bucket

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak_test_bucket").

confirm() ->
    {RiakNodes, _CSNodes, _Stanchion} = rtcs:setup(4),

    FirstNode = hd(RiakNodes),

    {AccessKeyId, SecretAccessKey} = rtcs:create_user(FirstNode, 1),

    %% User config
    UserConfig = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(FirstNode)),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    %% Put 10 objects in the bucket
    Count1 = 10,
    load_objects(?TEST_BUCKET, Count1, UserConfig),

    %%Successively list the buckets, verify the output, and delete an
    %% object from the bucket until the bucket is empty again.
    delete_and_verify_objects(?TEST_BUCKET, Count1, UserConfig),

    %% Put 100 objects in the bucket
    Count2 = 100,
    load_objects(?TEST_BUCKET, Count2, UserConfig),

    %%Successively list the buckets, verify the output, and delete an
    %% object from the bucket until the bucket is empty again.
    delete_and_verify_objects(?TEST_BUCKET, Count2, UserConfig),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),
    pass.

load_objects(Bucket, Count, Config) ->
    [erlcloud_s3:put_object(Bucket,
                            integer_to_list(X),
                            crypto:rand_bytes(1000),
                            Config) || X <- lists:seq(1,Count)].

verify_object_list(ObjList, ExpectedCount) ->
    ?assertEqual(lists:sort([integer_to_list(X) || X <- lists:seq(1, ExpectedCount)]),
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(contents, ObjList)]).

delete_and_verify_objects(Bucket, 0, Config) ->
    verify_object_list(erlcloud_s3:list_objects(Bucket, Config), 0),
    ok;
delete_and_verify_objects(Bucket, Count, Config) ->
    verify_object_list(erlcloud_s3:list_objects(Bucket, Config), Count),
    erlcloud_s3:delete_object(Bucket, integer_to_list(Count), Config),
    delete_and_verify_objects(Bucket, Count-1, Config).
