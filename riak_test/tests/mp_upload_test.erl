-module(mp_upload_test).

%% @doc `riak_test' module for testing multipart upload behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak_test_bucket").
-define(TEST_KEY1, "riak_test_key1").
-define(TEST_KEY2, "riak_test_key2").
-define(PART_COUNT, 5).
-define(PART_SIZE, 20).

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

    %% Test cases
    basic_upload_test_case(?TEST_BUCKET, ?TEST_KEY1, UserConfig),
    aborted_upload_test_case(?TEST_BUCKET, ?TEST_KEY2, UserConfig),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),
    pass.

upload_and_assert_parts(Bucket, Key, UploadId, PartCount, Config) ->
    [upload_and_assert_part(Bucket,
                            Key,
                            UploadId,
                            X,
                            generate_part_data(),
                            Config)
     || X <- lists:seq(1, PartCount)].

upload_and_assert_part(Bucket, Key, UploadId, PartNum, PartData, Config) ->
    {RespHeaders, _UploadRes} = erlcloud_s3_multipart:upload_part(Bucket, Key, UploadId, PartNum, PartData, Config),
    PartEtag = proplists:get_value("etag", RespHeaders),
    PartsTerm = erlcloud_s3_multipart:parts_to_term(
                  erlcloud_s3_multipart:list_parts(Bucket, Key, UploadId, [], Config)),
    Parts = proplists:get_value(parts, PartsTerm),
    ?assertEqual(Bucket, proplists:get_value(bucket, PartsTerm)),
    ?assertEqual(Key, proplists:get_value(key, PartsTerm)),
    ?assertEqual(UploadId, proplists:get_value(upload_id, PartsTerm)),
    verify_part(PartEtag, proplists:get_value(PartNum, Parts)).

verify_part(_, undefined) ->
    ?assert(false);
verify_part(ExpectedEtag, PartInfo) ->
    ?assertEqual(ExpectedEtag, proplists:get_value(etag, PartInfo)).

generate_part_data() ->
    list_to_binary(
      ["a" || _ <- lists:seq(1, ?PART_SIZE)]).

aborted_upload_test_case(Bucket, Key, Config) ->
    %% Initiate a multipart upload
    lager:info("Initiating multipart upload"),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], Config),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),

    %% Verify the upload id is in list_uploads results and
    %% that the bucket information is correct
    UploadsTerm1 = erlcloud_s3_multipart:uploads_to_term(
                     erlcloud_s3_multipart:list_uploads(Bucket, [], Config)),
    Uploads1 = proplists:get_value(uploads, UploadsTerm1, []),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsTerm1)),
    ?assert(proplists:is_defined(UploadId, Uploads1)),

    lager:info("Uploading parts"),
    _EtagList = upload_and_assert_parts(Bucket,
                                       Key,
                                       UploadId,
                                       ?PART_COUNT,
                                       Config),

    %% List bucket contents and verify empty
    ObjList1= erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([], proplists:get_value(contents, ObjList1)),

    %% Abort upload
    lager:info("Aborting multipart upload"),
    ?assertEqual(ok, erlcloud_s3_multipart:abort_upload(Bucket,
                                                           Key,
                                                           UploadId,
                                                           Config)),

    %% List uploads and verify upload id is no longer present
    UploadsTerm2 = erlcloud_s3_multipart:uploads_to_term(
                     erlcloud_s3_multipart:list_uploads(Bucket, [], Config)),
    Uploads2 = proplists:get_value(uploads, UploadsTerm2, []),
    ?assertNot(proplists:is_defined(UploadId, Uploads2)),

    %% List bucket contents and verify key is still not listed
    ObjList2 = erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([], proplists:get_value(contents, ObjList2)).

basic_upload_test_case(Bucket, Key, Config) ->
    %% Initiate a multipart upload
    lager:info("Initiating multipart upload"),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], Config),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),

    %% Verify the upload id is in list_uploads results and
    %% that the bucket information is correct
    UploadsTerm1 = erlcloud_s3_multipart:uploads_to_term(
                     erlcloud_s3_multipart:list_uploads(Bucket, [], Config)),
    Uploads1 = proplists:get_value(uploads, UploadsTerm1, []),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsTerm1)),
    ?assert(proplists:is_defined(UploadId, Uploads1)),

    lager:info("Uploading parts"),
    EtagList = upload_and_assert_parts(Bucket,
                                       Key,
                                       UploadId,
                                       ?PART_COUNT,
                                       Config),

    %% List bucket contents and verify empty
    ObjList1= erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([], proplists:get_value(contents, ObjList1)),

    %% Complete upload
    lager:info("Completing multipart upload"),
    ?assertEqual(ok, erlcloud_s3_multipart:complete_upload(Bucket,
                                                           Key,
                                                           UploadId,
                                                           EtagList,
                                                           Config)),

    %% List uploads and verify upload id is no longer present
    UploadsTerm2 = erlcloud_s3_multipart:uploads_to_term(
                     erlcloud_s3_multipart:list_uploads(Bucket, [], Config)),
    Uploads2 = proplists:get_value(uploads, UploadsTerm2, []),
    ?assertNot(proplists:is_defined(UploadId, Uploads2)),

    %% List bucket contents and verify key is now listed
    ObjList2 = erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([Key],
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(contents, ObjList2)]),

    %% Delete uploaded object
    erlcloud_s3:delete_object(Bucket, Key, Config),

    %% List bucket contents and verify empty
    ObjList3 = erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([], proplists:get_value(contents, ObjList3)).
