%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(mp_upload_test).

%% @doc `riak_test' module for testing multipart upload behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").
-define(TEST_KEY1, "riak_test_key1").
-define(TEST_KEY2, "riak_test_key2").
-define(PART_COUNT, 5).
-define(GOOD_PART_SIZE, 5*1024*1024).
-define(BAD_PART_SIZE, 2*1024*1024).

confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    %% Test cases
    basic_upload_test_case(?TEST_BUCKET, ?TEST_KEY1, UserConfig),
    ok = parts_too_small_test_case(?TEST_BUCKET, ?TEST_KEY1, UserConfig),
    aborted_upload_test_case(?TEST_BUCKET, ?TEST_KEY2, UserConfig),
    nonexistent_bucket_listing_test_case("fake-bucket", UserConfig),

    %% Start 10 uploads for 10 different keys
    Count1 = 10,
    initiate_uploads(?TEST_BUCKET, Count1, UserConfig),

    %% Successively list the in-progress uploads, verify the output,
    %% and abort an upload until all uploads are aborted
    abort_and_verify_uploads(?TEST_BUCKET, Count1, UserConfig),

    %% Start 100 uploads for 100 different keys
    Count2 = 100,
    initiate_uploads(?TEST_BUCKET, Count2, UserConfig),

    %% List uploads and verify all 100 are returned
    UploadList1 = erlcloud_s3_multipart:list_uploads(?TEST_BUCKET, [], UserConfig),
    verify_upload_list(UploadList1, Count2),

    %% @TODO Use max-uploads option to request first 50 results
    %% Options1 = [{max_uploads, 50}],
    %% UploadList2 = erlcloud_s3_multipart:list_uploads(?TEST_BUCKET, Options1, UserConfig),
    %% verify_upload_list(ObjList1, 50, 100),

    %% Initiate uploads for 2 sets of 4 objects with keys that have
    %% a common subdirectory
    Prefix1 = "0/prefix1/",
    Prefix2 = "0/prefix2/",
    initiate_uploads(?TEST_BUCKET, 4, Prefix1, UserConfig),
    initiate_uploads(?TEST_BUCKET, 4, Prefix2, UserConfig),

    %% @TODO Uncomment this block once support for `max-uploads' is done.
    %% Use `max-uploads', `prefix' and `delimiter' to get first 50
    %% results back and verify results are truncated and 2 common
    %% prefixes are returned.
    %% Options2 = [{max_uploads, 50}, {prefix, "0/"}, {delimiter, "/"}],
    %% UploadList3 = erlcloud_s3_multipart:list_uploads(?TEST_BUCKET, Options2, UserConfig),
    %% CommonPrefixes = proplists:get_value(common_prefixes, UploadList3),
    %% ?assert(lists:member([{prefix, Prefix1}], CommonPrefixes)),
    %% ?assert(lists:member([{prefix, Prefix2}], CommonPrefixes)),
    %% verify_upload_list(UploadList3, 48, 100),

    %% @TODO Replace this with the commented-out code blocks above and
    %% below this one once the support for `max-uploads' is in place.
    %% Use `prefix' and `delimiter' to get the active uploads back and
    %% verify that 2 common prefixes are returned.
    Options2 = [{prefix, "0/"}, {delimiter, "/"}],
    UploadList3 = erlcloud_s3_multipart:list_uploads(?TEST_BUCKET, Options2, UserConfig),
    CommonPrefixes1 = proplists:get_value(common_prefixes, UploadList3),
    ?assert(lists:member([{prefix, Prefix1}], CommonPrefixes1)),
    ?assert(lists:member([{prefix, Prefix2}], CommonPrefixes1)),
    ?assertEqual([], proplists:get_value(uploads, UploadList3)),

    %% Use `delimiter' to get the active uploads back and
    %% verify that 2 common prefixes are returned.
    Options3 = [{delimiter, "/"}],
    UploadList4 = erlcloud_s3_multipart:list_uploads(?TEST_BUCKET, Options3, UserConfig),
    CommonPrefixes2 = proplists:get_value(common_prefixes, UploadList4),
    ?assert(lists:member([{prefix, "0/"}], CommonPrefixes2)),
    verify_upload_list(UploadList4, Count2),

    %% @TODO Uncomment this block once support for `max-uploads' is done.
    %% Use `key-marker' and `upload-id-marker' to request
    %% remainder of in-progress upload results
    %% Options3 = [{key_marker, "48"}, {upload_id_marker, "X"}],
    %% UploadList4 = erlcloud_s3_multipart:list_uploads(?TEST_BUCKET, Options3, UserConfig),
    %% verify_upload_list(UploadList4, 52, 100, 49),

    %% Abort all uploads for the bucket
    abort_uploads(?TEST_BUCKET, UserConfig),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),
    pass.

upload_and_assert_parts(Bucket, Key, UploadId, PartCount, Size, Config) ->
    [{X, upload_and_assert_part(Bucket,
                                Key,
                                UploadId,
                                X,
                                generate_part_data(X, Size),
                                Config)}
     || X <- lists:seq(1, PartCount)].

upload_and_assert_part(Bucket, Key, UploadId, PartNum, PartData, Config) ->
    {RespHeaders, _UploadRes} = erlcloud_s3_multipart:upload_part(Bucket, Key, UploadId, PartNum, PartData, Config),
    PartEtag = proplists:get_value("ETag", RespHeaders),
    PartsTerm = erlcloud_s3_multipart:parts_to_term(
                  erlcloud_s3_multipart:list_parts(Bucket, Key, UploadId, [], Config)),
    Parts = proplists:get_value(parts, PartsTerm),
    ?assertEqual(Bucket, proplists:get_value(bucket, PartsTerm)),
    ?assertEqual(Key, proplists:get_value(key, PartsTerm)),
    ?assertEqual(UploadId, proplists:get_value(upload_id, PartsTerm)),
    verify_part(PartEtag, proplists:get_value(PartNum, Parts)),
    PartEtag.

verify_part(_, undefined) ->
    ?assert(false);
verify_part(ExpectedEtag, PartInfo) ->
    ?assertEqual(ExpectedEtag, proplists:get_value(etag, PartInfo)).

generate_part_data(X, Size)
  when 0 =< X, X =< 255 ->
    list_to_binary(
      [X || _ <- lists:seq(1, Size)]).

aborted_upload_test_case(Bucket, Key, Config) ->
    %% Initiate a multipart upload
    lager:info("Initiating multipart upload"),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], Config),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),

    %% Verify the upload id is in list_uploads results and
    %% that the bucket information is correct
    UploadsList1 = erlcloud_s3_multipart:list_uploads(Bucket, [], Config),
    Uploads1 = proplists:get_value(uploads, UploadsList1, []),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsList1)),
    ?assert(upload_id_present(UploadId, Uploads1)),

    lager:info("Uploading parts"),
    _EtagList = upload_and_assert_parts(Bucket,
                                        Key,
                                        UploadId,
                                        ?PART_COUNT,
                                        ?GOOD_PART_SIZE,
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
    UploadsList2 = erlcloud_s3_multipart:list_uploads(Bucket, [], Config),
    Uploads2 = proplists:get_value(uploads, UploadsList2, []),
    ?assertNot(upload_id_present(UploadId, Uploads2)),

    %% List bucket contents and verify key is still not listed
    ObjList2 = erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([], proplists:get_value(contents, ObjList2)).

nonexistent_bucket_listing_test_case(Bucket, Config) ->
    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3_multipart:list_uploads(Bucket, [], Config)).

basic_upload_test_case(Bucket, Key, Config) ->
    %% Initiate a multipart upload
    lager:info("Initiating multipart upload"),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], Config),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),

    %% Verify the upload id is in list_uploads results and
    %% that the bucket information is correct
    UploadsList1 = erlcloud_s3_multipart:list_uploads(Bucket, [], Config),
    Uploads1 = proplists:get_value(uploads, UploadsList1, []),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsList1)),
    ?assert(upload_id_present(UploadId, Uploads1)),

    lager:info("Uploading parts"),
    EtagList = upload_and_assert_parts(Bucket,
                                       Key,
                                       UploadId,
                                       ?PART_COUNT,
                                       ?GOOD_PART_SIZE,
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
    UploadsList2 = erlcloud_s3_multipart:list_uploads(Bucket, [], Config),
    Uploads2 = proplists:get_value(uploads, UploadsList2, []),
    ?assertNot(upload_id_present(UploadId, Uploads2)),

    %% List bucket contents and verify key is now listed
    ObjList2 = erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([Key],
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(contents, ObjList2)]),

    %% Get the object: it better be what we expect
    ExpectedObj = list_to_binary([generate_part_data(X, ?GOOD_PART_SIZE) ||
                                     X <- lists:seq(1, ?PART_COUNT)]),
    GetRes = erlcloud_s3:get_object(Bucket, Key, Config),
    ?assertEqual(ExpectedObj, proplists:get_value(content, GetRes)),

    %% Delete uploaded object
    erlcloud_s3:delete_object(Bucket, Key, Config),

    %% List bucket contents and verify empty
    ObjList3 = erlcloud_s3:list_objects(Bucket, Config),
    ?assertEqual([], proplists:get_value(contents, ObjList3)).

parts_too_small_test_case(Bucket, Key, Config) ->
    %% Initiate a multipart upload
    lager:info("Initiating multipart upload (bad)"),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], Config),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),

    lager:info("Uploading parts (bad)"),
    EtagList = upload_and_assert_parts(Bucket,
                                       Key,
                                       UploadId,
                                       ?PART_COUNT,
                                       ?BAD_PART_SIZE,
                                       Config),

    %% Complete upload
    lager:info("Completing multipart upload (bad)"),

    {'EXIT', {{aws_error, {http_error, 400, _, Body}}, _Backtrace}} =
        (catch erlcloud_s3_multipart:complete_upload(Bucket,
                                                     Key,
                                                     UploadId,
                                                     EtagList,
                                                     Config)),
    ?assertMatch({match, _},
                 re:run(Body, "EntityTooSmall", [multiline])),

    Abort = fun() -> erlcloud_s3_multipart:abort_upload(Bucket,
                                                        Key,
                                                        UploadId,
                                                        Config)
            end,
    ?assertEqual(ok, Abort()),
    ?assertError({aws_error, {http_error, 404, _, _}}, Abort()),
    ok.

initiate_uploads(Bucket, Count, Config) ->
    initiate_uploads(Bucket, Count, [], Config).

initiate_uploads(Bucket, Count, KeyPrefix, Config) ->
    [erlcloud_s3_multipart:initiate_upload(Bucket,
                                           KeyPrefix ++ integer_to_list(X),
                                           "text/plain",
                                           [],
                                           Config) || X <- lists:seq(1, Count)].

verify_upload_list(UploadList, ExpectedCount) ->
    verify_upload_list(UploadList, ExpectedCount, ExpectedCount, 1).

%% verify_upload_list(UploadList, ExpectedCount, TotalCount) ->
%%     verify_upload_list(UploadList, ExpectedCount, TotalCount, 1).

verify_upload_list(UploadList, ExpectedCount, TotalCount, 1)
  when ExpectedCount =:= TotalCount ->
    ?assertEqual(lists:sort([integer_to_list(X) || X <- lists:seq(1, ExpectedCount)]),
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(uploads, UploadList)]);
verify_upload_list(UploadList, ExpectedCount, TotalCount, Offset) ->
    ?assertEqual(lists:sublist(
                   lists:sort([integer_to_list(X) || X <- lists:seq(1, TotalCount)]),
                   Offset,
                   ExpectedCount),
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(uploads, UploadList)]).

abort_and_verify_uploads(Bucket, 0, Config) ->
    verify_upload_list(erlcloud_s3_multipart:list_uploads(Bucket, [], Config), 0),
    ok;
abort_and_verify_uploads(Bucket, Count, Config) ->
    UploadList = erlcloud_s3_multipart:list_uploads(Bucket, [], Config),
    verify_upload_list(UploadList, Count),
    Key = integer_to_list(Count),
    UploadId = upload_id_for_key(Key, UploadList),
    erlcloud_s3_multipart:abort_upload(Bucket, Key, UploadId, Config),
    abort_and_verify_uploads(Bucket, Count-1, Config).

upload_id_present(UploadId, UploadList) ->
    [] /= [UploadData || UploadData <- UploadList,
                         proplists:get_value(upload_id, UploadData) =:= UploadId].

upload_id_for_key(Key, UploadList) ->
    Uploads = proplists:get_value(uploads, UploadList),
    [KeyUpload] = [UploadData || UploadData <- Uploads,
                                 proplists:get_value(key, UploadData) =:= Key],
    proplists:get_value(upload_id, KeyUpload).

abort_uploads(Bucket, Config) ->
    UploadList = erlcloud_s3_multipart:list_uploads(Bucket, [], Config),
    [begin
         Key = proplists:get_value(key, Upload),
         UploadId = proplists:get_value(upload_id, Upload),
         erlcloud_s3_multipart:abort_upload(Bucket, Key, UploadId, Config)
     end || Upload <- proplists:get_value(uploads, UploadList)].
