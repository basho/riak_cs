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

-module(buckets_test).

%% @doc `riak_test' module for testing object get behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET,        "riak-test-bucket").
-define(KEY_SINGLE_BLOCK,   "riak_test_key1").
-define(REGION,             "boom-boom-tokyo-42").

%% keys for multipart uploaded objects
-define(KEY_MP,        "riak_test_mp").  % single part, single block


confirm() ->
    SetupConfig =  [{cs, rtcs:cs_config([{region, ?REGION}])}],
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(1, SetupConfig),


    %% User 1, Cluster 1 config
    {AccessKeyId, SecretAccessKey} = rtcs:create_user(hd(RiakNodes), 1),
    UserConfig1 = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(hd(RiakNodes))),

    ok = verify_create_delete(UserConfig),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ok = verify_bucket_location(UserConfig),

    ok = verify_bucket_delete_fails(UserConfig),

    ok = verify_bucket_mpcleanup(UserConfig),

    ok = verify_bucket_mpcleanup_racecond_and_fix(UserConfig, UserConfig1,
                                                  RiakNodes, hd(CSNodes)),

    ok = verify_cleanup_orphan_mp(UserConfig, UserConfig1, RiakNodes, hd(CSNodes)),

    ok = verify_max_buckets_per_user(UserConfig),

    rtcs:pass().


verify_create_delete(UserConfig) ->
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    ok.

verify_bucket_delete_fails(UserConfig) ->
    %% setup objects
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),

    %% verify bucket deletion fails if any objects exist
    lager:info("deleting bucket ~p (to fail)", [?TEST_BUCKET]),
    ?assertError({aws_error, {http_error, _, _, _}},
                 erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    %% cleanup object
    erlcloud_s3:delete_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, UserConfig),
    ok.


verify_bucket_mpcleanup(UserConfig) ->
    Bucket = ?TEST_BUCKET,
    Key = ?KEY_SINGLE_BLOCK,
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], UserConfig),
    lager:info("InitUploadRes = ~p", [InitUploadRes]),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),
    lager:info("UploadId = ~p", [UploadId]),

    %% make sure that mp uploads created
    UploadsList1 = erlcloud_s3_multipart:list_uploads(Bucket, [], UserConfig),
    Uploads1 = proplists:get_value(uploads, UploadsList1, []),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsList1)),
    ?assert(mp_upload_test:upload_id_present(UploadId, Uploads1)),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    %% check that writing mp uploads never resurrect
    %% after bucket delete
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),
    UploadsList2 = erlcloud_s3_multipart:list_uploads(Bucket, [], UserConfig),
    Uploads2 = proplists:get_value(uploads, UploadsList2, []),
    ?assertEqual([], Uploads2),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsList2)),
    ?assertNot(mp_upload_test:upload_id_present(UploadId, Uploads2)),
    ok.

%% @doc in race condition: on delete_bucket
verify_bucket_mpcleanup_racecond_and_fix(UserConfig, UserConfig1,
                                         RiakNodes, CSNode) ->
    Key = ?KEY_MP,
    Bucket = ?TEST_BUCKET,
    prepare_bucket_with_orphan_mp(Bucket, Key, UserConfig, RiakNodes),

    %% then fail on creation
    %%TODO: check fail fail fail => 500
    ?assertError({aws_error, {http_error, 500, [], _}},
                 erlcloud_s3:create_bucket(Bucket, UserConfig)),

    ?assertError({aws_error, {http_error, 500, [], _}},
                 erlcloud_s3:create_bucket(Bucket, UserConfig1)),

    %% but we have a cleanup script, for existing system with 1.4.x or earlier
    %% DO cleanup here
    case rpc:call(CSNode, riak_cs_console, cleanup_orphan_multipart, []) of
        {badrpc, Error} ->
            lager:error("cleanup_orphan_multipart error: ~p~n", [Error]),
            throw(Error);
        Res ->
            lager:info("Result of cleanup_orphan_multipart: ~p~n", [Res])
    end,

    %% list_keys here? wait for GC?

    %% and Okay, it's clear, another user creates same bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig1)),

    %% Nothing found
    UploadsList2 = erlcloud_s3_multipart:list_uploads(Bucket, [], UserConfig1),
    Uploads2 = proplists:get_value(uploads, UploadsList2, []),
    ?assertEqual([], Uploads2),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsList2)),
    ok.

%% @doc cleanup orphan multipart for 30 buckets (> pool size)
verify_cleanup_orphan_mp(UserConfig, UserConfig1, RiakNodes, CSNode) ->
    [begin
         Suffix = integer_to_list(Index),
         Bucket = ?TEST_BUCKET ++ Suffix,
         Key = ?KEY_MP ++ Suffix,
         prepare_bucket_with_orphan_mp(Bucket, Key, UserConfig, RiakNodes)
     end || Index <- lists:seq(1, 30)],

    %% but we have a cleanup script, for existing system with 1.4.x or earlier
    %% DO cleanup here
    case rpc:call(CSNode, riak_cs_console, cleanup_orphan_multipart, []) of
        {badrpc, Error} ->
            lager:error("cleanup_orphan_multipart error: ~p~n", [Error]),
            throw(Error);
        Res ->
            lager:info("Result of cleanup_orphan_multipart: ~p~n", [Res])
    end,

    %% and Okay, it's clear, another user creates same bucket
    Bucket1 = ?TEST_BUCKET ++ "1",
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket1, UserConfig1)),

    %% Nothing found
    UploadsList = erlcloud_s3_multipart:list_uploads(Bucket1, [], UserConfig1),
    Uploads = proplists:get_value(uploads, UploadsList, []),
    ?assertEqual([], Uploads),
    ?assertEqual(Bucket1, proplists:get_value(bucket, UploadsList)),
    ok.

prepare_bucket_with_orphan_mp(BucketName, Key, UserConfig, RiakNodes) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),
    _InitUploadRes = erlcloud_s3_multipart:initiate_upload(BucketName, Key, [], [], UserConfig),

    %% Reserve riak object to emulate prior 1.4.5 behavior afterwards
    {ok, ManiObj} = rc_helper:get_riakc_obj(RiakNodes, objects, BucketName, Key),

    ?assertEqual(ok, erlcloud_s3:delete_bucket(BucketName, UserConfig)),

    %% emulate a race condition, during the deletion MP initiate happened
    ok = rc_helper:update_riakc_obj(RiakNodes, objects, BucketName, Key, ManiObj).


verify_max_buckets_per_user(UserConfig) ->
    [{buckets, Buckets}] = erlcloud_s3:list_buckets(UserConfig),
    lager:debug("existing buckets: ~p", [Buckets]),
    BucketNameBase = "toomanybuckets",
    [begin
         BucketName = BucketNameBase++integer_to_list(N),
         lager:debug("creating bucket ~p", [BucketName]),
         ?assertEqual(ok,
                      erlcloud_s3:create_bucket(BucketName, UserConfig))
     end
     || N <- lists:seq(1,100-length(Buckets))],
    lager:debug("100 buckets created", []),
    BucketName1 = BucketNameBase ++ "101",
    ?assertError({aws_error, {http_error, 400, [], _}},
                 erlcloud_s3:create_bucket(BucketName1, UserConfig)),
    ok.
    
verify_bucket_location(UserConfig) ->
    ?assertEqual(?REGION,
                 erlcloud_s3:get_bucket_attribute(?TEST_BUCKET, location, UserConfig)).
