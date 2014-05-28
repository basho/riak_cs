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

%% keys for multipart uploaded objects
-define(KEY_MP,        "riak_test_mp").  % single part, single block


confirm() ->
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(1),


    %% User 1, Cluster 1 config
    {AccessKeyId, SecretAccessKey} = rtcs:create_user(hd(RiakNodes), 1),
    UserConfig1 = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(hd(RiakNodes))),

    ok = verify_create_delete(UserConfig),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ok = verify_bucket_delete_fails(UserConfig),

    ok = verify_bucket_mpcleanup(UserConfig),

    ok = verify_bucket_mpcleanup_racecond_andfix(UserConfig, UserConfig1,
                                                 RiakNodes, hd(CSNodes)),

    pass.


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
verify_bucket_mpcleanup_racecond_andfix(UserConfig, UserConfig1,
                                        RiakNodes, CSNode) ->
    Key = ?KEY_MP,
    Bucket = ?TEST_BUCKET,
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(Bucket, Key, [], [], UserConfig),
    lager:info("InitUploadRes = ~p", [InitUploadRes]),

    %% Reserve riak object to emulate prior 1.4.5 behavior afterwards
    {ok, ManiObj} = rc_helper:get_riakc_obj(RiakNodes, manifest, Bucket, Key),

    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    %% emulate a race condition, during the deletion MP initiate happened
    ok = rc_helper:update_riakc_obj(RiakNodes, manifest, Bucket, Key, ManiObj),

    %% then fail on creation
    %%TODO: check fail fail fail => 500
    ?assertError({aws_error, {http_error, 500, [], _}},
                 erlcloud_s3:create_bucket(Bucket, UserConfig)),

    ?assertError({aws_error, {http_error, 500, [], _}},
                 erlcloud_s3:create_bucket(Bucket, UserConfig1)),

    %% but we have a cleanup script, for existing system with 1.4.x or earlier
    %% DO cleanup here
    Res = rpc:call(CSNode, riak_cs_console, cleanup_orphan_multipart, []),
    lager:info("Result of cleanup_orphan_multipart: ~p~n", [Res]),

    %% list_keys here? wait for GC?

    %% and Okay, it's clear, another user creates same bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig1)),

    %% Nothing found
    UploadsList2 = erlcloud_s3_multipart:list_uploads(Bucket, [], UserConfig1),
    Uploads2 = proplists:get_value(uploads, UploadsList2, []),
    ?assertEqual([], Uploads2),
    ?assertEqual(Bucket, proplists:get_value(bucket, UploadsList2)),
    ok.
