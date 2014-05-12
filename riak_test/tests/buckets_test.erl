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
-define(KEY_MULTIPLE_BLOCK, "riak_test_key2").

%% keys for multipart uploaded objects
-define(KEY_MP_TINY,        "riak_test_mp_tiny").  % single part, single block
-define(KEY_MP_SMALL,       "riak_test_mp_small"). % single part, multiple blocks
-define(KEY_MP_LARGE,       "riak_test_mp_large"). % multiple parts


confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1),

    verify_create_delete(UserConfig),
    
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    %% setup objects
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),

    lager:info("deleting bucket ~p (to fail)", [?TEST_BUCKET]),
    ?assertError({aws_error, {http_error, _, _, _}}, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    erlcloud_s3:delete_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, UserConfig),

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

    pass.


verify_create_delete(UserConfig) ->
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)).

%% multipart_upload(Bucket, Key, Sizes, Config) ->
%%     InitRes = erlcloud_s3_multipart:initiate_upload(
%%                 Bucket, Key, "text/plain", [], Config),
%%     UploadId = erlcloud_xml:get_text(
%%                  "/InitiateMultipartUploadResult/UploadId", InitRes),
%%     Content = upload_parts(Bucket, Key, UploadId, Config, 1, Sizes, [], []),
%%     basic_get_test_case(Bucket, Key, Content, Config),
%%     Content.

