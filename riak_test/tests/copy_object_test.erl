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

-module(copy_object_test).

%% @doc `riak_test' module for testing copy object behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET,        "riak-test-bucket").
-define(TEST_BUCKET2,       "riak-test-bucket2").
-define(KEY_SINGLE_BLOCK,   "riak_test_key1").
-define(KEY_MULTIPLE_BLOCK, "riak_test_key2").

%% keys for multipart uploaded objects
-define(KEY_MP_TINY,        "riak_test_mp_tiny").  % single part, single block
-define(KEY_MP_SMALL,       "riak_test_mp_small"). % single part, multiple blocks
-define(KEY_MP_LARGE,       "riak_test_mp_large"). % multiple parts


confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET2]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET2, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    non_mp_copy_cases(UserConfig),
    mp_copy_cases(UserConfig),
    pass.

non_mp_copy_cases(UserConfig) ->
    %% setup objects
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),
    MultipleBlock = crypto:rand_bytes(4000000), % not aligned to block boundary
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock, UserConfig),

    %% basic copy test cases
    basic_copy_test_case(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, ?TEST_BUCKET, ?KEY_SINGLE_BLOCK++"-copy1", SingleBlock, UserConfig),
    basic_copy_test_case(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, ?TEST_BUCKET2, ?KEY_SINGLE_BLOCK++"-copy2", SingleBlock, UserConfig),
    basic_copy_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, ?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK++"-copy1", MultipleBlock, UserConfig),
    basic_copy_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, ?TEST_BUCKET2, ?KEY_MULTIPLE_BLOCK++"-copy2", MultipleBlock, UserConfig),

    %% condtional copy test cases
    %% @TODO
    ok.

mp_copy_cases(_UserConfig) ->
    %% @TODO
    ok.

basic_copy_test_case(SrcBucket, SrcKey, DestBucket, DestKey, ExpectedContent, Config) ->
    _CopyResult1 = erlcloud_s3:copy_object(DestBucket, DestKey, SrcBucket, SrcKey, Config),
    Obj = erlcloud_s3:get_object(DestBucket, DestKey, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
