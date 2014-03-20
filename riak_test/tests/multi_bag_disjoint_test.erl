%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(multi_bag_disjoint_test).

%% @doc `riak_test' module for testing multi bag disjoint configuration

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET,   "riak-test-bucket").
-define(KEY_NORMAL,    "key_normal").
-define(KEY_MULTIPART, "key_multipart").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup1x1x1(multi_bag_config()),
    bag_input(),
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    assert_object_in_expected_bag(RiakNodes, UserConfig, normal),
    assert_object_in_expected_bag(RiakNodes, UserConfig, multipart),
    pass.

multi_bag_config() ->
    CustomConfig =
        [{multi_bag, [{"bag-A", "127.0.0.1", 10017},
                      {"bag-B", "127.0.0.1", 10027},
                      {"bag-C", "127.0.0.1", 10037}]}],
    [{cs, rtcs:cs_config(CustomConfig)}].

weights() ->
    [
     {<<"manifest">>, [
                       [{<<"id">>, <<"bag-B">>}, {<<"weight">>, 100}]
                      ]},
     {<<"block">>, [
                    [{<<"id">>, <<"bag-C">>}, {<<"weight">>, 100}]
                   ]}
    ].

bag_input() ->
    InputRes = rtcs:bag_input(1, mochijson2:encode(weights())),
    lager:info("riak-cs-mc input result: ~s", [InputRes]).

assert_object_in_expected_bag(RiakNodes, UserConfig, UploadType) ->
    {Bucket, Key, Content} = upload(UserConfig, UploadType),
    assert_whole_content(Bucket, Key, Content, UserConfig),
    [_MasterBag, BagB, BagC] = RiakNodes,
    rtcs_bag:assert_object_in_expected_bag(Bucket, Key, UploadType,
                                           RiakNodes, [BagB], [BagC]),
    ok.

upload(UserConfig, normal) ->
    Content = crypto:rand_bytes(mb(4)),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_NORMAL, Content, UserConfig),
    {?TEST_BUCKET, ?KEY_NORMAL, Content};
upload(UserConfig, multipart) ->
    Content = rtcs_multipart:multipart_upload(?TEST_BUCKET, ?KEY_MULTIPART,
                                              [mb(10), mb(5), mb(9) + 123, mb(6), 400],
                                              UserConfig),
    {?TEST_BUCKET, ?KEY_MULTIPART, Content}.

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

assert_whole_content(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
