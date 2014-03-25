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

-module(multi_bag_transition_test).

%% @doc `riak_test' module for testing transition from single bag configuration
%% to multiple bag one

-export([confirm/0]).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("eunit/include/eunit.hrl").
%% -include_lib("riak_cs/include/riak_cs.hrl").
-include("riak_cs.hrl").

-define(CS_CURRENT, <<"build_paths.cs_current">>).

-define(OLD_BUCKET,     "multi-bag-transition-old").
-define(NEW_BUCKET,     "multi-bag-transition-new").
-define(OLD_KEY_IN_OLD, "old_key_in_old").
-define(NEW_KEY_IN_OLD, "new_key_in_old").
-define(NEW_KEY_IN_NEW, "new_key_in_new").

confirm() ->
    %% setup single bag cluster at first
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup1x1x1(),
    OldInOldContent = setup_old_bucket_and_key(UserConfig, ?OLD_BUCKET, ?OLD_KEY_IN_OLD),

    transition_to_multi_bag_configuration(UserConfig, lists:zip(CSNodes, RiakNodes)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?NEW_BUCKET, UserConfig)),
    NewInOldContent = rand_content(),
    erlcloud_s3:put_object(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
    NewInNewContent = rand_content(),
    erlcloud_s3:put_object(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

    [BagMaster, BagB, BagC] = RiakNodes,
    assert_whole_content(?OLD_BUCKET, ?OLD_KEY_IN_OLD, OldInOldContent, UserConfig),
    assert_whole_content(?OLD_BUCKET, ?NEW_KEY_IN_OLD, NewInOldContent, UserConfig),
    assert_whole_content(?NEW_BUCKET, ?NEW_KEY_IN_NEW, NewInNewContent, UserConfig),

    %% TODO: s3 list for two buckets.
    [BagMaster, BagB, BagC] = RiakNodes,

    rtcs_bag:assert_object_in_expected_bag(?OLD_BUCKET, ?OLD_KEY_IN_OLD, normal,
                                           RiakNodes, [BagMaster], [BagMaster]),
    rtcs_bag:assert_object_in_expected_bag(?OLD_BUCKET, ?NEW_KEY_IN_OLD, normal,
                                           RiakNodes, [BagMaster], [BagC]),
    rtcs_bag:assert_object_in_expected_bag(?NEW_BUCKET, ?NEW_KEY_IN_NEW, normal,
                                           RiakNodes, [BagB], [BagC]),
    pass.

setup_old_bucket_and_key(UserConfig, Bucket, Key) ->
    lager:info("creating bucket ~p", [Bucket]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
    Content = rand_content(),
    erlcloud_s3:put_object(Bucket, Key, Content, UserConfig),
    Content.

rand_content() ->
    crypto:rand_bytes(4 * 1024 * 1024).

transition_to_multi_bag_configuration(AdminConfig, NodeList) ->
    Config = multi_bag_config(),
    #aws_config{access_key_id=K, secret_access_key=S} = AdminConfig,
    rtcs:stop_cs_and_stanchion_nodes(NodeList),
    rt:pmap(fun({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtcs:update_cs_config(rt_config:get(?CS_CURRENT),
                                          N,
                                          proplists:get_value(cs, Config),
                                          {K, S}),
                    rtcs:start_cs(N)
            end, NodeList),
    [ok = rt:wait_until_pingable(CSNode) || {CSNode, _RiakNode} <- NodeList],
    bag_input(),
    ok.

multi_bag_config() ->
    CustomConfig = [{multi_bag, [{"bag-A", "127.0.0.1", 10017},
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

assert_whole_content(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
