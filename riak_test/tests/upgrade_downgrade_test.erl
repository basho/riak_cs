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

-module(upgrade_downgrade_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(TEST_BUCKET, "riak-test-bucket-foobar").
-define(KEY_SINGLE_BLOCK,   "riak_test_key1").
-define(KEY_MULTIPLE_BLOCK, "riak_test_key2").

confirm() ->
    PrevConfig = rtcs_config:previous_configs(),
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} =
        rtcs:setup(2, PrevConfig, previous),

    lager:info("nodes> ~p", [rt_config:get(rt_nodes)]),
    lager:info("versions> ~p", [rt_config:get(rt_versions)]),

    {ok, Data} = prepare_all_data(UserConfig),
    ok = verify_all_data(UserConfig, Data),

    AdminCreds = {UserConfig#aws_config.access_key_id,
                  UserConfig#aws_config.secret_access_key},
    {_, RiakCurrentVsn} =
        rtcs_dev:riak_root_and_vsn(current, rt_config:get(build_type, oss)),

    %% Upgrade!!!
    [begin
         N = rtcs_dev:node_id(RiakNode),
         lager:debug("upgrading ~p", [N]),
         rtcs_exec:stop_cs(N, previous),
         ok = rt:upgrade(RiakNode, RiakCurrentVsn),
         rt:wait_for_service(RiakNode, riak_kv),
         ok = rtcs_config:upgrade_cs(N, AdminCreds),
         rtcs:set_advanced_conf({cs, current, N},
                                [{riak_cs,
                                  [{riak_host, {"127.0.0.1", rtcs_config:pb_port(1)}}]}]),
         rtcs_exec:start_cs(N, current)
     end
     || RiakNode <- RiakNodes],
    rt:wait_until_ring_converged(RiakNodes),
    rtcs_exec:stop_stanchion(previous),
    rtcs_config:migrate_stanchion(previous, current, AdminCreds),
    rtcs_exec:start_stanchion(current),

    ok = verify_all_data(UserConfig, Data),
    ok = cleanup_all_data(UserConfig),
    lager:info("Upgrading to current successfully done"),

    {ok, Data2} = prepare_all_data(UserConfig),

    {_, RiakPrevVsn} =
        rtcs_dev:riak_root_and_vsn(previous, rt_config:get(build_type, oss)),


    %% Downgrade!!
    rtcs_exec:stop_stanchion(current),
    rtcs_config:migrate_stanchion(current, previous, AdminCreds),
    rtcs_exec:start_stanchion(previous),
    [begin
         N = rtcs_dev:node_id(RiakNode),
         lager:debug("downgrading ~p", [N]),
         rtcs_exec:stop_cs(N, current),
         rt:stop(RiakNode),
         rt:wait_until_unpingable(RiakNode),

         %% get the bitcask directory
         BitcaskDataDir = filename:join([rtcs_dev:node_path(RiakNode), "data", "bitcask"]),
         lager:info("downgrading Bitcask datadir ~s...", [BitcaskDataDir]),
         %% and run the downgrade script:
         %% Downgrading from 2.0 does not work...
         %% https://github.com/basho/bitcask/issues/178
         %% And here's the downgrade script, which is downloaded at `make compile-riak-test`.
         %% https://github.com/basho/bitcask/pull/184
         Result = downgrade_bitcask:main([BitcaskDataDir]),
         lager:info("downgrade script done: ~p", [Result]),

         ok = rt:upgrade(RiakNode, RiakPrevVsn),
         rt:wait_for_service(RiakNode, riak_kv),
         ok = rtcs_config:migrate_cs(current, previous, N, AdminCreds),
         rtcs_exec:start_cs(N, previous)
     end
     || RiakNode <- RiakNodes],
    rt:wait_until_ring_converged(RiakNodes),

    ok = verify_all_data(UserConfig, Data2),
    lager:info("Downgrading to previous successfully done"),

    rtcs:pass().

%% TODO: add more data and test cases
prepare_all_data(UserConfig) ->
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    %% setup objects
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),
    MultipleBlock = crypto:rand_bytes(4000000), % not aligned to block boundary
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock, UserConfig),

    {ok, [{single_block, SingleBlock},
          {multiple_block, MultipleBlock}]}.

%% TODO: add more data and test cases
verify_all_data(UserConfig, Data) ->
    SingleBlock = proplists:get_value(single_block, Data),
    MultipleBlock = proplists:get_value(multiple_block, Data),

    %% basic GET test cases
    basic_get_test_case(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),
    basic_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock, UserConfig),

    ok.

cleanup_all_data(UserConfig) ->
    erlcloud_s3:delete_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, UserConfig),
    erlcloud_s3:delete_object(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, UserConfig),
    erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig),
    ok.

basic_get_test_case(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
