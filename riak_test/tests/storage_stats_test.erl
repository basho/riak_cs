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

-module(storage_stats_test).
%% @doc Integration test for storage statistics.

-compile(export_all).
-export([confirm/0]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET1, "storage-stats-test-1").
-define(BUCKET2, "storage-stats-test-2").
-define(BUCKET3, "storage-stats-test-3").
-define(KEY, "1").

confirm() ->
    Config = [{riak, rtcs:riak_config()}, {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1, Config),
    TestSpecs = [store_object(?BUCKET1, UserConfig),
                 delete_object(?BUCKET2, UserConfig),
                 store_objects(?BUCKET3, UserConfig)],
    {Begin, End} = calc_storage_stats(),
    lists:map(fun(Spec) ->
                      assert_storage_stats(UserConfig, Spec, {Begin, End})
              end, TestSpecs),
    pass.

store_object(Bucket, UserConfig) ->
    lager:info("creating bucket ~p", [Bucket]),
    %% Create bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
    %% Put 100-byte object
    Block = crypto:rand_bytes(100),
    ?assertEqual([{version_id, "null"}], erlcloud_s3:put_object(Bucket, ?KEY, Block, UserConfig)),
    ExpectedObjects = 1,
    ExpectedBytes = 100,
    {Bucket, ExpectedObjects, ExpectedBytes}.

delete_object(Bucket, UserConfig) ->
    lager:info("creating bucket ~p", [Bucket]),
    %% Create bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
    %% Put 100-byte object
    Block = crypto:rand_bytes(100),
    ?assertEqual([{version_id, "null"}], erlcloud_s3:put_object(Bucket, ?KEY, Block, UserConfig)),
    ?assertEqual([{delete_marker, false}, {version_id, "null"}], erlcloud_s3:delete_object(Bucket, ?KEY, UserConfig)),
    ExpectedObjects = 0,
    ExpectedBytes = 0,
    {Bucket, ExpectedObjects, ExpectedBytes}.

store_objects(Bucket, UserConfig) ->
    lager:info("creating bucket ~p", [Bucket]),
    %% Create bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket, UserConfig)),
    %% Put 100-byte object 10 times
    Block = crypto:rand_bytes(100),
    [?assertEqual([{version_id, "null"}],
                  erlcloud_s3:put_object(Bucket, integer_to_list(Key), Block, UserConfig))
     || Key <- lists:seq(1, 10)],
    ExpectedObjects = 10,
    ExpectedBytes = 1000,
    {Bucket, ExpectedObjects, ExpectedBytes}.

calc_storage_stats() ->
    Begin = rtcs:datetime(),
    Res = rtcs:calculate_storage(1),
    lager:info("riak-cs-storage batch result: ~s", [Res]),
    ExpectRegexp = "Batch storage calculation started.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)),
    %% TODO ensure storage calculation finished.
    timer:sleep(1000),
    End = rtcs:datetime(),
    {Begin, End}.

assert_storage_stats(UserConfig, {Bucket, ExpectedObjects, ExpectedBytes}, {Begin, End}) ->
    Samples = samples_from_json_request(UserConfig, {Begin, End}),
    lager:debug("Storage samples: ~p", [Samples]),
    ?assertEqual(1, length(Samples)),
    [Sample|_] = Samples,
    lager:info("Storage sample: ~p", [Sample]),

    ?assertEqual(ExpectedObjects, rtcs:json_get([list_to_binary(Bucket), <<"Objects">>],   Sample)),
    ?assertEqual(ExpectedBytes,   rtcs:json_get([list_to_binary(Bucket), <<"Bytes">>],     Sample)),
    ?assert(rtcs:json_get([<<"StartTime">>], Sample) =/= notfound),
    ?assert(rtcs:json_get([<<"EndTime">>],   Sample) =/= notfound),
    pass.

samples_from_json_request(UserConfig, {Begin, End}) ->
    KeyId = UserConfig#aws_config.access_key_id,
    StatsKey = filename:join(["usage", KeyId, "bj", Begin, End]),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, UserConfig),
    lager:debug("GET Storage stats response: ~p", [GetResult]),
    Usage = mochijson2:decode(proplists:get_value(content, GetResult)),
    lager:debug("Usage Response: ~p", [Usage]),
    rtcs:json_get([<<"Storage">>, <<"Samples">>], Usage).
