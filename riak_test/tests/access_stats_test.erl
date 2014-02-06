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

-module(access_stats_test).
%% @doc Integration test for access statistics.
%% TODO: Only several kinds of stats are covered

-compile(export_all).
-export([confirm/0]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "access-stats-test-1").
-define(KEY, "a").

confirm() ->
    Config = [{riak, rtcs:riak_config()}, {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4, Config),
    {Begin, End} = generate_some_accesses(UserConfig),
    flush_access_stats(),
    assert_access_stats(UserConfig, {Begin, End}),
    pass.

generate_some_accesses(UserConfig) ->
    Begin = rtcs:datetime(),
    lager:info("creating bucket ~p", [?BUCKET]),
    %% Create bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    %% Put 100-byte object, twice
    Block = crypto:rand_bytes(100),
    _ = erlcloud_s3:put_object(?BUCKET, ?KEY, Block, UserConfig),
    _ = erlcloud_s3:put_object(?BUCKET, ?KEY, Block, UserConfig),
    %% Get 100-byte object, once
    _ = erlcloud_s3:get_object(?BUCKET, ?KEY, UserConfig),
    %% Head Object
    _ = erlcloud_s3:head_object(?BUCKET, ?KEY, UserConfig),
    %% List objects (GET bucket)
    _ = erlcloud_s3:list_objects(?BUCKET, UserConfig),
    %% Delete object
    _ = erlcloud_s3:delete_object(?BUCKET, ?KEY, UserConfig),
    %% Delete bucket
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?BUCKET, UserConfig)),
    End = rtcs:datetime(),
    {Begin, End}.

flush_access_stats() ->
    Res = rtcs:flush_access(1),
    lager:info("riak-cs-access flush result: ~s", [Res]),
    ExpectRegexp = "0 more archives to flush\nAll access logs were flushed.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)).

assert_access_stats(UserConfig, {Begin, End}) ->
    KeyId = UserConfig#aws_config.access_key_id,
    StatsKey = lists:flatten(["usage/", KeyId, "/aj/", Begin, "/", End, "/"]),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, UserConfig),
    lager:debug("GET Access stats response: ~p", [GetResult]),
    Usage = mochijson2:decode(proplists:get_value(content, GetResult)),
    lager:debug("Usage Response: ~p", [Usage]),
    Samples = node_samples_from_usage(<<"rcs-dev1@127.0.0.1">>, Usage),
    lager:info("Access samples: ~p", [Samples]),

    ?assertEqual(  1, sum_samples([<<"BucketCreate">>, <<"Count">>], Samples)),
    ?assertEqual(  2, sum_samples([<<"KeyWrite">>, <<"Count">>], Samples)),
    ?assertEqual(200, sum_samples([<<"KeyWrite">>, <<"BytesIn">>], Samples)),
    ?assertEqual(  0, sum_samples([<<"KeyWrite">>, <<"BytesOut">>], Samples)),
    ?assertEqual(  1, sum_samples([<<"KeyRead">>, <<"Count">>], Samples)),
    ?assertEqual(  0, sum_samples([<<"KeyRead">>, <<"BytesIn">>], Samples)),
    ?assertEqual(100, sum_samples([<<"KeyRead">>, <<"BytesOut">>], Samples)),
    ?assertEqual(  1, sum_samples([<<"KeyStat">>, <<"Count">>], Samples)),
    ?assertEqual(  0, sum_samples([<<"KeyStat">>, <<"BytesOut">>], Samples)),
    ?assertEqual(  1, sum_samples([<<"BucketRead">>, <<"Count">>], Samples)),
    ?assertEqual(  1, sum_samples([<<"KeyDelete">>, <<"Count">>], Samples)),
    ?assertEqual(  1, sum_samples([<<"BucketDelete">>, <<"Count">>], Samples)),
    pass.

node_samples_from_usage(Node, Usage) ->
    ListOfNodeStats = rtcs:json_get([<<"Access">>, <<"Nodes">>], Usage),
    lager:debug("ListOfNodeStats: ~p", [ListOfNodeStats]),
    [NodeStats | _] = lists:dropwhile(fun(NodeStats) ->
                                              rtcs:json_get(<<"Node">>, NodeStats) =/= Node
                                      end, ListOfNodeStats),
    rtcs:json_get(<<"Samples">>, NodeStats).

%% Sum up statistics entries in possibly multiple samples
sum_samples(Keys, Samples) ->
    sum_samples(Keys, Samples, 0).
sum_samples(_Keys, [], Sum) ->
    Sum;
sum_samples(Keys, [Sample | Samples], Sum) ->
    InSample = case rtcs:json_get(Keys, Sample) of
                   notfound ->
                       0;
                   Value when is_integer(Value) ->
                       Value
                  end,
    sum_samples(Keys, Samples, Sum + InSample).
