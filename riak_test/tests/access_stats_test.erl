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
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "access-stats-test-1").
-define(KEY, "a").

confirm() ->
    Config = [{riak, rtcs_config:riak_config()}, {stanchion, rtcs_config:stanchion_config()},
              {cs, rtcs_config:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(2, Config),
    rt:setup_log_capture(hd(CSNodes)),

    {Begin, End} = generate_some_accesses(UserConfig),
    flush_access_stats(),
    assert_access_stats(json, UserConfig, {Begin, End}),
    assert_access_stats(xml, UserConfig, {Begin, End}),
    verify_stats_lost_logging(UserConfig, RiakNodes, CSNodes),

    rtcs:pass().

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
    Res = rtcs_exec:flush_access(1),
    lager:info("riak-cs-access flush result: ~s", [Res]),
    ExpectRegexp = "All access logs were flushed.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)).

assert_access_stats(Format, UserConfig, {Begin, End}) ->
    KeyId = UserConfig#aws_config.access_key_id,
    FormatInstruction = case Format of
                            json -> "j";
                            xml -> "x"
                        end,
    StatsKey = lists:flatten(["usage/", KeyId, "/a", FormatInstruction, "/",
                              Begin, "/", End, "/"]),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, UserConfig),
    lager:debug("GET Access stats response: ~p", [GetResult]),
    Content = proplists:get_value(content, GetResult),
    Samples = node_samples_from_content(Format, "rcs-dev1@127.0.0.1", Content),
    lager:debug("Access samples (~s): ~p", [Format, Samples]),

    ?assertEqual(  1, sum_samples(Format, "BucketCreate", "Count", Samples)),
    ?assertEqual(  2, sum_samples(Format, "KeyWrite",     "Count", Samples)),
    ?assertEqual(200, sum_samples(Format, "KeyWrite",     "BytesIn", Samples)),
    ?assertEqual(  0, sum_samples(Format, "KeyWrite",     "BytesOut", Samples)),
    ?assertEqual(  1, sum_samples(Format, "KeyRead",      "Count", Samples)),
    ?assertEqual(  0, sum_samples(Format, "KeyRead",      "BytesIn", Samples)),
    ?assertEqual(100, sum_samples(Format, "KeyRead",      "BytesOut", Samples)),
    ?assertEqual(  1, sum_samples(Format, "KeyStat",      "Count", Samples)),
    ?assertEqual(  0, sum_samples(Format, "KeyStat",      "BytesOut", Samples)),
    ?assertEqual(  1, sum_samples(Format, "BucketRead",   "Count", Samples)),
    ?assertEqual(  1, sum_samples(Format, "KeyDelete",    "Count", Samples)),
    ?assertEqual(  1, sum_samples(Format, "BucketDelete", "Count", Samples)),
    pass.

verify_stats_lost_logging(UserConfig, RiakNodes, CSNodes) ->
    KeyId = UserConfig#aws_config.access_key_id,
    {_Begin, _End} = generate_some_accesses(UserConfig),
    %% kill riak
    [ rt:brutal_kill(Node) || Node <- RiakNodes ],
    %% force archive
    flush_access_stats(),
    %% check logs, at same node with flush_access_stats
    CSNode = hd(CSNodes),
    lager:info("Checking log in ~p", [CSNode]),
    ExpectLine = io_lib:format("lost access stat: User=~s, Slice=", [KeyId]),
    lager:debug("expected log line: ~s", [ExpectLine]),
    true = rt:expect_in_log(CSNode, ExpectLine),
    pass.


node_samples_from_content(json, Node, Content) ->
    Usage = mochijson2:decode(Content),
    ListOfNodeStats = rtcs:json_get([<<"Access">>, <<"Nodes">>], Usage),
    lager:debug("ListOfNodeStats: ~p", [ListOfNodeStats]),
    NodeBin = list_to_binary(Node),
    [NodeStats | _] = lists:dropwhile(
                        fun(NodeStats) ->
                                rtcs:json_get(<<"Node">>, NodeStats) =/= NodeBin
                        end, ListOfNodeStats),
    rtcs:json_get(<<"Samples">>, NodeStats);
node_samples_from_content(xml, Node, Content) ->
    {Usage, _Rest} = xmerl_scan:string(unicode:characters_to_list(Content, utf8)),
    xmerl_xpath:string("/Usage/Access/Nodes/Node[@name='" ++ Node ++ "']/Sample", Usage).

sum_samples(json, OperationType, StatsKey, Data) ->
    sum_samples_json([list_to_binary(OperationType), list_to_binary(StatsKey)], Data);
sum_samples(xml, OperationType, StatsKey, Data) ->
    sum_samples_xml(OperationType, StatsKey, Data).

%% Sum up statistics entries in possibly multiple samples
sum_samples_json(Keys, Samples) ->
    sum_samples_json(Keys, Samples, 0).
sum_samples_json(_Keys, [], Sum) ->
    Sum;
sum_samples_json(Keys, [Sample | Samples], Sum) ->
    InSample = case rtcs:json_get(Keys, Sample) of
                   notfound ->
                       0;
                   Value when is_integer(Value) ->
                       Value
               end,
    sum_samples_json(Keys, Samples, Sum + InSample).

sum_samples_xml(OperationType, StatsKey, Samples) ->
    lists:sum([list_to_integer(T#xmlText.value) ||
                  Sample <- Samples,
                  T <- xmerl_xpath:string(
                         "/Sample/Operation[@type='" ++ OperationType ++ "']/" ++
                             StatsKey ++ " /text()",
                         Sample)]).
