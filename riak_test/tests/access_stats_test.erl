%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------

-module(access_stats_test).
%% @doc Integration test for access statistics.
%% TODO: Only several kinds of stats are covered

-export([confirm/0]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "access-stats-test-1").
-define(KEY, "a").

% default time generate_some_accesses spends doing its thing
-define(DURATION_SECS, 11).

confirm() ->
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(2),
    rt:setup_log_capture(hd(CSNodes)),

    Results = generate_some_accesses(UserConfig),
    lager:debug("Client results: ~p", [Results]),
    flush_access_stats(),
    assert_access_stats(json, UserConfig, Results),
    assert_access_stats(xml, UserConfig, Results),
    verify_stats_lost_logging(UserConfig, RiakNodes, CSNodes),

    rtcs:pass().

generate_some_accesses(UserConfig) ->
    generate_some_accesses(UserConfig, ?DURATION_SECS).

generate_some_accesses(UserConfig, DurationSecs) ->
    Begin = calendar:universal_time(),
    Until = calendar:datetime_to_gregorian_seconds(Begin) + DurationSecs,
    R0 = dict:new(),
    lager:info("creating bucket ~p", [?BUCKET]),
    %% Create bucket
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    R1 = dict:update_counter({"BucketCreate", "Count"}, 1, R0),
    %% do stuff for a while ...
    R2 = generate_some_accesses(UserConfig, Until, R1),
    %% Delete bucket
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?BUCKET, UserConfig)),
    Results = dict:update_counter({"BucketDelete", "Count"}, 1, R2),
    %% Illegal URL such that riak_cs_access_log_handler:handle_event/2 gets {log_access, #wm_log_data{notes=undefined}}
    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:get_object("", "//a", UserConfig)), %% Path-style access
    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:get_object("riak-cs", "pong", UserConfig)),
    {rtcs:datetime(Begin), rtcs:datetime(), dict:to_list(Results)}.

generate_some_accesses(UserConfig, UntilGregSecs, R0) ->
    %% Put random object, twice
    O1Size = 100 + random:uniform(100),
    O2Size = 100 + random:uniform(100),
    _ = erlcloud_s3:put_object(?BUCKET, ?KEY,
            crypto:rand_bytes(O1Size), UserConfig),
    _ = erlcloud_s3:put_object(?BUCKET, ?KEY,
            crypto:rand_bytes(O2Size), UserConfig),
    R1 = dict:update_counter({"KeyWrite", "Count"}, 2, R0),
    R2 = dict:update_counter({"KeyWrite", "BytesIn"}, O1Size + O2Size, R1),
    %% Get object, once
    _ = erlcloud_s3:get_object(?BUCKET, ?KEY, UserConfig),
    R3 = dict:update_counter({"KeyRead", "Count"}, 1, R2),
    R4 = dict:update_counter({"KeyRead", "BytesOut"}, O2Size, R3),
    %% Head Object
    _ = erlcloud_s3:head_object(?BUCKET, ?KEY, UserConfig),
    R5 = dict:update_counter({"KeyStat", "Count"}, 1, R4),
    %% List objects (GET bucket)
    _ = erlcloud_s3:list_objects(?BUCKET, UserConfig),
    R6 = dict:update_counter({"BucketRead", "Count"}, 1, R5),
    %% Delete object
    _ = erlcloud_s3:delete_object(?BUCKET, ?KEY, UserConfig),
    Results = dict:update_counter({"KeyDelete", "Count"}, 1, R6),
    GregSecsNow = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    if
        GregSecsNow < UntilGregSecs ->
            generate_some_accesses(UserConfig, UntilGregSecs, Results);
        true ->
            Results
    end.

flush_access_stats() ->
    Res = rtcs_exec:flush_access(1),
    lager:info("riak-cs-access flush result: ~s", [Res]),
    ExpectRegexp = "All access logs were flushed.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)).

assert_access_stats(Format, UserConfig, {Begin, End, ClientStats}) ->
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

    ?assertEqual(client_result({"BucketCreate", "Count"},       ClientStats),
            sum_samples(Format, "BucketCreate", "Count",        Samples)),
    ?assertEqual(client_result({"KeyWrite",     "Count"},       ClientStats),
            sum_samples(Format, "KeyWrite",     "Count",        Samples)),
    ?assertEqual(client_result({"KeyWrite",     "BytesIn"},     ClientStats),
            sum_samples(Format, "KeyWrite",     "BytesIn",      Samples)),
    ?assertEqual(client_result({"KeyWrite",     "BytesOut"},    ClientStats),
            sum_samples(Format, "KeyWrite",     "BytesOut",     Samples)),
    ?assertEqual(client_result({"KeyRead",      "Count"},       ClientStats),
            sum_samples(Format, "KeyRead",      "Count",        Samples)),
    ?assertEqual(client_result({"KeyRead",      "BytesIn"},     ClientStats),
            sum_samples(Format, "KeyRead",      "BytesIn",      Samples)),
    ?assertEqual(client_result({"KeyRead",      "BytesOut"},    ClientStats),
            sum_samples(Format, "KeyRead",      "BytesOut",     Samples)),
    ?assertEqual(client_result({"KeyStat",      "Count"},       ClientStats),
            sum_samples(Format, "KeyStat",      "Count",        Samples)),
    ?assertEqual(client_result({"KeyStat",      "BytesOut"},    ClientStats),
            sum_samples(Format, "KeyStat",      "BytesOut",     Samples)),
    ?assertEqual(client_result({"BucketRead",   "Count"},       ClientStats),
            sum_samples(Format, "BucketRead",   "Count",        Samples)),
    ?assertEqual(client_result({"KeyDelete",    "Count"},       ClientStats),
            sum_samples(Format, "KeyDelete",    "Count",        Samples)),
    ?assertEqual(client_result({"BucketDelete", "Count"},       ClientStats),
            sum_samples(Format, "BucketDelete", "Count",        Samples)),
    pass.

verify_stats_lost_logging(UserConfig, RiakNodes, CSNodes) ->
    KeyId = UserConfig#aws_config.access_key_id,
    %% one second ought to be enough
    _ = generate_some_accesses(UserConfig, 1),
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

client_result(Key, ResultSet) ->
    proplists:get_value(Key, ResultSet, 0).

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
