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
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("riak_cs.hrl").

-define(BUCKET1, "storage-stats-test-1").
-define(BUCKET2, "storage-stats-test-2").
-define(BUCKET3, "storage-stats-test-3").

-define(BUCKET4, "storage-stats-test-4").
-define(BUCKET5, "storage-stats-test-5").
-define(BUCKET6, "storage-stats-test-6").
-define(BUCKET7, "storage-stats-test-7").
-define(BUCKET8, "storage-stats-test-8").
-define(KEY, "1").

confirm() ->
    Config = [%{riak, rtcs:riak_config([{riak_kv, [{delete_mode, keep}]}])},
              {riak, rtcs:riak_config()},
              {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1, Config),
    TestSpecs = [store_object(?BUCKET1, UserConfig),
                 delete_object(?BUCKET2, UserConfig),
                 store_objects(?BUCKET3, UserConfig),

                 %% for CS #840 regression
                 {?BUCKET4, 0, 0},
                 {?BUCKET5, 0, 0},
                 {?BUCKET6, 0, 0},
                 store_object(?BUCKET7, UserConfig),
                 {?BUCKET8, 0, 0}
                ],

    verify_cs840_regression(UserConfig, RiakNodes),

    {Begin, End} = calc_storage_stats(),
    {JsonStat, XmlStat} = storage_stats_request(UserConfig, Begin, End),
    lists:map(fun(Spec) ->
                      assert_storage_json_stats(Spec, JsonStat),
                      assert_storage_xml_stats(Spec, XmlStat)
              end, TestSpecs),
    pass.


%% @doc garbage data to check #840 regression,
%% due to this garbages, following tests may fail
%% makes manifest in BUCKET(4,5,6,7) to garbage, which can
%% be generated from former versions of riak cs than 1.4.5
verify_cs840_regression(UserConfig, RiakNodes) ->

    Pid = rt:pbc(hd(RiakNodes)),

    store_object(?BUCKET4, UserConfig),
    store_object(?BUCKET5, UserConfig),
    store_object(?BUCKET6, UserConfig),
    %% bucket7 should be counted
    store_object(?BUCKET8, UserConfig), %% bucket 8 should no be counted

    %% None of thes objects should not be calculated effective in storage
    mess_with_state_various_props(Pid, UserConfig,
                                  %% state=writing, .props=undefined
                                  [{?BUCKET4, ?KEY, writing, undefined},
                                   %% state=writing, /= multipart, normal writing object
                                   {?BUCKET5, ?KEY, writing, []},
                                   %% badly created ongoing multipart upload (not really)
                                   {?BUCKET6, ?KEY, writing, [{multipart, pocketburgerking}]},
                                   %% state=active, .props=undefined
                                   {?BUCKET7, ?KEY, active, undefined}
                                  ]),

    %% this should not be counted
    %% tombstone  (see above adding {delete_mode, keep} to riak_kv
    Bucket = <<"0o:", (crypto:md5(list_to_binary(?BUCKET8)))/binary>>,
    {ok, RiakObject} = riakc_pb_socket:get(Pid, Bucket, list_to_binary(?KEY)),
    %% This leaves a tombstone
    ok = riakc_pb_socket:delete_obj(Pid, RiakObject),

    ok = riakc_pb_socket:stop(Pid),
    ok.
    %% garbage end

mess_with_state_various_props(Pid, UserConfig, VariousProps) ->
    F = fun({CSBucket, CSKey, NewState, Props}) ->
                ?assertEqual([{version_id, "null"}],
                             erlcloud_s3:put_object(CSBucket, CSKey, <<"pocketburgers">>, UserConfig)),
                Bucket = <<"0o:", (crypto:md5(list_to_binary(CSBucket)))/binary>>,
                {ok, RiakObject0} = riakc_pb_socket:get(Pid, Bucket, list_to_binary(CSKey)),
                [{UUID, Manifest0}|_] = hd([binary_to_term(V) || V <- riakc_obj:get_values(RiakObject0)]),
                Manifest1 = Manifest0?MANIFEST{state=NewState, props=Props},
                RiakObject = riakc_obj:update_value(RiakObject0,
                                                    term_to_binary([{UUID, Manifest1}])),
                lager:info("~p", [Manifest1?MANIFEST.props]),
                ok = riakc_pb_socket:put(Pid, RiakObject)
        end,
    lists:foreach(F, VariousProps).


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
    %% TODO workaround to #766
    timer:sleep(1000),
    Res = rtcs:calculate_storage(1),
    lager:info("riak-cs-storage batch result: ~s", [Res]),
    ExpectRegexp = "Batch storage calculation started.\n$",
    ?assertMatch({match, _}, re:run(Res, ExpectRegexp)),
    %% TODO ensure storage calculation finished.
    timer:sleep(1000),
    End = rtcs:datetime(),
    {Begin, End}.

assert_storage_json_stats({Bucket, ExpectedObjects, ExpectedBytes}, Sample) ->
    ?assertEqual(ExpectedObjects, rtcs:json_get([list_to_binary(Bucket), <<"Objects">>],   Sample)),
    ?assertEqual(ExpectedBytes,   rtcs:json_get([list_to_binary(Bucket), <<"Bytes">>],     Sample)),
    ?assert(rtcs:json_get([<<"StartTime">>], Sample) =/= notfound),
    ?assert(rtcs:json_get([<<"EndTime">>],   Sample) =/= notfound),
    pass.

assert_storage_xml_stats({Bucket, ExpectedObjects, ExpectedBytes}, Sample) ->
    ?assertEqual(ExpectedObjects, proplists:get_value('Objects', proplists:get_value(Bucket, Sample))),
    ?assertEqual(ExpectedBytes,   proplists:get_value('Bytes', proplists:get_value(Bucket, Sample))),
    ?assert(proplists:get_value('StartTime', Sample) =/= notfound),
    ?assert(proplists:get_value('EndTime', Sample)   =/= notfound),
    pass.

storage_stats_request(UserConfig, Begin, End) ->
    {storage_stats_json_request(UserConfig, Begin, End),
     storage_stats_xml_request(UserConfig, Begin, End)}.

storage_stats_json_request(UserConfig, Begin, End) ->
    Samples = samples_from_json_request(UserConfig, {Begin, End}),
    lager:debug("Storage samples[json]: ~p", [Samples]),
    ?assertEqual(1, length(Samples)),
    [Sample] = Samples,
    lager:info("Storage sample[json]: ~p", [Sample]),
    Sample.

storage_stats_xml_request(UserConfig, Begin, End) ->
    Samples = samples_from_xml_request(UserConfig, {Begin, End}),
    lager:debug("Storage samples[xml]: ~p", [Samples]),
    ?assertEqual(1, length(Samples)),
    [Sample] = Samples,
    ParsedSample = to_proplist_stats(Sample),
    lager:info("Storage sample[xml]: ~p", [ParsedSample]),
    ParsedSample.

samples_from_json_request(UserConfig, {Begin, End}) ->
    KeyId = UserConfig#aws_config.access_key_id,
    StatsKey = string:join(["usage", KeyId, "bj", Begin, End], "/"),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, UserConfig),
    lager:debug("GET Storage stats response[json]: ~p", [GetResult]),
    Usage = mochijson2:decode(proplists:get_value(content, GetResult)),
    lager:debug("Usage Response[json]: ~p", [Usage]),
    rtcs:json_get([<<"Storage">>, <<"Samples">>], Usage).

samples_from_xml_request(UserConfig, {Begin, End}) ->
    KeyId = UserConfig#aws_config.access_key_id,
    StatsKey = string:join(["usage", KeyId, "bx", Begin, End], "/"),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, UserConfig),
    lager:debug("GET Storage stats response[xml]: ~p", [GetResult]),
    {Usage, _Rest} = xmerl_scan:string(binary_to_list(proplists:get_value(content, GetResult))),
    lager:debug("Usage Response[xml]: ~p", [Usage]),
    xmerl_xpath:string("//Storage/Samples/Sample",Usage).

to_proplist_stats(Sample) ->
    lists:foldl(fun extract_bucket/2, [], Sample#xmlElement.content)
        ++ lists:foldl(fun extract_slice/2, [], Sample#xmlElement.attributes).

extract_bucket(#xmlElement{name='Bucket', attributes=[#xmlAttribute{value=Bucket}], content=Content}, Acc) ->
    [{Bucket, lists:foldl(fun extract_usage/2,[], Content)}|Acc].

extract_slice(#xmlAttribute{name=Name, value=Value}, Acc) ->
    [{Name, Value}|Acc].

extract_usage(#xmlElement{name=Name, content=[Content]}, Acc) ->
    [{Name, extract_value(Content)}|Acc].

extract_value(#xmlText{value=Content}) ->
    list_to_integer(Content).
