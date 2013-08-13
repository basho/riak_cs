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

-module(stats_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),

    confirm_initial_stats(query_stats(UserConfig, rtcs:cs_port(hd(RiakNodes)))),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    Object = crypto:rand_bytes(500),
    erlcloud_s3:put_object(?TEST_BUCKET, "object_one", Object, UserConfig),
    erlcloud_s3:get_object(?TEST_BUCKET, "object_one", UserConfig),
    erlcloud_s3:delete_object(?TEST_BUCKET, "object_one", UserConfig),
    erlcloud_s3:list_buckets(UserConfig),

    lager:info("Confirming stats"),
    Stats1 = query_stats(UserConfig, rtcs:cs_port(hd(RiakNodes))),
    confirm_stat_count(Stats1, <<"service_get_buckets">>, 2),
    confirm_stat_count(Stats1, <<"object_get">>, 1),
    confirm_stat_count(Stats1, <<"object_put">>, 1),
    confirm_stat_count(Stats1, <<"object_delete">>, 1),
    pass.

query_stats(UserConfig, Port) ->
    lager:debug("Querying stats"),
    Date = httpd_util:rfc1123_date(),
    Resource = "/riak-cs/stats",
    Cmd="curl -s -H 'Date: " ++ Date ++ "' -H 'Authorization: " ++
        rtcs:make_authorization("GET", Resource, [], UserConfig, Date) ++ "' http://localhost:" ++
        integer_to_list(Port) ++ Resource,
    lager:info("Stats query cmd: ~p", [Cmd]),
    Output = os:cmd(Cmd),
    lager:debug("Stats output=~p~n",[Output]),
    {struct, JsonData} = mochijson2:decode(Output),
    JsonData.

confirm_initial_stats(StatData) ->
    %% Check for values for all meters to be 0 when system is initially started
    [?assertEqual([0,0.0,0.0,0.0,0.0,0.0],
                  proplists:get_value(StatType, StatData))
                  || StatType <- [<<"block_get">>,
                                  <<"block_put">>,
                                  <<"block_delete">>,
                                  <<"service_get_buckets">>,
                                  <<"bucket_list_keys">>,
                                  <<"bucket_create">>,
                                  <<"bucket_delete">>,
                                  <<"bucket_get_acl">>,
                                  <<"bucket_put_acl">>,
                                  <<"object_get">>,
                                  <<"object_put">>,
                                  <<"object_head">>,
                                  <<"object_delete">>,
                                  <<"object_get_acl">>,
                                  <<"object_put_acl">>]],
    ?assertEqual([7,0,1], proplists:get_value(<<"request_pool">>, StatData)),
    ?assertEqual([2,0,0], proplists:get_value(<<"bucket_list_pool">>, StatData)).

confirm_stat_count(StatData, StatType, ExpectedCount) ->
    ?assertEqual(ExpectedCount, hd(proplists:get_value(StatType, StatData))).
