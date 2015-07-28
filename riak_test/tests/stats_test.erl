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

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1),

    lager:info("Confirming initial stats"),
    confirm_initial_stats(cs, UserConfig, rtcs:cs_port(hd(RiakNodes))),
    confirm_initial_stats(stanchion, UserConfig, rtcs:stanchion_port()),

    do_some_api_calls(UserConfig, "bucket1", "bucket2"),

    lager:info("Confirming stats after some operations"),
    confirm_stats(cs, UserConfig, rtcs:cs_port(hd(RiakNodes))),
    confirm_stats(stanchion, UserConfig, rtcs:stanchion_port()),
    rtcs:pass().

confirm_initial_stats(cs, UserConfig, Port) ->
    StatData = query_stats(cs, UserConfig, Port),
    lager:debug("length(StatData) = ~p", [length(StatData)]),
    ?assert(1125 < length(StatData)),
    [begin
         StatKey = list_to_binary(StatType ++ "_out_one"),
         lager:debug("StatKey: ~p~n", [StatKey]),
         ?assert(proplists:is_defined(StatKey, StatData)),
         Value = proplists:get_value(StatKey, StatData),
         ?assertEqual(0, Value)
     end || StatType <- ["service_get",
                         "list_objects_get",
                         "bucket_put",
                         "bucket_delete",
                         "bucket_acl_get",
                         "bucket_acl_put",
                         "object_get",
                         "object_put",
                         "object_head",
                         "object_delete",
                         "object_acl_get",
                         "object_acl_put",
                         "riakc_get_block_n_one",
                         "riakc_put_block",
                         "riakc_delete_block_constrained"
                        ]],
    ?assertEqual(1, proplists:get_value(<<"velvet_create_user_in_one">>, StatData)),
    ?assertEqual(rtcs:request_pool_size() - 1,
                 proplists:get_value(<<"request_pool_workers">>, StatData)),
    ?assertEqual(rtcs:bucket_list_pool_size(),
                 proplists:get_value(<<"bucket_list_pool_workers">>, StatData));

confirm_initial_stats(stanchion, UserConfig, Port) ->
    Stats = query_stats(stanchion, UserConfig, Port),
    lager:debug("length(Stats) = ~p", [length(Stats)]),
    ?assert(160 < length(Stats)),
    [begin
         StatKey = list_to_binary(StatType ++ "_one"),
         lager:debug("StatKey: ~p~n", [StatKey]),
         ?assert(proplists:is_defined(StatKey, Stats)),
         Value = proplists:get_value(StatKey, Stats),
         ?assertEqual(0, Value)
     end || StatType <- ["bucket_create",
                         "bucket_delete",
                         "bucket_put_acl",
                         "riakc_get_cs_bucket",
                         "riakc_delete_cs_bucket",
                         "riakc_get_cs_user",
                         "riakc_list_all_manifest_keys",
                         "riakc_list_all_user_keys"
                        ]],
    confirm_stat_count(Stats, <<"user_create_one">>, 1),
    confirm_stat_count(Stats, <<"riakc_put_cs_user_one">>, 1),

    ?assert(proplists:is_defined(<<"waiting_time_mean">>, Stats)),
    ?assert(proplists:is_defined(<<"waiting_time_median">>, Stats)),
    ?assert(proplists:is_defined(<<"waiting_time_95">>, Stats)),
    ?assert(proplists:is_defined(<<"waiting_time_99">>, Stats)),
    ?assert(proplists:is_defined(<<"waiting_time_100">>, Stats)),
    ?assert(proplists:is_defined(<<"sys_process_count">>, Stats)),
    ?assert(proplists:is_defined(<<"webmachine_mochiweb_active_sockets">>, Stats)),
    ok.

confirm_stats(cs, UserConfig, Port) ->
    confirm_status_cmd(cs, "service_get_in_one"),
    Stats = query_stats(cs, UserConfig, Port),
    confirm_stat_count(Stats, <<"service_get_out_one">>, 2),
    confirm_stat_count(Stats, <<"object_get_out_one">>, 1),
    confirm_stat_count(Stats, <<"object_put_out_one">>, 1),
    confirm_stat_count(Stats, <<"object_delete_out_one">>, 1);
confirm_stats(stanchion, UserConfig, Port) ->
    confirm_status_cmd(stanchion, "bucket_create_one"),
    Stats = query_stats(stanchion, UserConfig, Port),
    confirm_stat_count(Stats, <<"user_create_one">>, 1),
    confirm_stat_count(Stats, <<"bucket_create_one">>, 2),
    confirm_stat_count(Stats, <<"bucket_delete_one">>, 1),
    confirm_stat_count(Stats, <<"riakc_put_cs_user_one">>, 1),
    confirm_stat_count(Stats, <<"riakc_put_cs_bucket_one">>, 3),
    %% this heavy list/gets can be reduced to ONE per delete-bucket (/-o-)/ ⌒ ┤
    confirm_stat_count(Stats, <<"riakc_list_all_manifest_keys_one">>, 2).

do_some_api_calls(UserConfig, Bucket1, Bucket2) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket1, UserConfig)),

    ?assertMatch([{buckets, [[{name, Bucket1}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    Object = crypto:rand_bytes(500),
    erlcloud_s3:put_object(Bucket1, "object_one", Object, UserConfig),
    erlcloud_s3:get_object(Bucket1, "object_one", UserConfig),
    erlcloud_s3:delete_object(Bucket1, "object_one", UserConfig),
    erlcloud_s3:list_buckets(UserConfig),

    ?assertEqual(ok, erlcloud_s3:create_bucket(Bucket2, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(Bucket2, UserConfig)),
    ok.

query_stats(Type, UserConfig, Port) ->
    lager:debug("Querying stats to ~p", [Type]),
    Date = httpd_util:rfc1123_date(),
    {Resource, SignType} = case Type of
                               cs -> {"/riak-cs/stats", s3};
                               stanchion -> {"/stats", velvet}
                           end,
    Cmd="curl -s -H 'Date: " ++ Date ++ "' -H 'Authorization: " ++
        rtcs:make_authorization(SignType, "GET", Resource, [], UserConfig, Date) ++
        "' http://localhost:" ++
        integer_to_list(Port) ++ Resource,
    lager:info("Stats query cmd: ~p", [Cmd]),
    Output = os:cmd(Cmd),
    lager:debug("Stats output=~p~n",[Output]),
    {struct, JsonData} = mochijson2:decode(Output),
    JsonData.

confirm_stat_count(StatData, StatType, ExpectedCount) ->
    lager:debug("confirm_stat_count for ~p", [StatType]),
    ?assertEqual(ExpectedCount, proplists:get_value(StatType, StatData)).

confirm_status_cmd(Type, ExpectedToken) ->
    Cmd = case Type of
              cs ->
                  rtcs:riakcs_statuscmd(rtcs:get_rt_config(cs, current), 1);
              stanchion ->
                  rtcs:stanchion_statuscmd(rtcs:get_rt_config(stanchion, current))
          end,
    Res = os:cmd(Cmd),
    ?assert(string:str(Res, ExpectedToken) > 0).
