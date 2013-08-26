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

-module(cs_654_regression_test).
%% @doc Integration test for [https://github.com/basho/riak_cs/issues/654]

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "cs-654-test-bucket").
-define(KEY_PREFIX, "a/").

-export([confirm/0]).

confirm() ->
    Config = [{riak, rtcs:riak_config()}, {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4, Config),
    run_test(UserConfig).

run_test(UserConfig) ->
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),
    Count = 1002,
    list_objects_test_helper:load_objects(?TEST_BUCKET, Count, ?KEY_PREFIX, UserConfig),
    list_objects_test_helper:delete_objects(?TEST_BUCKET, Count, ?KEY_PREFIX, UserConfig),
    ListObjectsOptions = [{delimiter, "/"}],
    ?assertEqual([],
                 proplists:get_value(contents,
                                     erlcloud_s3:list_objects(?TEST_BUCKET,
                                                              ListObjectsOptions,
                                                              UserConfig))),
    pass.
