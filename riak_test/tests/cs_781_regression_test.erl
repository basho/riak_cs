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

-module(cs_781_regression_test).
%% @doc Integration test for [https://github.com/basho/riak_cs/issues/781]

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").


-export([confirm/0]).

confirm() ->
    Config = [{riak, rtcs:riak_config()}, {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(2, Config),
    run_test(UserConfig).

-define(TEST_BUCKET_1, "cs-781-test-bucket-1").

format_int(Int) ->
    binary_to_list(iolist_to_binary(io_lib:format("~4..0B", [Int]))).

run_test(UserConfig) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_1, UserConfig)),
    Count = 1003,
    [erlcloud_s3:put_object(?TEST_BUCKET_1,
                            format_int(X),
                            crypto:rand_bytes(100),
                            UserConfig) || X <- lists:seq(1, Count)],
    erlcloud_s3:delete_object(?TEST_BUCKET_1, format_int(1), UserConfig),
    erlcloud_s3:delete_object(?TEST_BUCKET_1, format_int(2), UserConfig),
    ?assertEqual(true,
                 proplists:get_value(is_truncated,
                                     erlcloud_s3:list_objects(?TEST_BUCKET_1,
                                                              [],
                                                              UserConfig))).

