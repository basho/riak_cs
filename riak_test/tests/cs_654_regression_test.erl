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


-export([confirm/0]).

confirm() ->
    Config = [{riak, rtcs:riak_config()}, {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(2, Config),
    run_test_empty_common_prefixes(UserConfig),
    run_test_no_duplicate_key(UserConfig),
    run_test_no_infinite_loop(UserConfig).

%% Test for the original issue found in Github #654:
%% ```
%% $ s3cmd mb s3://test/
%% $ mkdir a
%% $ for i in {0001..1002}; do echo ${i} > a/${i}.txt; done   # in zsh
%% $ s3cmd put --recursive a s3://test
%% $ s3cmd del --recursive --force s3://test
%% $ s3cmd ls s3://test     # !!FAIL!!
%% '''

-define(TEST_BUCKET_1, "cs-654-test-bucket-1").
-define(KEY_PREFIX_1, "a/").

run_test_empty_common_prefixes(UserConfig) ->
    lager:info("creating bucket ~p", [?TEST_BUCKET_1]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_1, UserConfig)),
    Count = 1002,
    list_objects_test_helper:load_objects(?TEST_BUCKET_1, Count, ?KEY_PREFIX_1, UserConfig),
    list_objects_test_helper:delete_objects(?TEST_BUCKET_1, Count, ?KEY_PREFIX_1, UserConfig),
    ListObjectsOptions = [{delimiter, "/"}],
    ?assertEqual([],
                 proplists:get_value(contents,
                                     erlcloud_s3:list_objects(?TEST_BUCKET_1,
                                                              ListObjectsOptions,
                                                              UserConfig))),
    pass.

%% Test for issue in comment
%% [https://github.com/basho/riak_cs/pull/655#issuecomment-23309088]
%% The comment is reproduced here:
%%
%% When there are both some common prefixes and non-prefixed keys,
%% next start key is "rewinded" by skip_past_prefix_and_delimiter.
%%
%% A situation is like this:
%%
%% 100 active objects under 0/
%% 1 active object whose name is 1.txt
%% 1000 pending_delete objects under 2/
%% ls reports duplicated 1.txt.
%%
%% ```
%% $ s3cmd ls s3://test
%%                        DIR   s3://test/0/
%% 2013-08-27 02:09         9   s3://test/1.txt
%% 2013-08-27 02:09         9   s3://test/1.txt
%% '''

-define(TEST_BUCKET_2, "cs-654-test-bucket-2").
-define(ACTIVE_PREFIX, "0/").
-define(SINGLE_OBJECT, "1.txt").
-define(PENDING_DELETE_PREFIX, "2/").

run_test_no_duplicate_key(UserConfig) ->
    lager:info("creating bucket ~p", [?TEST_BUCKET_2]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_2, UserConfig)),

    list_objects_test_helper:load_objects(?TEST_BUCKET_2, 100, ?ACTIVE_PREFIX,
                                          UserConfig),

    erlcloud_s3:put_object(?TEST_BUCKET_2, ?SINGLE_OBJECT,
                            crypto:rand_bytes(100), UserConfig),

    list_objects_test_helper:load_objects(?TEST_BUCKET_2, 1000,
                                          ?PENDING_DELETE_PREFIX,
                                          UserConfig),
    list_objects_test_helper:delete_objects(?TEST_BUCKET_2, 1000,
                                            ?PENDING_DELETE_PREFIX,
                                            UserConfig),

    ListObjectsOptions = [{delimiter, "/"}],
    Response = erlcloud_s3:list_objects(?TEST_BUCKET_2,
                                        ListObjectsOptions,
                                        UserConfig),
    [SingleResult] = proplists:get_value(contents, Response),
    ?assertEqual("1.txt", proplists:get_value(key, SingleResult)),
    pass.

%% Test for issue in comment
%% [https://github.com/basho/riak_cs/pull/655#issuecomment-23390742]
%% The comment is reproduced here:
%% Found one more issue.
%%
%% Infinite loop happens for list objects request with prefix without delimiter.
%%
%% Assume test bucket has 1100 active keys under prefix 0/.
%% s3cmd -c s3cfg.dev1.alice ls s3://test/0 (no slash at the end) does not respond
%% and CPU is used constantly even after killing s3cmd.

-define(TEST_BUCKET_3, "cs-654-test-bucket-3").
-define(ACTIVE_PREFIX_2, "0/").

run_test_no_infinite_loop(UserConfig) ->
    lager:info("creating bucket ~p", [?TEST_BUCKET_3]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_3, UserConfig)),

    list_objects_test_helper:load_objects(?TEST_BUCKET_3, 1100, ?ACTIVE_PREFIX,
                                          UserConfig),

    ListObjectsOptions = [{delimiter, "/"}, {prefix, "0"}],
    Response = erlcloud_s3:list_objects(?TEST_BUCKET_2,
                                        ListObjectsOptions,
                                        UserConfig),
    [SingleResult] = proplists:get_value(common_prefixes, Response),
    ?assertEqual("0/", proplists:get_value(prefix, SingleResult)),
    pass.
