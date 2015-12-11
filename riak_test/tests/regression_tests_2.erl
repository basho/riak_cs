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

%% @doc regression_tests running with two node cluster, while
%% regression_tests.erl is for single node cluster In case of
%% rtcs:setup(2) with vanilla CS setup used. Otherwise feel free to
%% create an independent module like cs743_regression_test.


-module(regression_tests_2).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(2),

    ok = verify_cs631(UserConfig, "cs-631-test-bukcet"),
    ok = verify_cs654(UserConfig),
    ok = verify_cs781(UserConfig, "cs-781-test-bucket"),
    ok = verify_cs1255(UserConfig, "cs-1255-test-bucket"),

    %% Append your next regression tests here

    rtcs:pass().


%% @doc Integration test for [https://github.com/basho/riak_cs/issues/631]
verify_cs631(UserConfig, BucketName) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),
    test_unknown_canonical_id_grant_returns_400(UserConfig, BucketName),
    test_canned_acl_and_grants_returns_400(UserConfig, BucketName),
    ok.

-define(KEY_1, "key-1").
-define(KEY_2, "key-2").
-define(VALUE, <<"632-test-value">>).

test_canned_acl_and_grants_returns_400(UserConfig, BucketName) ->
    Acl = [{acl, public_read}],
    Headers = [{"x-amz-grant-write", "email=\"doesnmatter@example.com\""}],
    ?assertError({aws_error, {http_error, 400, _, _}},
                 erlcloud_s3:put_object(BucketName, ?KEY_1, ?VALUE,
                                        Acl, Headers, UserConfig)).

test_unknown_canonical_id_grant_returns_400(UserConfig, BucketName) ->
    Acl = [],
    Headers = [{"x-amz-grant-write", "id=\"badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbad9badbad\""}],
    ?assertError({aws_error, {http_error, 400, _, _}},
                 erlcloud_s3:put_object(BucketName, ?KEY_2, ?VALUE,
                                        Acl, Headers, UserConfig)).

%% @doc Integration test for [https://github.com/basho/riak_cs/issues/654]
verify_cs654(UserConfig) ->
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
    ok.

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
    ok.

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
    ok.



format_int(Int) ->
    binary_to_list(iolist_to_binary(io_lib:format("~4..0B", [Int]))).

%% @doc Integration test for [https://github.com/basho/riak_cs/issues/781]
verify_cs781(UserConfig, BucketName) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),
    Count = 1003,
    [erlcloud_s3:put_object(BucketName,
                            format_int(X),
                            crypto:rand_bytes(100),
                            UserConfig) || X <- lists:seq(1, Count)],
    erlcloud_s3:delete_object(BucketName, format_int(1), UserConfig),
    erlcloud_s3:delete_object(BucketName, format_int(2), UserConfig),
    ?assertEqual(true,
                 proplists:get_value(is_truncated,
                                     erlcloud_s3:list_objects(BucketName,
                                                              [],
                                                              UserConfig))),
    ok.

%% Test for [https://github.com/basho/riak_cs/pull/1255]
verify_cs1255(UserConfig, BucketName) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),
    POSTData = {iolist_to_binary(crypto:rand_bytes(100)), "application/octet-stream"},

    %% put objects using a binary key
    erlcloud_s3:s3_request(UserConfig, put, BucketName, <<"/", 00>>, [], [], POSTData, []),
    erlcloud_s3:s3_request(UserConfig, put, BucketName, <<"/", 01>>, [], [], POSTData, []),
    erlcloud_s3:s3_request(UserConfig, put, BucketName, <<"/valid_key">>, [], [], POSTData, []),

    %% list objects without xmerl which throws error when parsing invalid charactor as XML 1.0
    {_Header, Body} = erlcloud_s3:s3_request(UserConfig, get, BucketName, "/", [], [], <<>>, []),
    ?assertMatch({_, _}, binary:match(list_to_binary(Body), <<"<Key>", 00, "</Key>">>)),
    ?assertMatch({_, _}, binary:match(list_to_binary(Body), <<"<Key>", 01, "</Key>">>)),
    ?assertMatch({_, _}, binary:match(list_to_binary(Body), <<"<Key>valid_key</Key>">>)),

    ok.
