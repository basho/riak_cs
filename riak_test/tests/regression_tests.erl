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

-module(regression_tests).

%% @doc this module gathers various regression tests which can be
%% separate easily. Regression tests which needs configuration change
%% can be written as different module. In case of rtcs:setup(1) with
%% vanilla CS setup used. Otherwise feel free to create an independent
%% module like cs743_regression_test.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET_CS347, "test-bucket-cs347").

confirm() ->
    {UserConfig, _} = SetupInfo = rtcs:setup(1),

    ok = verify_cs296(SetupInfo, "test-bucket-cs296"),
    ok = verify_cs347(SetupInfo, "test-bucket-cs347"),
    ok = verify_cs436(SetupInfo, "test-bucket-cs436"),
    ok = verify_cs512(UserConfig, "test-bucket-cs512"),

    %% Append your next regression tests here

    rtcs:pass().

%% @doc Regression test for `riak_cs' <a href="https://github.com/basho/riak_cs/issues/296">
%% issue 296</a>. The issue description is: 403 instead of 404 returned when
%% trying to list nonexistent bucket.
verify_cs296(_SetupInfo = {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}}, BucketName) ->
    lager:info("CS296: User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(BucketName, UserConfig)),

    lager:info("creating bucket ~p", [BucketName]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),

    ?assertMatch([{buckets, [[{name, BucketName}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    lager:info("deleting bucket ~p", [BucketName]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(BucketName, UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(BucketName, UserConfig)),
    ok.

%% @doc Regression test for `riak_cs' <a href="https://github.com/basho/riak_cs/issues/347">
%% issue 347</a>. The issue description is: No response body in 404 to the
%% bucket that have never been created once.
verify_cs347(_SetupInfo = {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}}, BucketName) ->

    lager:info("CS347: User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ListObjectRes1 =
        case catch erlcloud_s3:list_objects(BucketName, UserConfig) of
            {'EXIT', {{aws_error, Error}, _}} ->
                Error;
            Result ->
                Result
        end,
    ?assert(rtcs:check_no_such_bucket(ListObjectRes1, "/" ++ ?TEST_BUCKET_CS347 ++ "/")),

    lager:info("creating bucket ~p", [BucketName]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),

    ?assertMatch([{buckets, [[{name, BucketName}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    lager:info("deleting bucket ~p", [BucketName]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(BucketName, UserConfig)),

    ListObjectRes2 =
        case catch erlcloud_s3:list_objects(BucketName, UserConfig) of
            {'EXIT', {{aws_error, Error2}, _}} ->
                Error2;
            Result2 ->
                Result2
        end,
    ?assert(rtcs:check_no_such_bucket(ListObjectRes2, "/" ++ ?TEST_BUCKET_CS347 ++ "/")),
    ok.


%% @doc Regression test for `riak_cs' <a href="https://github.com/basho/riak_cs/issues/436">
%% issue 436</a>. The issue description is: A 500 is returned instead of a 404 when
%% trying to put to a nonexistent bucket.
verify_cs436(_SetupInfo = {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}}, BucketName) ->
    lager:info("CS436: User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}},
                 erlcloud_s3:put_object(BucketName,
                                        "somekey",
                                        crypto:rand_bytes(100),
                                        UserConfig)),

    %% Create and delete test bucket
    lager:info("creating bucket ~p", [BucketName]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),

    ?assertMatch([{buckets, [[{name, BucketName}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    lager:info("deleting bucket ~p", [BucketName]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(BucketName, UserConfig)),

    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    %% Attempt to put object again and ensure result is still 404
    ?assertError({aws_error, {http_error, 404, _, _}},
                 erlcloud_s3:put_object(BucketName,
                                        "somekey",
                                        crypto:rand_bytes(100),
                                        UserConfig)),
    ok.

-define(KEY, "cs512-key").

verify_cs512(UserConfig, BucketName) ->
    %% {ok, UserConfig} = setup(),
    ?assertEqual(ok, erlcloud_s3:create_bucket(BucketName, UserConfig)),
    put_and_get(UserConfig, BucketName, <<"OLD">>),
    put_and_get(UserConfig, BucketName, <<"NEW">>),
    delete(UserConfig, BucketName),
    assert_notfound(UserConfig,BucketName),
    ok.

put_and_get(UserConfig, BucketName, Data) ->
    erlcloud_s3:put_object(BucketName, ?KEY, Data, UserConfig),
    Props = erlcloud_s3:get_object(BucketName, ?KEY, UserConfig),
    ?assertEqual(proplists:get_value(content, Props), Data).

delete(UserConfig, BucketName) ->
    erlcloud_s3:delete_object(BucketName, ?KEY, UserConfig).

assert_notfound(UserConfig, BucketName) ->
    ?assertException(_,
                     {aws_error, {http_error, 404, _, _}},
                     erlcloud_s3:get_object(BucketName, ?KEY, UserConfig)).
