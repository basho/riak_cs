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

-module(object_get_conditional_test).

%% @doc `riak_test' module for testing conditional object get behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET, "riak-test-bucket").
-define(TEST_KEY,    "riak_test_key1").
-define(ETAG_NOTEXIST, "\"NoTeXiSt\"").

confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    {Content, Etag, ThreeDates} =
        setup_object(?TEST_BUCKET, ?TEST_KEY, UserConfig),
    lager:debug("Etag: ~p~n", [Etag]),
    lager:debug("{Before, LastModified, After}: ~p~n", [ThreeDates]),

    last_modified_condition_test_cases(?TEST_BUCKET, ?TEST_KEY,
                                       Content, ThreeDates, UserConfig),
    match_condition_test_cases(?TEST_BUCKET, ?TEST_KEY,
                               Content, Etag, UserConfig),
    pass.

setup_object(Bucket, Key, UserConfig) ->
    Content = crypto:rand_bytes(400),
    erlcloud_s3:put_object(Bucket, Key, Content, UserConfig),
    Obj = erlcloud_s3:get_object(Bucket, Key, UserConfig),
    ?assertEqual(Content, proplists:get_value(content, Obj)),
    Etag = proplists:get_value(etag, Obj),
    {Before, LastModified, After} = before_and_after_of_last_modified(Obj),
    {Content, Etag, {Before, LastModified, After}}.

before_and_after_of_last_modified(Obj) ->
    Headers = proplists:get_value(headers, Obj),
    LastModified = proplists:get_value("Last-Modified", Headers),
    LastModifiedErlDate = httpd_util:convert_request_date(LastModified),
    LastModifiedSec = calendar:datetime_to_gregorian_seconds(LastModifiedErlDate),
    Before = rfc1123_date(LastModifiedSec - 1),
    After = rfc1123_date(LastModifiedSec + 1),
    %% Sleep 1 sec because webmachine ignores if-modified-since header
    %% if it is future date.
    timer:sleep(1000),
    {Before, LastModified, After}.

rfc1123_date(GregorianSecs) ->
    ErlDate = calendar:gregorian_seconds_to_datetime(GregorianSecs),
    riak_cs_wm_utils:iso_8601_to_rfc_1123(riak_cs_wm_utils:iso_8601_datetime(ErlDate)).

last_modified_condition_test_cases(Bucket, Key, ExpectedContent,
                                   {Before, LastModified, After}, UserConfig) ->
    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_modified_since, Before}], UserConfig),
    not_modified_case(Bucket, Key,
                      [{if_modified_since, LastModified}], UserConfig),
    not_modified_case(Bucket, Key,
                      [{if_modified_since, After}], UserConfig),

    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_unmodified_since, After}], UserConfig),
    precondition_failed_case(Bucket, Key,
                             [{if_unmodified_since, Before}], UserConfig).

match_condition_test_cases(Bucket, Key, ExpectedContent,
                           Etag, UserConfig) ->
    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_match, Etag}], UserConfig),
    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_match, Etag ++ ", " ++ ?ETAG_NOTEXIST}], UserConfig),
    precondition_failed_case(Bucket, Key,
                             [{if_match, ?ETAG_NOTEXIST}], UserConfig),

    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_none_match, ?ETAG_NOTEXIST}], UserConfig),
    not_modified_case(Bucket, Key,
                      [{if_none_match, Etag}], UserConfig),
    not_modified_case(Bucket, Key,
                      [{if_none_match, Etag ++ ", " ++ ?ETAG_NOTEXIST}], UserConfig).

normal_get_case(Bucket, Key, ExpectedContent, Options, UserConfig) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Options, UserConfig),
    ?assertEqual(ExpectedContent, proplists:get_value(content, Obj)).

not_modified_case(Bucket, Key, Options, UserConfig) ->
    ?assertError({aws_error, {http_error, 304, _, _Body}},
                erlcloud_s3:get_object(Bucket, Key, Options, UserConfig)).

precondition_failed_case(Bucket, Key, Options, UserConfig) ->
    ?assertError({aws_error, {http_error, 412, _, _Body}},
                 erlcloud_s3:get_object(Bucket, Key, Options, UserConfig)).
