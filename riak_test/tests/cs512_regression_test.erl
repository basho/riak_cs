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

-module(cs512_regression_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "riak-test-bucket").
-define(KEY, "test-key").

confirm() ->
    {ok, UserConfig} = setup(),
    put_and_get(UserConfig, <<"OLD">>),
    put_and_get(UserConfig, <<"NEW">>),
    delete(UserConfig),
    assert_notfound(UserConfig),
    pass.

put_and_get(UserConfig, Data) ->
    erlcloud_s3:put_object(?BUCKET, ?KEY, Data, UserConfig),
    Props = erlcloud_s3:get_object(?BUCKET, ?KEY, UserConfig),
    ?assertEqual(proplists:get_value(content, Props), Data).

delete(UserConfig) ->
    erlcloud_s3:delete_object(?BUCKET, ?KEY, UserConfig).

assert_notfound(UserConfig) ->
    ?assertException(_,
        {aws_error, {http_error, 404, "Object Not Found", _}},
        erlcloud_s3:get_object(?BUCKET, ?KEY, UserConfig)).

setup() ->
    {UserConfig, _} = rtcs:setup(4),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    ?assertMatch([{buckets, [[{name, ?BUCKET}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),
    {ok, UserConfig}.
