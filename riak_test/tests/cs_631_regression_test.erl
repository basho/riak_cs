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

-module(cs_631_regression_test).
%% @doc Integration test for [https://github.com/basho/riak_cs/issues/631]

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "cs-631-test-bucket").
-define(KEY_1, "key-1").
-define(KEY_2, "key-2").
-define(VALUE, <<"632-test-value">>).


-export([confirm/0]).

confirm() ->
    Config = [{riak, rtcs:riak_config()}, {stanchion, rtcs:stanchion_config()},
              {cs, rtcs:cs_config([{fold_objects_for_list_keys, true}])}],
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(2, Config),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    test_unknown_canonical_id_grant_returns_400(UserConfig),
    test_canned_acl_and_grants_returns_400(UserConfig).

test_canned_acl_and_grants_returns_400(UserConfig) ->
    Acl = [{acl, public_read}],
    Headers = [{"x-amz-grant-write", "email=\"doesnmatter@example.com\""}],
    ?assertError({aws_error, {http_error, 400, _, _}},
                erlcloud_s3:put_object(?BUCKET, ?KEY_1, ?VALUE,
                                      Acl, Headers, UserConfig)).

test_unknown_canonical_id_grant_returns_400(UserConfig) ->
    Acl = [],
    Headers = [{"x-amz-grant-write", "id=\"badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbad9badbad\""}],
    ?assertError({aws_error, {http_error, 400, _, _}},
                erlcloud_s3:put_object(?BUCKET, ?KEY_2, ?VALUE,
                                      Acl, Headers, UserConfig)).
