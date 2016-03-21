%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(error_response_test).

-compile(export_all).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "error-response-test").
-define(BUCKET2, "error-response-test2").
-define(KEY, "a").
-define(ErrNodeId, 2).

confirm() ->
    {UserConfig, {RiakNodes, CSNodes, Stanchion}} = rtcs:setup(2),
    ErrCSNode = lists:nth(?ErrNodeId, CSNodes),
    ErrNode = lists:nth(?ErrNodeId, RiakNodes),
    ErrConfig = rtcs_admin:aws_config(UserConfig, [{port, rtcs_config:cs_port(ErrNode)}]),

    %% setup initial data
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?BUCKET, ?KEY, SingleBlock, UserConfig),

    %% vefity response for timeout during getting a user.
    rt_intercept:add(ErrCSNode, {riak_cs_riak_client, [{{get_user, 2}, get_user_timeout}]}),
    ?assertError({aws_error, {http_error, 503, [], _}},
                 erlcloud_s3:get_object(?BUCKET, ?KEY, ErrConfig)),
    rt_intercept:clean(ErrCSNode, riak_cs_riak_client),

    %% vefity response for timeout during getting block.
    %% FIXME: This should be http_error 503
    rt_intercept:add(ErrCSNode, {riak_cs_block_server, [{{get_block_local, 6}, get_block_local_timeout}]}),
    ?assertError({aws_error, {socket_error, retry_later}}, erlcloud_s3:get_object(?BUCKET, ?KEY, ErrConfig)),
    rt_intercept:clean(ErrCSNode, riak_cs_block_server),

    %% vefity response for timeout during get a bucket on stanchion.
    %% FIXME: This should be http_error 503
    rt_intercept:add(Stanchion, {riakc_pb_socket, [{{get, 5}, get_timeout}]}),
    ?assertError({aws_error, {http_error, 500, [], _}},
                 erlcloud_s3:create_bucket(?BUCKET2, ErrConfig)),
    rt_intercept:clean(Stanchion, riakc_pb_socket),

    %% vefity response for timeout during put a bucket on stanchion.
    %% FIXME: This should be http_error 503
    rt_intercept:add(Stanchion, {riakc_pb_socket, [{{put, 4}, put_timeout}]}),
    ?assertError({aws_error, {http_error, 500, [], _}},
                 erlcloud_s3:create_bucket(?BUCKET2, ErrConfig)),
    rt_intercept:clean(Stanchion, riakc_pb_socket),

    rtcs:pass().

