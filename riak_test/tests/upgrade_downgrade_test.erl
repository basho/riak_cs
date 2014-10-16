%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(upgrade_downgrade_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

confirm() ->
    {_UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1, rtcs:default_configs(), previous),
    %% confirm_initial_stats(query_stats(UserConfig, rtcs:cs_port(hd(RiakNodes)))),
    lager:info("~p", [rt_config:get(rt_nodes)]),
    lager:info("~p", [rt_config:get(rt_versions)]),
    %% lager:info("creating bucket ~p", [?TEST_BUCKET]),
    %% ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    %% ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
    %%     erlcloud_s3:list_buckets(UserConfig)),

    %% Object = crypto:rand_bytes(500),
    %% erlcloud_s3:put_object(?TEST_BUCKET, "object_one", Object, UserConfig),
    %% erlcloud_s3:get_object(?TEST_BUCKET, "object_one", UserConfig),
    %% erlcloud_s3:delete_object(?TEST_BUCKET, "object_one", UserConfig),
    %% erlcloud_s3:list_buckets(UserConfig),

    %% lager:info("Confirming stats"),
    %% Stats1 = query_stats(UserConfig, rtcs:cs_port(hd(RiakNodes))),
    %% confirm_stat_count(Stats1, <<"service_get_buckets">>, 2),
    %% confirm_stat_count(Stats1, <<"object_get">>, 1),
    %% confirm_stat_count(Stats1, <<"object_put">>, 1),
    %% confirm_stat_count(Stats1, <<"object_delete">>, 1),
    rtcs:pass().
