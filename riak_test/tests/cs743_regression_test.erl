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

-module(cs743_regression_test).

%% @doc Regression test for `riak_cs' <a href="https://github.com/basho/riak_cs/issues/286">
%% issue 286</a>.

-export([confirm/0]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

confirm() ->
    rtcs_dev:set_conf(cs, [{"stats.storage.archive_period", "1s"}]),
    rtcs_dev:set_advanced_conf(cs, [{riak_cs, [{storage_calc_timeout, 1}]}]),
    {UserConfig, {_RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(2),

    Begin = rtcs:datetime(),
    run_storage_batch(hd(CSNodes)),
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    N = 1024,
    lager:info("creating ~p objects in ~p", [N, ?TEST_BUCKET]),
    ok = etoomanyobjects(N, UserConfig),
    timer:sleep(1000),

    run_storage_batch(hd(CSNodes)),
    timer:sleep(1000),
    End = rtcs:datetime(),

    assert_storage_stats(UserConfig, Begin, End),
    rtcs:pass().

assert_storage_stats(UserConfig, Begin, End) ->
    KeyId = UserConfig#aws_config.access_key_id,
    StatsKey = lists:flatten(["usage/", KeyId, "/bj/", Begin, "/", End, "/"]),
    GetResult = erlcloud_s3:get_object("riak-cs", StatsKey, UserConfig),
    lager:info("Storage stats response: ~p", [GetResult]),
    Usage = mochijson2:decode(proplists:get_value(content, GetResult)),
    lager:info("Storage Usage: ~p", [Usage]),
    Samples = rtcs:json_get([<<"Storage">>, <<"Samples">>], Usage),

    ?assert(lists:any(
              fun(Sample) ->
                      case rtcs:json_get(list_to_binary(?TEST_BUCKET), Sample) of
                          notfound -> false;
                          ResultStr ->
                              ?assert(not is_integer(ResultStr)),
                              ?assertEqual(<<"{error,{timeout,[]}}">>, ResultStr),
                              true
                      end
              end,
              Samples)).
    %% supposed to be "{error, timeout}"

run_storage_batch(CSNode) ->
    {ok, Status0} = rpc:call(CSNode, riak_cs_storage_d, status, []),
    lager:info("~p", [Status0]),
    ok = rpc:call(CSNode, riak_cs_storage_d, start_batch, [[{recalc,true}]]),
    {ok, Status1} = rpc:call(CSNode, riak_cs_storage_d, status, []),
    lager:info("~p", [Status1]),
    %%{ok,
    %% {calculating,[{schedule,[]},{last,undefined},{current,{{2013,12,26},{3,55,29}}},
    %% {next,undefined},{elapsed,0},{users_done,1},{users_skipped,0},{users_left,0}]}}

    {_Status, Result} = Status1,
    1 = proplists:get_value(users_done,Result),
    0 = proplists:get_value(users_skipped,Result),
    0 = proplists:get_value(users_left,Result).

etoomanyobjects(N, UserConfig) ->
    SingleBlock = crypto:rand_bytes(400),
    lists:map(fun(I) ->
                      R = erlcloud_s3:put_object(?TEST_BUCKET, integer_to_list(I),
                                                 SingleBlock, UserConfig),
                      [{version_id,"null"}] = R
              end,
              lists:seq(1,N)),
    ok.
