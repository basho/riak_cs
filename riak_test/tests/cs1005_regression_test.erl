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

-module(cs1005_regression_test).

%% @doc regression test for CS issue #1005

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET_CS1005, "test-bucket-cs-1005").
-define(TEST_KEY, "repl-team").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _}} =
        rtcs:setup(2), %%, [{cs, [{riak_cs, {ring_creation_size, 4}}]}]),

    %% prepare 1000 2MB objects
    %% setting up the stage
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    lager:info("creating bucket ~p", [?TEST_BUCKET_CS1005]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET_CS1005, UserConfig)),
    %% The first object
    Binary2MB = crypto:rand_bytes(2*1024*2024),
    lager:info("creating 1000 2MB objects", []),
    
    rt:pmap(fun(P) ->
                    [begin
                         timer:sleep(777),
                         Key = integer_to_list(P) ++ "-" ++ integer_to_list(N),
                         erlcloud_s3:put_object(?TEST_BUCKET_CS1005, Key,
                                                Binary2MB, UserConfig),
                         lager:debug("created ~p", [Key])
                     end || N <- lists:seq(1, 10)]
            end, lists:seq(1, 8)),

    lager:info("running concurrent writers", []),

    {ok, Watcher} = rtcs_stats_checker:start_link(RiakNodes,
                                                  fun() -> ok end),

    Writers = rtcs_object_writer:start_writers(?TEST_BUCKET_CS1005,
                                               ?TEST_KEY,
                                               2*1024,
                                               UserConfig,
                                               777, 10),

    timer:sleep(10000),

    lager:info("leaving node2"),
    Node2 = hd(tl(RiakNodes)),
    rt:leave(Node2),
    ?assertEqual(ok, rt:wait_until_unpingable(Node2)),

    lager:info("stopping concurrent writers", []),
    rtcs_object_writer:stop_writers(Writers),
    R = rtcs_stats_checker:stop(Watcher),
    lager:info("final sibling state:", [R]),
    pass.
