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

-module(riak_cs_lfs_utils_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:start(lager).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

lfs_utils_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun test_block_count_1/0,
      fun test_block_count_2/0,
      fun test_block_count_3/0,
      fun test_block_count_4/0
     ]}.

test_block_count_1() ->
    ?assertEqual(riak_cs_lfs_utils:block_count(2, 1), 2).

test_block_count_2() ->
    ?assertEqual(riak_cs_lfs_utils:block_count(11, 2), 6).

test_block_count_3() ->
    ?assertEqual(riak_cs_lfs_utils:block_count(100, 100), 1).

test_block_count_4() ->
    ?assertEqual(riak_cs_lfs_utils:block_count(50, 100), 1).
