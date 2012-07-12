%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_lfs_utils_test).

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
    ?assertEqual(riak_moss_lfs_utils:block_count(2, 1), 2).

test_block_count_2() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(11, 2), 6).

test_block_count_3() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(100, 100), 1).

test_block_count_4() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(50, 100), 1).
