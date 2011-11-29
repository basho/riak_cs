%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_lfs_utils_test).

-export([lfs_utils_test_/0]).

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
      fun test_not_manifest/0,
      fun test_is_manifest/0,
      fun test_block_count_1/0,
      fun test_block_count_2/0,
      fun test_block_count_3/0,
      fun test_block_count_4/0,
      fun test_metadata_from_manifest/0
     ]}.

test_not_manifest() ->
    ?assertNot(riak_moss_lfs_utils:is_manifest(foo)).

test_is_manifest() ->
    Manifest =
        riak_moss_lfs_utils:new_manifest(<<"foo">>,
                                         <<"bar">>,
                                         <<"uuid">>,
                                         1024,
                                         <<"2522ccc1ca2a458eca94a9576d4b71c2">>,
                                         dict:new(),
                                         (10485760 * 100)), %% block size * 100
    ?assert(riak_moss_lfs_utils:is_manifest(Manifest)).

test_block_count_1() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(2, 1), 2).

test_block_count_2() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(11, 2), 6).

test_block_count_3() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(100, 100), 1).

test_block_count_4() ->
    ?assertEqual(riak_moss_lfs_utils:block_count(50, 100), 1).

test_metadata_from_manifest() ->
    Meta = dict:new(),
    Manifest =
        riak_moss_lfs_utils:new_manifest(<<"foo">>,
                                         <<"bar">>,
                                         <<"uuid">>,
                                         1024,
                                         <<"2522ccc1ca2a458eca94a9576d4b71c2">>,
                                         Meta,
                                         (10485760 * 100)), %% block size * 100

    ?assertEqual(Meta, riak_moss_lfs_utils:metadata_from_manifest(Manifest)).
