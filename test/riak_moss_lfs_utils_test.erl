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
      fun test_is_manifest/0
     ]}.

test_not_manifest() ->
    ?assertNot(riak_moss_lfs_utils:is_manifest(foo)).

test_is_manifest() ->
    Manifest = riak_moss_lfs_utils:new_manifest({<<"foo">>, <<"bar">>},
            <<"uuid">>,
            dict:new(),
            (10485760 * 100), %% block size * 100
            <<"2522ccc1ca2a458eca94a9576d4b71c2">>),
    ?assert(riak_moss_lfs_utils:is_manifest(Manifest)).




