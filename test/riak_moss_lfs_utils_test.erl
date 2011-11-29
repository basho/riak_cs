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
      fun test_not_manifest/0
     ]}.

test_not_manifest() ->
    ?assertNot(riak_moss_lfs_utils:is_manifest(foo)).
