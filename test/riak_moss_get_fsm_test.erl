%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_get_fsm_test).

-export([get_fsm_test_/0]).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:start(lager).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

get_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun foo/0
     ]}.

foo() ->
    ok.
