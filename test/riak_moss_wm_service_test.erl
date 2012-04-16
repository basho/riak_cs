%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_service_test).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("eunit/include/eunit.hrl").

service_test_() ->
    {setup,
     fun riak_moss_wm_test_utils:setup/0,
     fun riak_moss_wm_test_utils:teardown/1,
     [
     ]}.
