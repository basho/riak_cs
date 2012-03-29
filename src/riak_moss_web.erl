%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Convenience functions for setting up the riak_moss HTTP interface.

-module(riak_moss_web).

-include("riak_moss.hrl").

-export([dispatch_table/0]).

dispatch_table() ->
    case application:get_env(?RIAKCS, auth_bypass) of
        {ok, AuthBypass} ->
            ok;
        undefined ->
            AuthBypass = false
    end,
    [
     {[], riak_moss_wm_service, [{auth_bypass, AuthBypass}]},
     {["user"], riak_moss_wm_user, []},
     {["usage", '*'], riak_moss_wm_usage, [{auth_bypass, AuthBypass}]},
     {[bucket], riak_moss_wm_bucket, [{auth_bypass, AuthBypass}]},
     {[bucket, '*'], riak_moss_wm_key, [{auth_bypass, AuthBypass}]}
    ].
