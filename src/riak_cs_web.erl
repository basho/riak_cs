%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Convenience functions for setting up the riak_cs HTTP interface.

-module(riak_cs_web).

-export([dispatch_table/0]).

dispatch_table() ->
    case application:get_env(riak_cs, auth_bypass) of
        {ok, AuthBypass} ->
            ok;
        undefined ->
            AuthBypass = false
    end,
    StatsProps = stats_props(AuthBypass),

    [
     {[], riak_cs_wm_service, [{auth_bypass, AuthBypass}]},
     {["riak-cs", "stats"], riak_cs_wm_stats, StatsProps},
     {["riak-cs", "ping"], riak_cs_wm_ping, []},
     {["user"], riak_cs_wm_user, []},
     {["riak-cs", "users"], riak_cs_wm_users, [{auth_bypass, AuthBypass}]},
     {["riak-cs", "user", '*'], riak_cs_wm_user, [{auth_bypass, AuthBypass}]},
     {["usage", '*'], riak_cs_wm_usage, [{auth_bypass, AuthBypass}]},
     {[bucket], riak_cs_wm_bucket, [{auth_bypass, AuthBypass}]},
     {[bucket, '*'], riak_cs_wm_key, [{auth_bypass, AuthBypass}]}
    ].

stats_props(AuthBypass) ->
    [{auth_bypass, AuthBypass}].
