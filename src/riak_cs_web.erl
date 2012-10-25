%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Convenience functions for setting up the riak_cs HTTP interface.

-module(riak_cs_web).

-export([dispatch_table/0]).

-type dispatch_rule() :: {[string() | atom()], atom(), [term()]}.

%% @doc Setup the Webmachine dispatch table
-spec dispatch_table() -> [dispatch_rule()].
dispatch_table() ->
    StatsProps = stats_props(),
    admin_resources(StatsProps) ++
        base_resources(StatsProps) ++
        one_three_resources(riak_cs_utils:cs_version(), StatsProps).

-spec stats_props() -> [term()].
stats_props() ->
    [{auth_bypass, get_auth_bypass()}].

-spec admin_resources([term()]) -> [dispatch_rule()].
admin_resources(Props) ->
    [
     {["riak-cs", "stats"], riak_cs_wm_stats, Props},
     {["riak-cs", "ping"], riak_cs_wm_ping, []},
     {["user"], riak_cs_wm_user, []},
     {["riak-cs", "users"], riak_cs_wm_users, Props},
     {["riak-cs", "user", '*'], riak_cs_wm_user, Props},
     {["usage", '*'], riak_cs_wm_usage, Props}
    ].

-spec base_resources([term()]) -> [dispatch_rule()].
base_resources(Props) ->
    [
     %% Bucket resources
     {["buckets"], riak_cs_wm_bucket_list, Props},
     {["buckets", bucket], riak_cs_wm_bucket, Props},
     {["buckets", bucket, "objects"], riak_cs_wm_object_list, Props},
     {["buckets", bucket, "acl"], riak_cs_wm_bucket_acl, Props},
     {["buckets", bucket, "location"], riak_cs_wm_bucket_location, Props},
     {["buckets", bucket, "versioning"], riak_cs_wm_bucket_versioning, Props},
     %% Object resources
     {["buckets", bucket, "objects", object], riak_cs_wm_object, Props},
     {["buckets", bucket, "objects", object, "acl"], riak_cs_wm_object_acl, Props}
    ].

-spec one_three_resources(undefined | pos_integer(), [term()]) -> [dispatch_rule()].
one_three_resources(undefined, _) ->
    [];
one_three_resources(Version, _) when Version < 010300 ->
    [];
one_three_resources(_Version, _Props) ->
    [
    ].

-spec get_auth_bypass() -> boolean().
get_auth_bypass() ->
    get_auth_bypass(application:get_env(riak_cs, auth_bypass)).

-spec get_auth_bypass(undefined | {ok, boolean()}) -> boolean().
get_auth_bypass(undefined) ->
    false;
get_auth_bypass({ok, AuthBypass}) ->
    AuthBypass.
