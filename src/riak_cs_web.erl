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

%% @doc Convenience functions for setting up the riak_cs HTTP interface.

-module(riak_cs_web).

-export([admin_api_dispatch_table/0,
         object_api_dispatch_table/0]).

-include("riak_cs.hrl").

-type dispatch_rule() :: {[string() | atom()], atom(), [term()]}.

%% @doc Setup the Webmachine dispatch table for the admin API
-spec admin_api_dispatch_table() -> [dispatch_rule()].
admin_api_dispatch_table() ->
    admin_resources(stats_props()).

%% @doc Setup the Webmachine dispatch table for the object storage API
-spec object_api_dispatch_table() -> [dispatch_rule()].
object_api_dispatch_table() ->
    base_resources() ++
        one_three_resources(riak_cs_config:cs_version()).

-spec props(atom()) -> [term()].
props(Mod) ->
    [{auth_bypass, riak_cs_config:auth_bypass()},
     {auth_module, riak_cs_config:auth_module()},
     {policy_module, riak_cs_config:policy_module()},
     {submodule, Mod}].

-spec stats_props() -> [term()].
stats_props() ->
    [{admin_auth_enabled, riak_cs_config:admin_auth_enabled()},
     {auth_bypass, riak_cs_config:auth_bypass()}].

-spec admin_resources([term()]) -> [dispatch_rule()].
admin_resources(Props) ->
    [
     {["riak-cs", "stats"], riak_cs_wm_stats, Props},
     {["riak-cs", "disk_usage"], riak_cs_wm_disk_usage, []},
     {["riak-cs", "users_disk_usage"], riak_cs_wm_users_disk_usage, []},
     {["riak-cs", "ping"], riak_cs_wm_ping, []},
     {["riak-cs", "users"], riak_cs_wm_users, Props},
     {["riak-cs", "user", '*'], riak_cs_wm_user, Props},
     {["riak-cs", "usage", '*'], riak_cs_wm_usage, Props}
    ].

-spec base_resources() -> [dispatch_rule()].
base_resources() ->
    [
     %% Bucket resources
     {["buckets"], riak_cs_wm_common, props(riak_cs_wm_buckets)},
     {["buckets", bucket], riak_cs_wm_common, props(riak_cs_wm_bucket)},
     {["buckets", bucket, "objects"], riak_cs_wm_common, props(riak_cs_wm_objects)},
     {["buckets", bucket, "acl"], riak_cs_wm_common, props(riak_cs_wm_bucket_acl)},
     {["buckets", bucket, "location"], riak_cs_wm_common, props(riak_cs_wm_bucket_location)},
     {["buckets", bucket, "versioning"], riak_cs_wm_common, props(riak_cs_wm_bucket_versioning)},
     %% Object resources
     {["buckets", bucket, "objects", object], riak_cs_wm_common, props(riak_cs_wm_object)},
     {["buckets", bucket, "objects", object, "acl"], riak_cs_wm_common, props(riak_cs_wm_object_acl)}
    ].

-spec one_three_resources(undefined | pos_integer()) -> [dispatch_rule()].
one_three_resources(undefined) ->
    [];
one_three_resources(Version) when Version < 010300 ->
    [];
one_three_resources(_Version) ->
    [
     %% Bucket resources
     {["buckets", bucket, "uploads"], riak_cs_wm_common, props(riak_cs_wm_bucket_uploads)},
     {["buckets", bucket, "policy"], riak_cs_wm_common, props(riak_cs_wm_bucket_policy)},
     %% Object resources
     {["buckets", bucket, "objects", object, "uploads", uploadId], riak_cs_wm_common, props(riak_cs_wm_object_upload_part)},
     {["buckets", bucket, "objects", object, "uploads"], riak_cs_wm_common, props(riak_cs_wm_object_upload)}
    ].
