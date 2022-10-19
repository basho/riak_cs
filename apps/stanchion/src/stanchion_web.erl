%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021 TI Tokyo    All Rights Reserved.
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

%% @doc Convenience functions for setting up the stanchion HTTP interface.

-module(stanchion_web).

-export([dispatch_table/0]).

dispatch_table() ->
    AuthBypass =
        case application:get_env(stanchion, auth_bypass) of
            {ok, AuthBypass0} -> AuthBypass0;
            undefined ->         false
        end,
    [
     {["buckets"], stanchion_wm_buckets, [{auth_bypass, AuthBypass}]},
     {["buckets", bucket, "acl"], stanchion_wm_acl, [{auth_bypass, AuthBypass}]},
     {["buckets", bucket, "policy"], stanchion_wm_policy, [{auth_bypass, AuthBypass}]},
     {["buckets", bucket, "versioning"], stanchion_wm_versioning, [{auth_bypass, AuthBypass}]},
     {["buckets", bucket], stanchion_wm_bucket, [{auth_bypass, AuthBypass}]},
     {["users", key_id], stanchion_wm_user, [{auth_bypass, AuthBypass}]},
     {["users"], stanchion_wm_users, [{auth_bypass, AuthBypass}]},
     {["stats"], stanchion_wm_stats, [{auth_bypass, AuthBypass}]}
    ].
