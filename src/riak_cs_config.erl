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

-module(riak_cs_config).

-export([
         admin_creds/0,
         anonymous_user_creation/0,
         api/0,
         auth_bypass/0,
         admin_auth_enabled/0,
         auth_module/0,
         cluster_id/1,
         cs_version/0,
         enforce_multipart_part_size/0,
         get_env/3,
         key_list_multiplier/0,
         set_key_list_multiplier/1,
         md5_chunk_size/0,
         policy_module/0,
         proxy_get_active/0,
         response_module/0,
         response_module/1,
         riak_cs_stat/0,
         set_md5_chunk_size/1,
         use_t2b_compression/0
        ]).

%% OpenStack config
-export([os_auth_url/0,
         os_operator_roles/0,
         os_admin_token/0,
         os_s3_tokens_url/0,
         os_tokens_url/0,
         os_users_url/0]).

-include("riak_cs.hrl").
-include("oos_api.hrl").
-include("list_objects.hrl").

%% ===================================================================
%% General config options
%% ===================================================================

%% @doc Return the value of the `anonymous_user_creation' application
%% environment variable.
-spec anonymous_user_creation() -> boolean().
anonymous_user_creation() ->
    get_env(riak_cs, anonymous_user_creation, false).

%% @doc Return the credentials of the admin user
-spec admin_creds() -> {ok, {string(), string()}} | {error, term()}.
admin_creds() ->
    admin_creds_response(
      get_env(riak_cs, admin_key, undefined),
      get_env(riak_cs, admin_secret, undefined)).

-spec admin_creds_response(term(), term()) -> {ok, {term(), term()}} |
                                              {error, atom()}.
admin_creds_response(undefined, _) ->
    _ = lager:warning("The admin user's key id"
                      "has not been specified."),
    {error, admin_key_undefined};
admin_creds_response([], _) ->
    _ = lager:warning("The admin user's key id"
                      "has not been specified."),
    {error, admin_key_undefined};
admin_creds_response(_, undefined) ->
    _ = lager:warning("The admin user's secret"
                      "has not been specified."),
    {error, admin_secret_undefined};
admin_creds_response(_, []) ->
    _ = lager:warning("The admin user's secret"
                      "has not been specified."),
    {error, admin_secret_undefined};
admin_creds_response(Key, Secret) ->
    {ok, {Key, Secret}}.

%% @doc Get the active version of Riak CS to use in checks to
%% determine if new features should be enabled.
-spec cs_version() -> pos_integer() | undefined.
cs_version() ->
    get_env(riak_cs, cs_version, undefined).

-spec api() -> s3 | oos.
api() ->
    api(application:get_env(riak_cs, rewrite_module)).

-spec api({ok, atom()} | undefined) -> s3 | oos | undefined.
api({ok, ?S3_API_MOD}) ->
    s3;
api({ok, ?OOS_API_MOD}) ->
    oos;
api(_) ->
    undefined.

-spec auth_bypass() -> boolean().
auth_bypass() ->
    get_env(riak_cs, auth_bypass, false).

-spec admin_auth_enabled() -> boolean().
admin_auth_enabled() ->
    get_env(riak_cs, admin_auth_enabled, true).

-spec auth_module() -> atom().
auth_module() ->
    get_env(riak_cs, auth_module, ?DEFAULT_AUTH_MODULE).

-spec enforce_multipart_part_size() -> boolean().
enforce_multipart_part_size() ->
    get_env(riak_cs, enforce_multipart_part_size, true).

-spec key_list_multiplier() -> float().
key_list_multiplier() ->
    get_env(riak_cs, key_list_multiplier, ?KEY_LIST_MULTIPLIER).

-spec set_key_list_multiplier(float()) -> 'ok'.
set_key_list_multiplier(Multiplier) ->
    application:set_env(riak_cs, key_list_multiplier, Multiplier).

-spec policy_module() -> atom().
policy_module() ->
    get_env(riak_cs, policy_module, ?DEFAULT_POLICY_MODULE).

-spec response_module() -> atom().
response_module() ->
    response_module(api()).

-spec response_module(atom()) -> atom().
response_module(oos) ->
    ?OOS_RESPONSE_MOD;
response_module(_) ->
    ?S3_RESPONSE_MOD.

-spec use_t2b_compression() -> boolean().
use_t2b_compression() ->
    get_env(riak_cs, compress_terms, ?COMPRESS_TERMS).

%% doc Return the current cluster ID. Used for repl
%% After obtaining the clusterid the first time,
%% store the value in app:set_env
-spec cluster_id(pid()) -> binary().
cluster_id(Pid) ->
    case application:get_env(riak_cs, cluster_id) of
        {ok, ClusterID} ->
            ClusterID;
        undefined ->
            Timeout = case application:get_env(riak_cs, cluster_id_timeout) of
                          {ok, Value} ->
                              Value;
                          undefined   ->
                              ?DEFAULT_CLUSTER_ID_TIMEOUT
                      end,
            maybe_get_cluster_id(proxy_get_active(), Pid, Timeout)
    end.

%% @doc If `proxy_get' is enabled then attempt to determine the cluster id
-spec maybe_get_cluster_id(boolean(), pid(), integer()) -> undefined | binary().
maybe_get_cluster_id(true, Pid, Timeout) ->
    try
        case riak_repl_pb_api:get_clusterid(Pid, Timeout) of
            {ok, ClusterID} ->
                application:set_env(riak_cs, cluster_id, ClusterID),
                ClusterID;
            _ ->
                _ = lager:debug("Unable to obtain cluster ID"),
                undefined
        end
    catch _:_ ->
            %% Disable `proxy_get' so we do not repeatedly have to
            %% handle this same exception. This would happen if an OSS
            %% install has `proxy_get' enabled.
            application:set_env(riak_cs, proxy_get, disabled),
            undefined
    end;
maybe_get_cluster_id(false, _, _) ->
    undefined.

%% @doc Return the configured md5 chunk size
-spec md5_chunk_size() -> non_neg_integer().
md5_chunk_size() ->
    get_env(riak_cs, md5_chunk_size, ?DEFAULT_MD5_CHUNK_SIZE).

-spec riak_cs_stat() -> boolean().
riak_cs_stat() ->
    get_env(riak_cs, riak_cs_stat, true).

%% @doc Helper fun to set the md5 chunk size
-spec set_md5_chunk_size(non_neg_integer()) -> ok | {error, invalid_value}.
set_md5_chunk_size(Size) when is_integer(Size) andalso Size > 0 ->
    application:set_env(riak_cs, md5_chunk_size, Size);
set_md5_chunk_size(_) ->
    {error, invalid_value}.

%% doc Check app.config to see if repl proxy_get is enabled
%% Defaults to false.
proxy_get_active() ->
    case application:get_env(riak_cs, proxy_get) of
        {ok, enabled} ->
            true;
        {ok, disabled} ->
            false;
        {ok, _} ->
            _ = lager:warning("proxy_get value in app.config is invalid"),
            false;
        undefined -> false
    end.

%% ===================================================================
%% S3 config options
%% ===================================================================

%% ===================================================================
%% OpenStack config options
%% ===================================================================

-spec os_auth_url() -> term().
os_auth_url() ->
    get_env(riak_cs, os_auth_url, ?DEFAULT_OS_AUTH_URL).

-spec os_operator_roles() -> [term()].
os_operator_roles() ->
    ordsets:from_list(get_env(riak_cs,
                              os_operator_roles,
                              ?DEFAULT_OS_OPERATOR_ROLES)).

-spec os_admin_token() -> term().
os_admin_token() ->
    get_env(riak_cs, os_admin_token, ?DEFAULT_OS_ADMIN_TOKEN).

-spec os_s3_tokens_url() -> term().
os_s3_tokens_url() ->
    os_auth_url() ++
        get_env(riak_cs,
                os_s3_tokens_resource,
                ?DEFAULT_S3_TOKENS_RESOURCE).

-spec os_tokens_url() -> term().
os_tokens_url() ->
    os_auth_url() ++
        get_env(riak_cs,
                os_tokens_resource,
                ?DEFAULT_TOKENS_RESOURCE).

-spec os_users_url() -> term().
os_users_url() ->
    os_auth_url() ++
        get_env(riak_cs,
                os_users_resource,
                ?DEFAULT_OS_USERS_RESOURCE).

%% ===================================================================
%% Wrapper for `application:get_env'
%% ===================================================================

%% @doc Get an application environment variable or return a default term.
-spec get_env(atom(), atom(), term()) -> term().
get_env(App, Key, Default) ->
    case application:get_env(App, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.
