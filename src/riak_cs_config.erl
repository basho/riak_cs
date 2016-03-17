%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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
         warnings/0,
         admin_creds/0,
         anonymous_user_creation/0,
         api/0,
         auth_bypass/0,
         admin_auth_enabled/0,
         auth_module/0,
         auth_v4_enabled/0,
         cluster_id/1,
         cs_version/0,
         disable_local_bucket_check/0,
         enforce_multipart_part_size/0,
         gc_batch_size/0,
         get_env/3,
         key_list_multiplier/0,
         set_key_list_multiplier/1,
         md5_chunk_size/0,
         gc_paginated_indexes/0,
         policy_module/0,
         proxy_get_active/0,
         response_module/0,
         response_module/1,
         riak_cs_stat/0,
         set_md5_chunk_size/1,
         use_t2b_compression/0,
         trust_x_forwarded_for/0,
         gc_key_suffix_max/0,
         set_gc_key_suffix_max/1,
         user_buckets_prune_time/0,
         set_user_buckets_prune_time/1,
         riak_host_port/0,
         connect_timeout/0,
         queue_if_disconnected/0,
         auto_reconnect/0,
         is_multibag_enabled/0,
         set_multibag_appenv/0,
         max_buckets_per_user/0,
         max_key_length/0,
         read_before_last_manifest_write/0,
         region/0,
         stanchion/0,
         use_2i_for_storage_calc/0,
         detailed_storage_calc/0,
         quota_modules/0,
         active_delete_threshold/0,
         fast_user_get/0,
         root_host/0
        ]).

%% Timeouts hitting Riak
-export([ping_timeout/0,
         get_user_timeout/0,
         get_bucket_timeout/0,
         get_manifest_timeout/0,
         get_block_timeout/0, %% for n_val=3
         local_get_block_timeout/0, %% for n_val=1, default 5
         proxy_get_block_timeout/0, %% for remote
         get_access_timeout/0,
         get_gckey_timeout/0,
         put_user_timeout/0,
         put_manifest_timeout/0,
         put_block_timeout/0,
         put_access_timeout/0,
         put_gckey_timeout/0,
         put_user_usage_timeout/0,
         delete_manifest_timeout/0,
         delete_block_timeout/0,
         delete_gckey_timeout/0,
         list_keys_list_objects_timeout/0,
         list_keys_list_users_timeout/0,
         list_keys_list_buckets_timeout/0,
         storage_calc_timeout/0,
         list_objects_timeout/0, %% using mapred (v0)
         fold_objects_timeout/0, %% for cs_bucket_fold
         get_index_range_gckeys_timeout/0,
         get_index_range_gckeys_call_timeout/0,
         get_index_list_multipart_uploads_timeout/0,
         cluster_id_timeout/0
        ]).

%% OpenStack config
-export([os_auth_url/0,
         os_operator_roles/0,
         os_admin_token/0,
         os_s3_tokens_url/0,
         os_tokens_url/0,
         os_users_url/0]).

-include("riak_cs_gc.hrl").
-include("oos_api.hrl").
-include("s3_api.hrl").
-include("list_objects.hrl").

-define(MAYBE_WARN(Bool, Msg),
        case (Bool) of
            true -> _ = lager:warning((Msg));
            _ -> ok
        end).

-spec warnings() -> ok.
warnings() ->
    ?MAYBE_WARN(not riak_cs_list_objects_utils:fold_objects_for_list_keys(),
                "`fold_objects_for_list_keys` is set as false."
                " This will be removed at next major version."),
    ?MAYBE_WARN(anonymous_user_creation(),
                "`anonymous_user_creation` is set as true. Set this as false"
                " when this CS nodes is populated as public service."),
    ?MAYBE_WARN(not gc_paginated_indexes(),
                "`gc_paginated_indexes` is set as false. "
                " This will be removed at next major version."),
    ok.

%% ===================================================================
%% General config options
%% ===================================================================

%% @doc Return the value of the `anonymous_user_creation' application
%% environment variable.
-spec anonymous_user_creation() -> boolean().
anonymous_user_creation() ->
    get_env(riak_cs, anonymous_user_creation, false).

%% @doc Return the credentials of the admin user
-spec admin_creds() -> {ok, {string()|undefined, string()|undefined}}.
admin_creds() ->
    {ok, {get_env(riak_cs, admin_key, undefined),
          get_env(riak_cs, admin_secret, undefined)}}.

%% @doc Get the active version of Riak CS to use in checks to
%% determine if new features should be enabled.
-spec cs_version() -> pos_integer() | undefined.
cs_version() ->
    get_env(riak_cs, cs_version, undefined).

-spec api() -> s3 | oos.
api() ->
    api(get_env(riak_cs, rewrite_module, ?S3_API_MOD)).

-spec api(atom() | undefined) -> s3 | oos | undefined.
api(?S3_API_MOD) ->
    s3;
api(?S3_LEGACY_API_MOD) ->
    s3;
api(?OOS_API_MOD) ->
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

-spec auth_v4_enabled() -> boolean().
auth_v4_enabled() ->
    get_env(riak_cs, auth_v4_enabled, false).

-spec disable_local_bucket_check() -> boolean().
disable_local_bucket_check() ->
    get_env(riak_cs, disable_local_bucket_check, false).

-spec enforce_multipart_part_size() -> boolean().
enforce_multipart_part_size() ->
    get_env(riak_cs, enforce_multipart_part_size, true).

-spec gc_batch_size() -> non_neg_integer().
gc_batch_size() ->
    get_env(riak_cs, gc_batch_size, ?DEFAULT_GC_BATCH_SIZE).

-spec key_list_multiplier() -> float().
key_list_multiplier() ->
    get_env(riak_cs, key_list_multiplier, ?KEY_LIST_MULTIPLIER).

-spec set_key_list_multiplier(float()) -> 'ok'.
set_key_list_multiplier(Multiplier) ->
    application:set_env(riak_cs, key_list_multiplier, Multiplier).

-spec policy_module() -> atom().
policy_module() ->
    get_env(riak_cs, policy_module, ?DEFAULT_POLICY_MODULE).

%% @doc paginated 2i is supported after Riak 1.4
%% When using Riak CS `>= 1.5' with Riak `=< 1.3' (it rarely happens)
%% this should be set as false at app.config.
-spec gc_paginated_indexes() -> atom().
gc_paginated_indexes() ->
    get_env(riak_cs, gc_paginated_indexes, true).

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
-spec cluster_id(fun()) -> binary().
cluster_id(GetClusterIdFun) ->
    case application:get_env(riak_cs, cluster_id) of
        {ok, ClusterID} ->
            ClusterID;
        undefined ->
            ClusterId = GetClusterIdFun(undefined),
            application:set_env(riak_cs, cluster_id, ClusterId),
            ClusterId
    end.

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
-spec proxy_get_active() -> boolean().
proxy_get_active() ->
    case application:get_env(riak_cs, proxy_get, false) of
        enabled ->   true;
        disabled -> false;
        Flag when is_boolean(Flag) -> Flag;
        Other ->
            _ = lager:warning("proxy_get value in advanced.config is invalid: ~p",
                              [Other]),
            false
    end.

-spec trust_x_forwarded_for() -> true | false.
trust_x_forwarded_for() ->
    case application:get_env(riak_cs, trust_x_forwarded_for) of
        {ok, true} -> true;
        {ok, false} -> false;
        {ok, _} ->
            _ = lager:warning("trust_x_forwarded_for value in app.config is invalid"),
            false;
        undefined -> false %% secure by default!
    end.

%% @doc Return the max value for GC bucket key suffix.
%% A suffix value is a random integer between 1 and the returned value.
-spec gc_key_suffix_max() -> pos_integer().
gc_key_suffix_max() ->
    get_env(riak_cs, gc_key_suffix_max, ?DEFAULT_GC_KEY_SUFFIX_MAX).

-spec set_gc_key_suffix_max(integer()) -> ok | {error, invalid_value}.
set_gc_key_suffix_max(MaxValue) when is_integer(MaxValue) andalso MaxValue > 0 ->
    application:set_env(riak_cs, gc_key_suffix_max, MaxValue);
set_gc_key_suffix_max(_MaxValue) ->
    {error, invalid_value}.

-spec user_buckets_prune_time() -> pos_integer().
user_buckets_prune_time() ->
    get_env(riak_cs, user_buckets_prune_time, ?USER_BUCKETS_PRUNE_TIME).

-spec set_user_buckets_prune_time(pos_integer()) -> ok | {error, invalid_value}.
set_user_buckets_prune_time(PruneTime) when is_integer(PruneTime) andalso PruneTime > 0 ->
    application:set_env(riak_cs, user_buckets_prune_time, PruneTime);
set_user_buckets_prune_time(_PruneTime) ->
    {error, invalid_value}.

%% @doc copied from `riak_cs_access_archiver'
-spec riak_host_port() -> {inet:hostname(), inet:port_number()}.
riak_host_port() ->
    case application:get_env(riak_cs, riak_host) of
        {ok, RiakHost} -> RiakHost;
        undefined -> {"127.0.0.1", 8087}
    end.

-spec connect_timeout() -> pos_integer().
connect_timeout() ->
    case application:get_env(riak_cs, riakc_connect_timeout) of
        {ok, ConfigValue} ->
            ConfigValue;
        undefined ->
            10000
    end.

%% @doc choose client connection option: true by default
-spec auto_reconnect() -> [{auto_reconnect, boolean()}].
auto_reconnect() ->
    case application:get_env(riak_cs, riakc_auto_reconnect) of
        {ok, true} ->  [{auto_reconnect, true}];
        {ok, false} -> [{auto_reconnect, false}];
        _ -> [{auto_reconnect, true}]
    end.

%% @doc choose client connection option: undefined by default, let
%% riak-erlang-client choose the default behaviour
-spec queue_if_disconnected() -> [{queue_if_disconnected, boolean()}].
queue_if_disconnected() ->
    case application:get_env(riak_cs, riakc_queue_if_disconnected) of
        {ok, true} ->  [{queue_if_disconnected, true}];
        {ok, false} -> [{queue_if_disconnected, false}];
        _ -> []
    end.

-spec is_multibag_enabled() -> boolean().
is_multibag_enabled() ->
    application:get_env(riak_cs_multibag, bags) =/= undefined.

%% Set riak_cs_multibag app env vars for backward compatibility
%% to hide any bags from user-facing (TM) parts.
-spec set_multibag_appenv() -> ok.
set_multibag_appenv() ->
    case application:get_env(riak_cs, supercluster_members) of
        undefined -> ok;
        {ok, Bags} -> application:set_env(riak_cs_multibag, bags, Bags)
    end,
    case application:get_env(riak_cs, supercluster_weight_refresh_interval) of
        undefined -> ok;
        {ok, Interval} -> application:set_env(
                            riak_cs_multibag, weight_refresh_interval, Interval)
    end.

-spec max_buckets_per_user() -> non_neg_integer() | unlimited.
max_buckets_per_user() ->
    get_env(riak_cs, max_buckets_per_user, ?DEFAULT_MAX_BUCKETS_PER_USER).

-spec max_key_length() -> pos_integer() | unlimited.
max_key_length() ->
    get_env(riak_cs, max_key_length, ?MAX_S3_KEY_LENGTH).

%% @doc Return `stanchion' configuration data.
-spec stanchion() -> {string(), pos_integer(), boolean()}.
stanchion() ->
    {Host, Port} = case application:get_env(riak_cs, stanchion_host) of
                       {ok, HostPort} -> HostPort;
                       undefined ->
                           _ = lager:warning("No stanchion access defined. Using default."),
                           {?DEFAULT_STANCHION_IP, ?DEFAULT_STANCHION_PORT}
                   end,
    SSL = case application:get_env(riak_cs, stanchion_ssl) of
              {ok, SSL0} -> SSL0;
              undefined ->
                  _ = lager:warning("No ssl flag for stanchion access defined. Using default."),
                  ?DEFAULT_STANCHION_SSL
          end,
    {Host, Port, SSL}.

%% @doc This options is useful for use case involving high churn and
%% concurrency on a fixed set of keys and when not using a Riak
%% version >= 2.0.0 with DVVs enabled. It helps to avoid sibling
%% explosion in such use cases that can debilitate a system.
-spec read_before_last_manifest_write() -> boolean().
read_before_last_manifest_write() ->
    get_env(riak_cs, read_before_last_manifest_write, true).

%% @doc -> current region name
%% As Riak CS currently assuming 1 cluster per region,
%% so region =:= location constraint =:= service endpoint.
-spec region() -> string().
region() ->
    get_env(riak_cs, region, ?DEFAULT_REGION).

%% @doc This switch changes mapreduce input from listkeys to
%% fold_objects after riak_kv's mapreduce input optimization
%% merged and enabled. With fold_objects it will have 1/2 IO
%% when scanning the whole bucket.
-spec use_2i_for_storage_calc() -> boolean().
use_2i_for_storage_calc() ->
    get_env(riak_cs, use_2i_for_storage_calc, false).

%% @doc Cauculate detailed summary information of bucket
%% usage or not.
-spec detailed_storage_calc() -> boolean().
detailed_storage_calc() ->
    get_env(riak_cs, detailed_storage_calc, false).

-spec quota_modules() -> [module()].
quota_modules() ->
    get_env(riak_cs, quota_modules, []).

%% @doc smaller than block size recommended, to avoid multiple DELETE
%% calls to riak per single manifest deletion.
-spec active_delete_threshold() -> non_neg_integer().
active_delete_threshold() ->
    get_env(riak_cs, active_delete_threshold, 0).

-spec fast_user_get() -> boolean().
fast_user_get() ->
    get_env(riak_cs, fast_user_get, false).

-spec root_host() -> string().
root_host() ->
    get_env(riak_cs, cs_root_host, ?ROOT_HOST).

%% ===================================================================
%% ALL Timeouts hitting Riak
%% ===================================================================

-define(TIMEOUT_CONFIG_FUNC(ConfigName),
        ConfigName() ->
               get_env(riak_cs, ConfigName,
                       get_env(riak_cs, riakc_timeouts, ?DEFAULT_RIAK_TIMEOUT))).

%% @doc Return the configured ping timeout. Default is 5 seconds.
-spec ping_timeout() -> pos_integer().
ping_timeout() ->
    get_env(riak_cs, ping_timeout, ?DEFAULT_PING_TIMEOUT).

%% timeouts in milliseconds
?TIMEOUT_CONFIG_FUNC(get_user_timeout).
?TIMEOUT_CONFIG_FUNC(get_bucket_timeout).
?TIMEOUT_CONFIG_FUNC(get_manifest_timeout).
?TIMEOUT_CONFIG_FUNC(get_block_timeout).

local_get_block_timeout() ->
    get_env(riak_cs, local_get_block_timeout, timer:seconds(5)).

?TIMEOUT_CONFIG_FUNC(proxy_get_block_timeout).
?TIMEOUT_CONFIG_FUNC(get_access_timeout).
?TIMEOUT_CONFIG_FUNC(get_gckey_timeout).
?TIMEOUT_CONFIG_FUNC(put_user_timeout).
?TIMEOUT_CONFIG_FUNC(put_manifest_timeout).
?TIMEOUT_CONFIG_FUNC(put_block_timeout).
?TIMEOUT_CONFIG_FUNC(put_access_timeout).
?TIMEOUT_CONFIG_FUNC(put_gckey_timeout).
?TIMEOUT_CONFIG_FUNC(put_user_usage_timeout).
?TIMEOUT_CONFIG_FUNC(delete_manifest_timeout).
?TIMEOUT_CONFIG_FUNC(delete_block_timeout).
?TIMEOUT_CONFIG_FUNC(delete_gckey_timeout).
?TIMEOUT_CONFIG_FUNC(list_keys_list_objects_timeout).
?TIMEOUT_CONFIG_FUNC(list_keys_list_users_timeout).
?TIMEOUT_CONFIG_FUNC(list_keys_list_buckets_timeout).
?TIMEOUT_CONFIG_FUNC(storage_calc_timeout).
?TIMEOUT_CONFIG_FUNC(list_objects_timeout).
?TIMEOUT_CONFIG_FUNC(fold_objects_timeout).
?TIMEOUT_CONFIG_FUNC(get_index_range_gckeys_timeout).
?TIMEOUT_CONFIG_FUNC(get_index_range_gckeys_call_timeout).
?TIMEOUT_CONFIG_FUNC(get_index_list_multipart_uploads_timeout).
?TIMEOUT_CONFIG_FUNC(cluster_id_timeout).

-undef(TIMEOUT_CONFIG_FUNC).

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
