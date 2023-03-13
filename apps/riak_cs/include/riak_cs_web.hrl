%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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

-ifndef(RIAK_CS_WEB_HRL).
-define(RIAK_CS_WEB_HRL, included).

-include("manifest.hrl").
-include("moss.hrl").
-include("aws_api.hrl").

-type mochiweb_headers() :: gb_trees:tree().

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").
-define(RCS_RAW_URL_HEADER, "x-rcs-raw-url").
-define(OOS_API_VSN_HEADER, "x-oos-api-version").
-define(OOS_ACCOUNT_HEADER, "x-oos-account").

-define(JSON_TYPE, "application/json").
-define(XML_TYPE, "application/xml").
-define(WWWFORM_TYPE, "application/x-www-form-urlencoded").

-type api() :: aws | oos.

-record(rcs_s3_context, {start_time =  os:system_time(millisecond) :: non_neg_integer(),
                         auth_bypass :: atom(),
                         user :: undefined | moss_user(),
                         user_object :: undefined | riakc_obj:riakc_obj(),
                         role :: undefined | role(),
                         bucket :: undefined | binary(),
                         acl :: 'undefined' | acl(),
                         requested_perm :: undefined | acl_perm(),
                         riak_client :: undefined | pid(),
                         rc_pool :: atom(),    % pool name which riak_client belongs to
                         auto_rc_close = true :: boolean(),
                         submodule :: module(),
                         exports_fun :: undefined | function(),
                         auth_module :: module(),
                         response_module :: module(),
                         policy_module :: module(),
                         %% Key for API rate and latency stats.
                         %% If `stats_prefix' or `stats_key' is `no_stats', no stats
                         %% will be gathered by riak_cs_wm_common.
                         %% The prefix is defined by `stats_prefix()' callback of sub-module.
                         %% If sub-module provides only `stats_prefix' (almost the case),
                         %% stats key is [Prefix, HttpMethod]. Otherwise, sum-module
                         %% can set specific `stats_key' by any callback that returns
                         %% this context.
                         stats_prefix = no_stats :: atom(),
                         stats_key = prefix_and_method :: prefix_and_method |
                                                          no_stats |
                                                          riak_cs_stats:key(),
                         local_context :: term(),
                         api :: atom()
                        }).

-record(rcs_iam_context, {start_time = os:system_time(millisecond) :: non_neg_integer(),
                          auth_bypass :: atom(),
                          user :: undefined | moss_user(),
                          role :: undefined | role(),
                          riak_client :: undefined | pid(),
                          rc_pool :: atom(),    % pool name which riak_client belongs to
                          auto_rc_close = true :: boolean(),
                          auth_module :: module(),
                          response_module :: module(),
                          stats_prefix = no_stats :: atom(),
                          stats_key = prefix_and_method :: prefix_and_method |
                                                           no_stats |
                                                           riak_cs_stats:key(),
                          api :: atom()
                         }).


-record(key_context, {manifest :: undefined | 'notfound' | lfs_manifest(),
                      upload_id :: undefined | binary(),
                      part_number :: undefined | integer(),
                      part_uuid :: undefined | binary(),
                      get_fsm_pid :: undefined | pid(),
                      putctype :: undefined | string(),
                      bucket :: undefined | binary(),
                      bucket_object :: undefined | notfound | riakc_obj:riakc_obj(),
                      key :: undefined | binary(),
                      obj_vsn = ?LFS_DEFAULT_OBJECT_VERSION :: binary(),
                      owner :: undefined | string(),
                      size :: undefined | non_neg_integer(),
                      content_md5 :: undefined | binary(),
                      update_metadata = false :: boolean()}).

-record(access_v1, {
          method :: 'PUT' | 'GET' | 'POST' | 'DELETE' | 'HEAD',
          target :: atom(), % object | object_acl | ....
          id :: string(),
          bucket :: binary(),
          key = <<>> :: undefined | binary(),
          req %:: #wm_reqdata{} % request of webmachine
         }).
-type access() :: #access_v1{}.


%% === objects ===

-type next_marker() :: undefined | binary().

-type list_objects_req_type() :: objects | versions.

-record(list_objects_request,
        {
         req_type :: list_objects_req_type(),

         %% the name of the bucket
         name :: binary(),

         %% how many keys to return in the response
         max_keys :: non_neg_integer(),

         %% a 'starts-with' parameter
         prefix :: binary() | undefined,

         %% a binary to group keys by
         delimiter :: binary() | undefined,

         %% the key and version_id to start with
         marker :: binary() | undefined,
         version_id_marker :: binary() | undefined
        }).
-type list_object_request() :: #list_objects_request{}.
-define(LOREQ, #list_objects_request).

-record(list_objects_response,
        {
         %% Params just echoed back from the request --------------------------

         %% the name of the bucket
         name :: binary(),

         %% how many keys were requested to be
         %% returned in the response
         max_keys :: non_neg_integer(),

         %% a 'starts-with' parameter
         prefix :: binary() | undefined,

         %% a binary to group keys by
         delimiter :: binary() | undefined,

         %% the marker used in the _request_
         marker :: binary() | undefined,
         %% the (optional) marker to use for pagination
         %% in the _next_ request
         next_marker :: next_marker(),
         %% (marker and next_marker not used in ListObjectsV2)

         %% The actual response -----------------------------------------------
         is_truncated :: boolean(),

         contents :: [list_objects_key_content()],

         common_prefixes :: list_objects_common_prefixes()
        }).

-type list_objects_response() :: #list_objects_response{}.
-define(LORESP, #list_objects_response).

-record(list_objects_key_content,
        {
         key :: binary(),
         last_modified :: term(),
         etag :: binary(),
         size :: non_neg_integer(),
         owner :: list_objects_owner(),
         storage_class :: binary()
        }).
-type list_objects_key_content() :: #list_objects_key_content{}.
-define(LOKC, #list_objects_key_content).


-record(list_object_versions_response,
        {
         %% Params just echoed back from the request --------------------------

         %% the name of the bucket
         name :: binary(),

         %% how many keys were requested to be
         %% returned in the response
         max_keys :: non_neg_integer(),

         %% a 'starts-with' parameter
         prefix :: binary() | undefined,

         %% a binary to group keys by
         delimiter :: binary() | undefined,

         %% the marker used in the _request_
         key_marker :: binary() | undefined,
         version_id_marker :: binary() | undefined,

         %% the (optional) marker to use for pagination
         %% in the _next_ request
         next_key_marker :: next_marker(),
         next_version_id_marker :: next_marker(),

         %% The actual response -----------------------------------------------
         is_truncated :: boolean(),

         contents :: [list_object_versions_key_content()],

         common_prefixes :: list_objects_common_prefixes()
        }).

-type list_object_versions_response() :: #list_object_versions_response{}.
-define(LOVRESP, #list_object_versions_response).

-record(list_object_versions_key_content,
        {
         key :: binary(),
         last_modified :: term(),
         etag :: binary(),
         is_latest :: boolean(),
         version_id :: binary(),
         size :: non_neg_integer(),
         owner :: list_objects_owner(),
         storage_class :: binary()
        }).
-type list_object_versions_key_content() :: #list_object_versions_key_content{}.
-define(LOVKC, #list_object_versions_key_content).

-record(list_objects_owner, {
        id :: binary(),
        display_name :: binary()}).
-type list_objects_owner() :: #list_objects_owner{}.

-type list_objects_common_prefixes() :: list(binary()).

-define(LIST_OBJECTS_CACHE, list_objects_cache).
-define(ENABLE_CACHE, true).
-define(CACHE_TIMEOUT, timer:minutes(15)).
-define(MIN_KEYS_TO_CACHE, 2000).
-define(MAX_CACHE_BYTES, 104857600). % 100MB


%% === buckets ===

-record(list_buckets_response,
        {
         %% the user record
         user :: rcs_user(),

         %% the list of bucket records
         buckets :: [cs_bucket()]
        }).
-type list_buckets_response() :: #list_buckets_response{}.
-define(LBRESP, #list_buckets_response).


%% === roles ===

-record(create_role_response, { role :: role()
                              , request_id :: string()
                              }
       ).

-record(get_role_response, { role :: role()
                           , request_id :: string()
                           }
       ).

-record(delete_role_response, { request_id :: string()
                              }
       ).

-record(list_roles_request, { max_items = 1000 :: non_neg_integer()
                            , path_prefix :: binary() | undefined
                            , marker :: binary() | undefined
                            , request_id :: string()
                            }
       ).
-type list_roles_request() :: #list_roles_request{}.
-define(LRREQ, #list_roles_request).

-record(list_roles_response, { marker :: binary() | undefined
                             , is_truncated :: boolean()
                             , roles :: [role()]
                             , request_id :: string()
                             }
       ).

-type list_roles_response() :: #list_roles_response{}.
-define(LRRESP, #list_objects_response).

-endif.
