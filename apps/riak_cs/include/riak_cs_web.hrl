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
-include_lib("webmachine/include/webmachine.hrl").

-type mochiweb_headers() :: gb_trees:tree().

-define(DEFAULT_AUTH_MODULE, riak_cs_aws_auth).
-define(DEFAULT_POLICY_MODULE, riak_cs_aws_policy).
-define(DEFAULT_RESPONSE_MODULE, riak_cs_aws_response).

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").
-define(RCS_RAW_URL_HEADER, "x-rcs-raw-url").
-define(OOS_API_VSN_HEADER, "x-oos-api-version").
-define(OOS_ACCOUNT_HEADER, "x-oos-account").

-define(JSON_TYPE, "application/json").
-define(XML_TYPE, "application/xml").
-define(WWWFORM_TYPE, "application/x-www-form-urlencoded").

-type api() :: aws | oos.

-record(rcs_web_context, {start_time = os:system_time(millisecond) :: non_neg_integer(),
                          request_id = riak_cs_wm_utils:make_request_id() :: binary(),
                          auth_bypass :: atom(),
                          user :: undefined | admin | rcs_user(),
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
                          auth_module = ?DEFAULT_AUTH_MODULE :: module(),
                          policy_module = ?DEFAULT_POLICY_MODULE :: module(),
                          response_module = ?DEFAULT_RESPONSE_MODULE :: module(),
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

-type action_target() :: object | object_part | object_acl
                       | bucket | bucket_acl | bucket_policy
                       | bucket_request_payment | bucket_location | bucket_uploads | no_bucket
                       | iam_entity
                       | sts_entity.

-record(access_v1, { method :: 'PUT' | 'GET' | 'POST' | 'DELETE' | 'HEAD'
                   , target :: action_target()
                   , id :: string()
                   , bucket :: binary()
                   , key = <<>> :: undefined | binary()
                   , action :: aws_action()
                   , req :: #wm_reqdata{}
                   }
       ).
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


%% === IAM ===

-record(create_user_response, { user :: rcs_user()
                              , request_id :: binary()
                              }
       ).
-record(get_user_response, { user :: rcs_user()
                           , request_id :: binary()
                           }
       ).
-record(delete_user_response, { request_id :: binary()
                              }
       ).
-record(list_users_request, { max_items = 1000 :: non_neg_integer()
                            , path_prefix :: binary() | undefined
                            , marker :: binary() | undefined
                            , request_id :: binary()
                            }
       ).
-record(list_users_response, { marker :: binary() | undefined
                             , is_truncated :: boolean()
                             , users :: [rcs_user()]
                             , request_id :: binary()
                             }
       ).


-record(create_role_response, { role :: role()
                              , request_id :: binary()
                              }
       ).
-record(get_role_response, { role :: role()
                           , request_id :: binary()
                           }
       ).
-record(delete_role_response, { request_id :: binary()
                              }
       ).
-record(list_roles_request, { max_items = 1000 :: non_neg_integer()
                            , path_prefix :: binary() | undefined
                            , marker :: binary() | undefined
                            , request_id :: binary()
                            }
       ).
-record(list_roles_response, { marker :: binary() | undefined
                             , is_truncated :: boolean()
                             , roles :: [role()]
                             , request_id :: binary()
                             }
       ).


-record(create_policy_response, { policy :: policy()
                                , request_id :: binary()
                                }
       ).
-record(get_policy_response, { policy :: policy()
                             , request_id :: binary()
                             }
       ).
-record(delete_policy_response, { request_id :: binary()
                                }
       ).
-record(list_policies_request, { max_items = 1000 :: non_neg_integer()
                               , only_attached = false :: boolean()
                               , policy_usage_filter = 'All' :: 'All' | 'PermissionsPolicy' | 'PermissionsBoundary'
                               , scope = all :: 'All' | 'AWS' | 'Local'
                               , path_prefix :: binary() | undefined
                               , marker :: binary() | undefined
                               , request_id :: binary()
                               }
       ).
-record(list_policies_response, { marker :: binary() | undefined
                                , is_truncated :: boolean()
                                , policies :: [policy()]
                                , request_id :: binary()
                                }
       ).


-record(create_saml_provider_response, { saml_provider_arn :: arn()
                                       , tags :: [tag()]
                                       , request_id :: binary()
                                       }
       ).
-record(get_saml_provider_response, { create_date :: non_neg_integer()
                                    , valid_until :: non_neg_integer()
                                    , tags :: [tag()]
                                    , request_id :: binary()
                                    }
       ).
-record(delete_saml_provider_response, { request_id :: binary()
                                       }
       ).
-record(list_saml_providers_request, { request_id :: binary()
                                     }
       ).
-record(saml_provider_list_entry, { create_date :: non_neg_integer()
                                  , valid_until :: non_neg_integer()
                                  , arn :: arn()
                                  }
       ).
-record(list_saml_providers_response, { saml_provider_list :: [#saml_provider_list_entry{}]
                                      , request_id :: binary()
                                      }
       ).

-record(assume_role_with_saml_response, { assumed_role_user :: assumed_role_user()
                                        , audience :: binary()
                                        , credentials :: credentials()
                                        , issuer :: binary()
                                        , name_qualifier :: binary()
                                        , packed_policy_size :: non_neg_integer()
                                        , source_identity :: binary()
                                        , subject :: binary()
                                        , subject_type :: binary()
                                        , request_id :: binary()
                                        }
       ).

-record(attach_role_policy_response, { request_id :: binary()
                                     }
       ).
-record(detach_role_policy_response, { request_id :: binary()
                                     }
       ).

-record(attach_user_policy_response, { request_id :: binary()
                                     }
       ).
-record(detach_user_policy_response, { request_id :: binary()
                                     }
       ).

-record(list_attached_user_policies_response, { policies :: [{flat_arn(), binary()}]
                                              , marker :: binary() | undefined
                                              , is_truncated :: boolean()
                                              , request_id :: binary()
                                              }
       ).

-endif.
