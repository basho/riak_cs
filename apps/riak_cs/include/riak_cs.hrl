%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

-ifndef(RIAK_CS_HRL).
-define(RIAK_CS_HRL, included).

-include("manifest.hrl").
-include("moss.hrl").

-define(RCS_VERSION, 030100).

-type riak_client() :: pid().

-record(context, {start_time :: undefined | erlang:timestamp(),
                  auth_bypass :: atom(),
                  user :: undefined | moss_user(),
                  user_object :: undefined | riakc_obj:riakc_obj(),
                  bucket :: undefined | binary(),
                  acl :: 'undefined' | acl(),
                  requested_perm :: undefined | acl_perm(),
                  riak_client :: undefined | riak_client(),
                  rc_pool :: atom(),    % pool name which riak_client belongs to
                  auto_rc_close = true :: boolean(),
                  submodule :: atom(),
                  exports_fun :: undefined | function(),
                  auth_module :: atom(),
                  response_module :: atom(),
                  policy_module :: atom(),
                  %% Key for API rate and latency stats.
                  %% If `stats_prefix' or `stats_key' is `no_stats', no stats
                  %% will be gathered by riak_cs_wm_common.
                  %% The prefix is defined by `stats_prefix()' callback of sub-module.
                  %% If sub-module provides only `stats_prefix' (almost the case),
                  %% stats key is [Prefix, HttpMethod]. Otherwise, sum-module
                  %% can set specific `stats_key' by any callback that returns
                  %% this context.
                  stats_prefix = no_stats :: atom(),
                  stats_key=prefix_and_method :: prefix_and_method |
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


-define(DEFAULT_MAX_BUCKETS_PER_USER, 100).
-define(DEFAULT_MAX_CONTENT_LENGTH, 5368709120). %% 5 GB
-define(DEFAULT_LFS_BLOCK_SIZE, 1048576).%% 1 MB
-define(XML_PROLOG, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").
-define(S3_XMLNS, "http://s3.amazonaws.com/doc/2006-03-01/").
-define(DEFAULT_STANCHION_IP, "127.0.0.1").
-define(DEFAULT_STANCHION_PORT, 8085).
-define(DEFAULT_STANCHION_SSL, true).
-define(EMAIL_INDEX, <<"email_bin">>).
-define(ID_INDEX, <<"c_id_bin">>).
-define(KEY_INDEX, <<"$key">>).
-define(AUTH_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers").
-define(ALL_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AllUsers").
-define(LOG_DELIVERY_GROUP, "http://acs.amazonaws.com/groups/s3/LogDelivery").
-define(DEFAULT_FETCH_CONCURRENCY, 1).
-define(DEFAULT_PUT_CONCURRENCY, 1).
-define(DEFAULT_DELETE_CONCURRENCY, 1).
%% A number to multiplied with the block size
%% to determine the PUT buffer size.
%% ex. 2 would mean BlockSize * 2
-define(DEFAULT_PUT_BUFFER_FACTOR, 1).
%% Similar to above, but for fetching
%% This is also max ram per fetch request
-define(DEFAULT_FETCH_BUFFER_FACTOR, 32).
-define(N_VAL_1_GET_REQUESTS, true).
-define(DEFAULT_PING_TIMEOUT, 5000).
-define(JSON_TYPE, "application/json").
-define(XML_TYPE, "application/xml").
-define(S3_API_MOD, riak_cs_s3_rewrite).
-define(S3_LEGACY_API_MOD, riak_cs_s3_rewrite_legacy).
-define(OOS_API_MOD, riak_cs_oos_rewrite).
-define(S3_RESPONSE_MOD, riak_cs_s3_response).
-define(OOS_RESPONSE_MOD, riak_cs_oos_response).

-define(COMPRESS_TERMS, false).

%% Major categories of Erlang-triggered DTrace probes
%%
%% The main R15B01 USDT probe that can be triggered by Erlang code is defined
%% like this:
%%
%% /**
%%  * Multi-purpose probe: up to 4 NUL-terminated strings and 4
%%  * 64-bit integer arguments.
%%  *
%%  * @param proc, the PID (string form) of the sending process
%%  * @param user_tag, the user tag of the sender
%%  * @param i1, integer
%%  * @param i2, integer
%%  * @param i3, integer
%%  * @param i4, integer
%%  * @param s1, string/iolist. D's arg6 is NULL if not given by Erlang
%%  * @param s2, string/iolist. D's arg7 is NULL if not given by Erlang
%%  * @param s3, string/iolist. D's arg8 is NULL if not given by Erlang
%%  * @param s4, string/iolist. D's arg9 is NULL if not given by Erlang
%%  */
%% probe user_trace__i4s4(char *proc, char *user_tag,
%%                        int i1, int i2, int i3, int i4,
%%                        char *s1, char *s2, char *s3, char *s4);
%%
%% The convention that we'll use of these probes is:
%%   param  D arg name  use
%%   -----  ----------  ---
%%   i1     arg2        Application category (see below)
%%   i2     arg3        1 = function entry, 2 = function return
%%                      NOTE! Not all function entry probes have a return probe
%%   i3-i4  arg4-arg5   Varies, zero usually means unused (but not always!)
%%   s1     arg6        Module name
%%   s2     arg7        Function name
%%   s3-4   arg8-arg9   Varies, NULL means unused
%%
-define(DT_BLOCK_OP,        700).
-define(DT_SERVICE_OP,      701).
-define(DT_BUCKET_OP,       702).
-define(DT_OBJECT_OP,       703).
%% perhaps add later? -define(DT_AUTH_OP,         704).
-define(DT_WM_OP,           705).

-define(USER_BUCKETS_PRUNE_TIME, 86400). %% one-day in seconds
-define(DEFAULT_CLUSTER_ID_TIMEOUT,5000).
-define(DEFAULT_AUTH_MODULE, riak_cs_s3_auth).
-define(DEFAULT_LIST_OBJECTS_MAX_KEYS, 1000).
-define(DEFAULT_MD5_CHUNK_SIZE, 1048576). %% 1 MB
-define(DEFAULT_MANIFEST_WARN_SIBLINGS, 20).
-define(DEFAULT_MANIFEST_WARN_BYTES, 5*1024*1024). %% 5MB
-define(DEFAULT_MANIFEST_WARN_HISTORY, 30).
-define(DEFAULT_MAX_PART_NUMBER, 10000).

%% timeout hitting Riak PB API
-define(DEFAULT_RIAK_TIMEOUT, 60000).

%% General system info
-define(WORD_SIZE, erlang:system_info(wordsize)).

-define(DEFAULT_POLICY_MODULE, riak_cs_s3_policy).

-record(access_v1, {
          method :: 'PUT' | 'GET' | 'POST' | 'DELETE' | 'HEAD',
          target :: atom(), % object | object_acl | ....
          id :: string(),
          bucket :: binary(),
          key = <<>> :: undefined | binary(),
          req %:: #wm_reqdata{} % request of webmachine
         }).

-type access() :: #access_v1{}.

-type policy() :: riak_cs_s3_policy:policy1().

-type digest() :: binary().

-define(USERMETA_BUCKET, "RCS-bucket").
-define(USERMETA_KEY,    "RCS-key").
-define(USERMETA_BCSUM,  "RCS-bcsum").

-define(OBJECT_BUCKET_PREFIX, <<"0o:">>).       % Version # = 0
-define(BLOCK_BUCKET_PREFIX, <<"0b:">>).        % Version # = 0

-define(MAX_S3_KEY_LENGTH, 1024).

-type mochiweb_headers() :: gb_trees:tree().

-endif.
