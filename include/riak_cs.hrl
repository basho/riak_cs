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

-include_lib("riak_cs_manifests/include/riak_cs_manifests.hrl").

-define(RCS_BUCKET, #moss_bucket_v1).
-define(MOSS_USER, #rcs_user_v2).
-define(RCS_USER, #rcs_user_v2).

-record(moss_user, {
          name :: string(),
          key_id :: string(),
          key_secret :: string(),
          buckets = []}).

-record(moss_user_v1, {
          name :: string(),
          display_name :: string(),
          email :: string(),
          key_id :: string(),
          key_secret :: string(),
          canonical_id :: string(),
          buckets=[] :: [cs_bucket()]}).

-record(rcs_user_v2, {
          name :: string(),
          display_name :: string(),
          email :: string(),
          key_id :: string(),
          key_secret :: string(),
          canonical_id :: string(),
          buckets=[] :: [cs_bucket()],
          status=enabled :: enabled | disabled}).
-type moss_user() :: #rcs_user_v2{} | #moss_user_v1{}.
-type rcs_user() :: #rcs_user_v2{} | #moss_user_v1{}.

-record(moss_bucket, {
          name :: string(),
          creation_date :: term(),
          acl :: acl()}).

-record(moss_bucket_v1, {
          name :: string() | binary(),
          last_action :: created | deleted,
          creation_date :: string(),
          modification_time :: erlang:timestamp(),
          acl :: acl()}).

-type cs_bucket() :: #moss_bucket_v1{}.
-type bucket_operation() :: create | delete | update_acl | update_policy
                          | delete_policy.
-type bucket_action() :: created | deleted.

-record(context, {start_time :: erlang:timestamp(),
                  auth_bypass :: atom(),
                  user :: undefined | moss_user(),
                  user_object :: riakc_obj:riakc_obj(),
                  bucket :: binary(),
                  acl :: 'undefined' | acl(),
                  requested_perm :: acl_perm(),
                  riak_client :: riak_client(),
                  rc_pool :: atom(),    % pool name which riak_client belongs to
                  auto_rc_close = true :: boolean(),
                  submodule :: atom(),
                  exports_fun :: function(),
                  auth_module :: atom(),
                  response_module :: atom(),
                  policy_module :: atom(),
                  local_context :: term(),
                  api :: atom()
                 }).

-record(key_context, {manifest :: 'notfound' | lfs_manifest(),
                      upload_id :: 'undefined' | binary(),
                      part_number :: 'undefined' | integer(),
                      part_uuid :: 'undefined' | binary(),
                      get_fsm_pid :: pid(),
                      putctype :: string(),
                      bucket :: binary(),
                      bucket_object :: undefined | notfound | riakc_obj:riakc_obj(),
                      key :: list(),
                      owner :: 'undefined' | string(),
                      size :: non_neg_integer(),
                      content_md5 :: 'undefined' | binary(),
                      update_metadata=false :: boolean()}).


-type riak_client() :: pid().

-define(USER_BUCKET, <<"moss.users">>).
-define(ACCESS_BUCKET, <<"moss.access">>).
-define(STORAGE_BUCKET, <<"moss.storage">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).
-define(GC_BUCKET, <<"riak-cs-gc">>).
-define(FREE_BUCKET_MARKER, <<"0">>).
-define(DEFAULT_MAX_BUCKETS_PER_USER, 100).
-define(XML_PROLOG, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").
-define(S3_XMLNS, 'http://s3.amazonaws.com/doc/2006-03-01/').
-define(DEFAULT_STANCHION_IP, "127.0.0.1").
-define(DEFAULT_STANCHION_PORT, 8085).
-define(DEFAULT_STANCHION_SSL, true).
-define(MD_BAG, <<"X-Rcs-Bag">>).
-define(MD_ACL, <<"X-Moss-Acl">>).
-define(MD_POLICY, <<"X-Rcs-Policy">>).
-define(EMAIL_INDEX, <<"email_bin">>).
-define(ID_INDEX, <<"c_id_bin">>).
-define(KEY_INDEX, <<"$key">>).
-define(AUTH_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers").
-define(ALL_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AllUsers").
-define(LOG_DELIVERY_GROUP, "http://acs.amazonaws.com/groups/s3/LogDelivery").
-define(N_VAL_1_GET_REQUESTS, true).
-define(DEFAULT_PING_TIMEOUT, 5000).
-define(JSON_TYPE, "application/json").
-define(XML_TYPE, "application/xml").
-define(S3_API_MOD, riak_cs_s3_rewrite).
-define(S3_LEGACY_API_MOD, riak_cs_s3_rewrite_legacy).
-define(OOS_API_MOD, riak_cs_oos_rewrite).
-define(S3_RESPONSE_MOD, riak_cs_s3_response).
-define(OOS_RESPONSE_MOD, riak_cs_oos_response).

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

%% just to persuade dyalizer
-type crypto_context() :: {'md4' | 'md5' | 'ripemd160' | 'sha' |
                           'sha224' | 'sha256' | 'sha384' | 'sha512',
                           binary()}.
-type digest() :: binary().

-define(USERMETA_BUCKET, "RCS-bucket").
-define(USERMETA_KEY,    "RCS-key").
-define(USERMETA_BCSUM,  "RCS-bcsum").

-define(OBJECT_BUCKET_PREFIX, <<"0o:">>).       % Version # = 0
-define(BLOCK_BUCKET_PREFIX, <<"0b:">>).        % Version # = 0
