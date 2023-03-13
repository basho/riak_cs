%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

-include("aws_api.hrl").
-include("riak_cs_web.hrl").

-define(RCS_VERSION, 030200).

-type riak_client() :: pid().

-define(AWS_API_MOD, riak_cs_aws_rewrite).
-define(OOS_API_MOD, riak_cs_oos_rewrite).
-define(AWS_RESPONSE_MOD, riak_cs_s3_response).
-define(OOS_RESPONSE_MOD, riak_cs_oos_response).

-define(DEFAULT_AUTH_MODULE, riak_cs_aws_auth).
-define(DEFAULT_POLICY_MODULE, riak_cs_s3_policy).

-define(DEFAULT_STANCHION_IP, "127.0.0.1").
-define(DEFAULT_STANCHION_PORT, 8085).
-define(DEFAULT_STANCHION_SSL, true).

-define(DEFAULT_MAX_BUCKETS_PER_USER, 100).
-define(DEFAULT_MAX_CONTENT_LENGTH, 5368709120). %% 5 GB
-define(DEFAULT_LFS_BLOCK_SIZE, 1048576).%% 1 MB

-define(XML_PROLOG, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").
-define(S3_XMLNS, "http://s3.amazonaws.com/doc/2006-03-01/").
-define(IAM_XMLNS, "https://iam.amazonaws.com/doc/2010-05-08/").

-define(USER_BUCKET, <<"moss.users">>).
-define(ACCESS_BUCKET, <<"moss.access">>).
-define(STORAGE_BUCKET, <<"moss.storage">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).
-define(SERVICE_BUCKET, <<"moss.service">>).
-define(IAM_BUCKET, <<"moss.iam">>).
-define(GC_BUCKET, <<"riak-cs-gc">>).
-define(FREE_BUCKET_MARKER, <<"0">>).
-define(FREE_ROLE_MARKER, <<"0">>).

-define(MD_BAG, <<"X-Rcs-Bag">>).
-define(MD_ACL, <<"X-Moss-Acl">>).
-define(MD_POLICY, <<"X-Rcs-Policy">>).
-define(MD_VERSIONING, <<"X-Rcs-Versioning">>).

-define(USERMETA_BUCKET, "RCS-bucket").
-define(USERMETA_KEY, "RCS-key").
-define(USERMETA_BCSUM, "RCS-bcsum").

-define(EMAIL_INDEX, <<"email_bin">>).
-define(ID_INDEX, <<"c_id_bin">>).
-define(KEY_INDEX, <<"$key">>).
-define(ROLE_ID_INDEX, <<"role_id_bin">>).
-define(ROLE_NAME_INDEX, <<"role_name_bin">>).
-define(ROLE_PATH_INDEX, <<"role_path_bin">>).

-define(STANCHION_DETAILS_KEY, <<"stanchion">>).

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
-define(DT_IAM_OP,          704).
-define(DT_WM_OP,           705).

-define(USER_BUCKETS_PRUNE_TIME, 86400). %% one-day in seconds
-define(DEFAULT_CLUSTER_ID_TIMEOUT,5000).
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

-define(OBJECT_BUCKET_PREFIX, <<"0o:">>).       % Version # = 0
-define(BLOCK_BUCKET_PREFIX, <<"0b:">>).        % Version # = 0

-define(MAX_S3_KEY_LENGTH, 1024).

-define(VERSIONED_KEY_SEPARATOR, <<5>>).

-endif.
