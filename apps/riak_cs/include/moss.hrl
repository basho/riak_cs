%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-ifndef(RCS_COMMON_MOSS_HRL).
-define(RCS_COMMON_MOSS_HRL, included).

-include("acl.hrl").

-define(RCS_BUCKET, #moss_bucket_v1).
-define(MOSS_USER, #rcs_user_v2).
-define(RCS_USER, #rcs_user_v2).

%% User
-record(moss_user, { name :: string()
                   , key_id :: string()
                   , key_secret :: string()
                   , buckets = []
                   }).

-record(moss_user_v1, { name :: string()
                      , display_name :: string()
                      , email :: string()
                      , key_id :: string()
                      , key_secret :: string()
                      , canonical_id :: string()
                      , buckets = [] :: [cs_bucket()]
                      }).

-record(rcs_user_v2, { name :: string()
                     , display_name :: string()
                     , email :: string()
                     , key_id :: string()
                     , key_secret :: string()
                     , canonical_id :: string()
                     , buckets = [] :: [cs_bucket()]
                     , status = enabled :: enabled | disabled
                     }).

-type moss_user() :: #rcs_user_v2{} | #moss_user_v1{}.
-type rcs_user() :: #rcs_user_v2{} | #moss_user_v1{}.


%% Bucket
-record(moss_bucket, { name :: string()
                     , creation_date :: term()
                     , acl :: acl()}).

-record(moss_bucket_v1, { name :: string() | binary()
                        , last_action :: undefined | created | deleted
                        , creation_date :: undefined | string()
                        , modification_time :: undefined | non_neg_integer()
                        , acl :: undefined | acl()
                        }).

-type cs_bucket() :: #moss_bucket_v1{}.

-type bucket_operation() :: create | delete | update_acl | update_policy
                          | delete_policy | update_versioning.
-type bucket_action() :: created | deleted.

-record(bucket_versioning, { status = suspended :: enabled | suspended
                           , mfa_delete = disabled :: disabled | enabled
                           %% Riak CS extensions
                           , use_subversioning = false :: boolean()
                           , can_update_versions = false :: boolean()
                           , repl_siblings = true :: boolean()
                           }).
-type bucket_versioning() :: #bucket_versioning{}.


-endif.
