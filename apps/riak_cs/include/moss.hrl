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

-include("aws_api.hrl").

%% All those cases of `| undefined | #{} | binary()` to proper field
%% types in various records in this file are for the sake of
%% exprec. Because in the exprec-generated frommap_thing, fields can
%% briefly be assigned a value of `undefined` or an unconverted
%% binary, dialyzer rightfully takes issue with it. Further, in custom
%% record conversion functions like riak_cs_iam:exprec_role, some
%% other fields exist as maps (before they are converted into proper
%% sub-records). Hence this type dilution and overall
%% kludginess. There surely is a less painful solution, of which I am,
%% as of releasing 3.2, sadly unaware.

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
                     , status = enabled :: undefined | enabled | disabled
                     }).

%% this now in part logically belongs in aws_api.hrl
-record(rcs_user_v3, { arn :: undefined | flat_arn()
                     , path = <<"/">> :: binary()
                     , create_date = os:system_time(millisecond) :: non_neg_integer()
                     %% , user_id :: binary() %% maps to id
                     %% , user_name :: binary() %% maps to name
                     , password_last_used :: undefined | null | non_neg_integer()
                     , permissions_boundary :: undefined | #{} | permissions_boundary()
                     , tags = [] :: [#{} | tag()]
                     , attached_policies = [] :: [flat_arn()]

                     , name :: undefined | binary()
                     , display_name :: undefined | binary()
                     , email :: undefined | binary()
                     , key_id :: undefined | binary()
                     , key_secret :: undefined | binary()
                     , id :: undefined | binary()
                     , buckets = [] :: [#{} | cs_bucket()]
                     , status = enabled :: enabled | disabled | binary()
                     }).

-type moss_user() :: #rcs_user_v2{} | #moss_user_v1{}.
-type rcs_user() :: #rcs_user_v3{}.
-define(IAM_USER, #rcs_user_v3).
-define(RCS_USER, #rcs_user_v3).


%% Bucket
-record(moss_bucket, { name :: string()
                     , creation_date :: term()
                     , acl :: #acl_v1{}}).

-record(moss_bucket_v1, { name :: string() | binary()
                        , last_action :: undefined | created | deleted
                        , creation_date :: undefined | string()
                        , modification_time :: undefined | erlang:timestamp()
                        , acl :: undefined | #acl_v2{}
                        }).

-record(moss_bucket_v2, { name :: undefined | binary()
                        , last_action :: undefined | created | deleted | binary()
                        , creation_date = os:system_time(millisecond) :: non_neg_integer()
                        , modification_time :: undefined | non_neg_integer()
                        , acl :: undefined | null | #{} | acl()
                        }).

-type cs_bucket() :: #moss_bucket_v2{}.
-define(RCS_BUCKET, #moss_bucket_v2).

-type bucket_operation() :: create | delete | update_acl | update_policy
                          | delete_policy | update_versioning.
-type bucket_action() :: created | deleted.


%% federated users

-record(temp_session, { assumed_role_user :: assumed_role_user()
                      , role :: role()
                      , credentials :: credentials()
                      , duration_seconds :: non_neg_integer()
                      , created = os:system_time(millisecond) :: non_neg_integer()
                      , inline_policy :: undefined | flat_arn()
                      , session_policies :: [flat_arn()]
                      , subject :: binary()
                      , source_identity :: binary()  %% both this and the following can provide the email
                      , email :: binary()            %% for our internal rcs_user
                      , user_id :: binary()
                      , canonical_id :: binary()
                      }
       ).
-type temp_session() :: #temp_session{}.
-define(TEMP_SESSION, #temp_session).

-endif.
