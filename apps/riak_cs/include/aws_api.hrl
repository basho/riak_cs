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

-ifndef(RIAK_CS_S3_API_HRL).
-define(RIAK_CS_S3_API_HRL, included).

-define(S3_ROOT_HOST, "s3.amazonaws.com").
-define(IAM_ROOT_HOST, "iam.amazonaws.com").

-define(SUBRESOURCES, ["acl", "location", "logging", "notification", "partNumber",
                       "policy", "requestPayment", "torrent", "uploadId", "uploads",
                       "versionId", "versioning", "versions", "website",
                       "delete", "lifecycle"]).

% type and record definitions for S3 policy API
-type s3_object_action() :: 's3:GetObject' | 's3:GetObjectVersion'
                          | 's3:GetObjectAcl' | 's3:GetObjectVersionAcl'
                          | 's3:PutObject' | 's3:PutObjectAcl'
                          | 's3:PutObjectVersionAcl'
                          | 's3:DeleteObject' | 's3:DeleteObjectVersion'
                          | 's3:ListObjectVersions'
                          | 's3:ListMultipartUploadParts'
                          | 's3:AbortMultipartUpload'
                        %%| 's3:GetObjectTorrent'         we never do this
                        %%| 's3:GetObjectVersionTorrent'  we never do this
                          | 's3:RestoreObject'.

-define(SUPPORTED_OBJECT_ACTION,
        [ 's3:GetObject', 's3:GetObjectAcl', 's3:PutObject', 's3:PutObjectAcl',
          's3:DeleteObject',
          's3:ListObjectVersions',
          's3:ListMultipartUploadParts', 's3:AbortMultipartUpload' ]).

-type s3_bucket_action() :: 's3:CreateBucket'
                          | 's3:DeleteBucket'
                          | 's3:ListBucket'
                          | 's3:ListBucketVersions'
                          | 's3:ListAllMyBuckets'
                          | 's3:ListBucketMultipartUploads'
                          | 's3:GetBucketAcl' | 's3:PutBucketAcl'
                          | 's3:GetBucketVersioning' | 's3:PutBucketVersioning'
                          | 's3:GetBucketRequestPayment' | 's3:PutBucketRequestPayment'
                          | 's3:GetBucketLocation'
                          | 's3:GetBucketPolicy' | 's3:DeleteBucketPolicy' | 's3:PutBucketPolicy'
                          | 's3:GetBucketNotification' | 's3:PutBucketNotification'
                          | 's3:GetBucketLogging' | 's3:PutBucketLogging'
                          | 's3:GetBucketWebsite' | 's3:PutBucketWebsite' | 's3:DeleteBucketWebsite'
                          | 's3:GetLifecycleConfiguration' | 's3:PutLifecycleConfiguration'.

-define(SUPPORTED_BUCKET_ACTION,
        [ 's3:CreateBucket', 's3:DeleteBucket', 's3:ListBucket', 's3:ListAllMyBuckets',
          's3:GetBucketAcl', 's3:PutBucketAcl',
          's3:GetBucketPolicy', 's3:DeleteBucketPolicy', 's3:PutBucketPolicy',
          's3:GetBucketVersioning', 's3:PutBucketVersioning',
          's3:ListBucketMultipartUploads']).

% one of string, numeric, date&time, boolean, IP address, ARN and existence of condition keys
-type string_condition_type() :: 'StringEquals' | streq            | 'StringNotEquals' | strneq
                               | 'StringEqualsIgnoreCase' | streqi | 'StringNotEqualsIgnoreCase' | strneqi
                               | 'StringLike' | strl               | 'StringNotLike' | strnl.

-define(STRING_CONDITION_ATOMS,
        [ 'StringEquals' , streq,           'StringNotEquals', strneq,
          'StringEqualsIgnoreCase', streqi, 'StringNotEqualsIgnoreCase', strneqi,
          'StringLike', strl,               'StringNotLike' , strnl]).

-type numeric_condition_type() :: 'NumericEquals' | numeq      | 'NumericNotEquals' | numneq
                                | 'NumericLessThan'  | numlt   | 'NumericLessThanEquals' | numlteq
                                | 'NumericGreaterThan' | numgt | 'NumericGreaterThanEquals' | numgteq.

-define(NUMERIC_CONDITION_ATOMS,
        [ 'NumericEquals', numeq,      'NumericNotEquals', numneq,
          'NumericLessThan' , numlt,   'NumericLessThanEquals', numlteq,
          'NumericGreaterThan', numgt, 'NumericGreaterThanEquals', numgteq]).

-type date_condition_type() :: 'DateEquals'         | dateeq
                             | 'DateNotEquals'      | dateneq
                             | 'DateLessThan'       | datelt
                             | 'DateLessThanEquals' | datelteq
                             | 'DateGreaterThan'    | dategt
                             | 'DateGreaterThanEquals' | dategteq.

-define(DATE_CONDITION_ATOMS,
        [ 'DateEquals',            dateeq
        , 'DateNotEquals',         dateneq
        , 'DateLessThan',          datelt
        , 'DateLessThanEquals',    datelteq
        , 'DateGreaterThan',       dategt
        , 'DateGreaterThanEquals', dategteq
        ]
       ).


-type ip_addr_condition_type() :: 'IpAddress' | 'NotIpAddress'.

-define(IP_ADDR_CONDITION_ATOMS,
        ['IpAddress', 'NotIpAddress']).

-type condition_pair() :: {date_condition_type(), [{'aws:CurrentTime', binary()}]}
                        | {numeric_condition_type(), [{'aws:EpochTime', non_neg_integer()}]}
                        | {boolean(), 'aws:SecureTransport'}
                        | {ip_addr_condition_type(), [{'aws:SourceIp', {IP::inet:ip_address(), inet:ip_address()}}]}
                        | {string_condition_type(),  [{'aws:UserAgent', binary()}]}
                        | {string_condition_type(),  [{'aws:Referer', binary()}]}.

-type service() :: s3 | iam.

-record(arn_v1, { provider = aws :: aws
                , service  = s3  :: service()
                , region         :: string()
                , id             :: binary()
                , path           :: string()
                }
       ).

-type arn() :: #arn_v1{}.
-define(S3_ARN, #arn_v1).

-type principal() :: '*'
                   | [{canonical_id, string()}|{aws, '*'}].

-record(statement, { sid = undefined :: undefined | binary() % had better use uuid: should be UNIQUE
                   , effect = deny :: allow | deny
                   , principal  = [] :: principal()
                   , action     = [] :: [ s3_object_action() | s3_bucket_action() ] | '*'
                   , not_action = [] :: [ s3_object_action() | s3_bucket_action() ] | '*'
                   , resource   = [] :: [ arn() ] | '*'
                   , condition_block = [] :: [ condition_pair() ]
                   }
       ).
-define(S3_STATEMENT, #statement).

-record(amz_policy, { version = <<"2008-10-17">> :: binary()  % no other value is allowed than default
                    , id = undefined :: undefined | binary()  % had better use uuid: should be UNIQUE
                    , statement = [] :: [#statement{}]
                    , creation_time = os:system_time(millisecond) :: non_neg_integer()
         }).
-type amz_policy() :: #amz_policy{}.
-define(AMZ_POLICY, #amz_policy).

-record(policy_v1, { arn :: string() | undefined
                   , attachment_count :: non_neg_integer() | undefined
                   , create_date = rts:iso8601(os:system_time(millisecond)) :: string() | undefined
                   , default_version_id :: string() | undefined
                   , description :: string() | undefined
                   , is_attachable :: boolean() | undefined
                   , path :: string() | undefined
                   , permissions_boundary_usage_count :: non_neg_integer() | undefined
                   , policy_id :: string() | undefined
                   , policy_name :: string() | undefined
                   , tags :: [tag()] | undefined
                   , update_date :: binary() | undefined
         }).
-type policy() :: #policy_v1{}.
-define(IAM_POLICY, #policy_v1).


-record(permissions_boundary, { permissions_boundary_arn :: arn()
                              , permissions_boundary_type = <<"Policy">> :: binary()
                              }
).
-type permissions_boundary() :: #permissions_boundary{}.
-define(IAM_PERMISSION_BOUNDARY, #permissions_boundary).


-record(tag, { key :: string()
             , value :: string()
             }
       ).
-type tag() :: #tag{}.
-define(IAM_TAG, #tag).


-record(role_last_used, { last_used_date :: non_neg_integer()
                        , region :: binary()
                        }
       ).
-type role_last_used() :: #role_last_used{}.
-define(IAM_ROLE_LAST_USED, #role_last_used).

-record(role_v1, { arn :: arn()
                 , assume_role_policy_document :: binary()
                 , create_date = rts:iso8601(os:system_time(millisecond)) :: non_neg_integer()
                 , description :: binary()
                 , max_session_duration :: non_neg_integer()
                 , path :: binary()
                 , permissions_boundary :: permissions_boundary()
                 , role_id :: binary()
                 , role_last_used :: role_last_used()
                 , role_name :: binary()
                 , tags :: [tag()]
                 }
       ).
-type role() :: #role_v1{}.
-define(IAM_ROLE, #role_v1).

-record(saml_provider_v1, { arn :: arn()
                          , create_date = rts:iso8601(os:system_time(millisecond)) :: non_neg_integer()
                          , saml_metadata_document :: string()
                          , tags :: [tag()]
                          , valid_until :: non_neg_integer()
                          }
       ).
-type saml_provider() :: #saml_provider_v1{}.
-define(IAM_SAML_PROVIDER, #saml_provider_v1).


-record(assumed_role_user, { arn :: arn()
                           , assumed_role_id :: binary()
                           }
       ).
-type assumed_role_user() :: #assumed_role_user{}.

-record(credentials, { access_key_id :: binary()
                     , expiration :: non_neg_integer()
                     , secret_access_key :: binary()
                     , session_token :: binary()
                     }
       ).
-type credentials() :: #credentials{}.


-define(DEFAULT_REGION, "us-east-1").

-define(AUTH_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers").
-define(ALL_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AllUsers").
-define(LOG_DELIVERY_GROUP, "http://acs.amazonaws.com/groups/s3/LogDelivery").

-endif.
