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

-include_lib("public_key/include/public_key.hrl").

-define(S3_ROOT_HOST, "s3.amazonaws.com").
-define(IAM_ROOT_HOST, "iam.amazonaws.com").
-define(STS_ROOT_HOST, "sts.amazonaws.com").

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

-define(SUPPORTED_OBJECT_ACTIONS,
        [ 's3:GetObject', 's3:GetObjectAcl', 's3:PutObject', 's3:PutObjectAcl',
          's3:DeleteObject',
          's3:ListObjectVersions',
          's3:ListMultipartUploadParts', 's3:AbortMultipartUpload'
        ]).

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

-define(SUPPORTED_BUCKET_ACTIONS,
        [ 's3:CreateBucket', 's3:DeleteBucket', 's3:ListBucket', 's3:ListAllMyBuckets',
          's3:GetBucketAcl', 's3:PutBucketAcl',
          's3:GetBucketPolicy', 's3:DeleteBucketPolicy', 's3:PutBucketPolicy',
          's3:GetBucketVersioning', 's3:PutBucketVersioning',
          's3:ListBucketMultipartUploads']).

-type s3_action() :: s3_bucket_action() | s3_object_action().

-define(SUPPORTED_S3_ACTIONS, ?SUPPORTED_BUCKET_ACTIONS ++ ?SUPPORTED_OBJECT_ACTIONS).


-type iam_action() :: 'iam:CreateUser' | 'iam:GetUser' | 'iam:DeleteUser' | 'iam:ListUsers'
                    | 'iam:CreateRole' | 'iam:GetRole' | 'iam:DeleteRole' | 'iam:ListRoles'
                    | 'iam:CreatePolicy' | 'iam:GetPolicy' | 'iam:DeletePolicy' | 'iam:ListPolicies'
                    | 'iam:AttachRolePolicy' | 'iam:DetachRolePolicy'
                    | 'iam:AttachUserPolicy' | 'iam:DetachUserPolicy'
                    | 'iam:CreateSAMLProvider' | 'iam:GetSAMLProvider' | 'iam:DeleteSAMLProvider' | 'iam:ListSAMLProviders'.

-define(SUPPORTED_IAM_ACTIONS,
        [ 'iam:CreateUser', 'iam:GetUser', 'iam:DeleteUser', 'iam:ListUsers'
        , 'iam:CreateRole', 'iam:GetRole', 'iam:DeleteRole', 'iam:ListRoles'
        , 'iam:CreatePolicy', 'iam:GetPolicy', 'iam:DeletePolicy', 'iam:ListPolicies'
        , 'iam:AttachRolePolicy', 'iam:DetachRolePolicy'
        , 'iam:AttachUserPolicy', 'iam:DetachUserPolicy'
        , 'iam:CreateSAMLProvider', 'iam:GetSAMLProvider', 'iam:DeleteSAMLProvider', 'iam:ListSAMLProviders'
        ]
       ).

-type sts_action() :: 'sts:AssumeRoleWithSAML'.

-define(SUPPORTED_STS_ACTIONS,
        [ 'sts:AssumeRoleWithSAML'
        ]
       ).

-type aws_action() :: s3_action() | iam_action() | sts_action()
                    | binary().  %% actions like "s3:Get*'

-define(SUPPORTED_ACTIONS, ?SUPPORTED_S3_ACTIONS ++ ?SUPPORTED_IAM_ACTIONS ++ ?SUPPORTED_STS_ACTIONS).


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

-type aws_service() :: s3 | iam | sts.

-record(arn_v1, { provider = aws :: aws
                , service  = s3  :: aws_service()
                , region         :: binary()
                , id             :: binary()
                , path           :: binary()
                }
       ).

-type arn() :: #arn_v1{}.
-define(S3_ARN, #arn_v1).

-type flat_arn() :: binary().

-type principal() :: '*'
                   | [{canonical_id, binary()} | {aws, '*'}].

-record(statement, { sid = undefined :: undefined | binary() % had better use uuid: should be UNIQUE
                   , effect = deny :: allow | deny
                   , principal  = [] :: principal()
                   , action     = [] :: aws_action() | [aws_action()]
                   , not_action = [] :: aws_action() | [aws_action()]
                   , resource   = [] :: [ flat_arn() ] | '*'
                   , condition_block = [] :: [ condition_pair() ]
                   }
       ).
-define(S3_STATEMENT, #statement).

-define(AMZ_POLICY_VERSION_2008, <<"2008-10-17">>).
-define(AMZ_POLICY_VERSION_2012, <<"2012-10-17">>).
-define(AMZ_POLICY_VERSION_2020, <<"2020-10-17">>).

-record(amz_policy, { version = ?AMZ_POLICY_VERSION_2020 :: binary()
                    , id = undefined :: undefined | binary()  % had better use uuid: should be UNIQUE
                    , statement = [] :: [#statement{}]
                    , creation_time = os:system_time(millisecond) :: non_neg_integer()
         }).
-type amz_policy() :: #amz_policy{}.
-define(AMZ_POLICY, #amz_policy).

-record(policy_v1, { arn :: flat_arn()
                   , path = <<"/">> :: binary()
                   , attachment_count = 0 :: non_neg_integer()
                   , create_date = os:system_time(millisecond) :: non_neg_integer()
                   , default_version_id = <<"v1">> :: binary()
                   , description = <<>> :: binary()
                   , is_attachable = true :: boolean()
                   , permissions_boundary_usage_count = 0 :: non_neg_integer()
                   , policy_document :: binary()
                   , policy_id :: binary()
                   , policy_name :: binary()
                   , tags = [] :: [tag()]
                   , update_date = os:system_time(millisecond) :: non_neg_integer()
         }).
-type policy() :: #policy_v1{}.
-define(IAM_POLICY, #policy_v1).


-record(permissions_boundary, { permissions_boundary_arn :: flat_arn()
                              , permissions_boundary_type = <<"Policy">> :: binary()
                              }
).
-type permissions_boundary() :: #permissions_boundary{}.
-define(IAM_PERMISSION_BOUNDARY, #permissions_boundary).


-record(tag, { key :: binary()
             , value :: binary()
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

-record(role_v1, { arn :: flat_arn()
                 , path = <<"/">> :: binary()
                 , assume_role_policy_document :: binary()
                 , create_date = os:system_time(millisecond) :: non_neg_integer()
                 , description :: binary()
                 , max_session_duration :: non_neg_integer()
                 , permissions_boundary :: flat_arn()  %% permissions_boundary()
                 , role_id :: binary()
                 , role_last_used :: undefined | role_last_used()
                 , role_name :: binary()
                 , tags :: [tag()]
                 , attached_policies :: [flat_arn()]
                 }
       ).
-type role() :: #role_v1{}.
-define(IAM_ROLE, #role_v1).

-record(saml_provider_v1, { arn :: flat_arn()
                          , saml_metadata_document :: binary()
                          , tags :: [tag()]
                          , name :: binary()
                          %% fields populated with values extracted from MD document
                          , create_date = os:system_time(millisecond) :: non_neg_integer()
                          , valid_until :: undefined | non_neg_integer()
                          , entity_id :: undefined | binary()
                          , consume_uri :: binary()
                          , certificates :: undefined | [{signing|encryption, #'OTPCertificate'{}, FP::binary()}]
                          }
       ).
-type saml_provider() :: #saml_provider_v1{}.
-define(IAM_SAML_PROVIDER, #saml_provider_v1).


-record(assumed_role_user, { arn :: flat_arn()
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


-type iam_entity() :: role | policy | user.

-define(ROLE_ID_PREFIX, "AROA").
-define(USER_ID_PREFIX, "AIDA").
-define(POLICY_ID_PREFIX, "ANPA").

-define(IAM_ENTITY_ID_LENGTH, 21).  %% length("AROAJQABLZS4A3QDU576Q").

-define(DEFAULT_REGION, "us-east-1").

-define(AUTH_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers").
-define(ALL_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AllUsers").
-define(LOG_DELIVERY_GROUP, "http://acs.amazonaws.com/groups/s3/LogDelivery").

-define(IAM_CREATE_USER_DEFAULT_EMAIL_HOST, "my-riak-cs-megacorp.com").

-endif.
