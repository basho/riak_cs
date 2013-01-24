-define(ROOT_HOST, "s3.amazonaws.com").
-define(SUBRESOURCES, ["acl", "location", "logging", "notification", "partNumber",
                       "policy", "requestPayment", "torrent", "uploadId", "uploads",
                       "versionId", "versioning", "versions", "website"]).



% type and record definitions for S3 policy API
-type s3_object_action() :: 's3:GetObject'       | 's3:GetObjectVersion'
                       | 's3:GetObjectAcl'    | 's3:GetObjectVersionAcl'
                       | 's3:PutObject'       | 's3:PutObjectAcl'
                       | 's3:PutObjectVersionAcl'
                       | 's3:DeleteObject'    | 's3:DeleteObjectVersion'
                       | 's3:ListMultipartUploadParts' %to be supported
                       | 's3:AbortMultipartUpload'     %to be supported
                       %| 's3:GetObjectTorrent'         we never do this
                       %| 's3:GetObjectVersionTorrent'  we never do this
                       | 's3:RestoreObject'.

-define(SUPPORTED_OBJECT_ACTION,
        [ 's3:GetObject', 's3:GetObjectAcl', 's3:PutObject', 's3:PutObjectAcl',
          's3:DeleteObject' ]).

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
          's3:GetBucketPolicy', 's3:DeleteBucketPolicy', 's3:PutBucketPolicy']).

% one of string, numeric, date&time, boolean, IP address, ARN and existence of condition keys
-type string_condition_type() :: 'StringEquals' | streq            | 'StringNotEquals' | strneq
                               | 'StringEqualsIgnoreCase' | streqi | 'StringNotEqualsIgnoreCase' | streqni
                               | 'StringLike' | strl               | 'StringNotLike' | strnl.

-type numeric_condition_type() :: 'NumericEquals' | numeq      | 'NumericNotEquals' | numneq
                                | 'NumericLessThan'  | numlt   | 'NumericLessThanEquals' | numlteq
                                | 'NumericGreaterThan' | numgt | 'NumericGreaterThanEquals' | numgteq.


-type date_condition_type() :: 'DateEquals'         | dateeq
                             | 'DateNotEquals'      | dateneq
                             | 'DateLessThan'       | datelt
                             | 'DateLessThanEquals' | datelteq
                             | 'DateGreaterThan'    | dategt
                             | 'DateGreaterThanEquals' | dategteq.

-type ip_addr_condition_type() :: 'IpAddress' | 'NotIpAddress'.

-type condition_pair() :: {date_condition_type(), [{'aws:CurrentTime', binary()}]}
                        | {numeric_condition_type(), [{'aws:EpochTime', non_neg_integer()}]}
                        | {boolean(), 'aws:SecureTransport'}
                        | {ip_addr_condition_type(), [{'aws:SourceIp', {IP::inet:ip_addr(), inet:ip_addr()}}]}
                        | {string_condition_type(),  [{'aws:UserAgent', binary()}]}
                        | {string_condition_type(),  [{'aws:Referer', binary()}]}.

-record(arn_v1, {
          provider = aws :: aws,
          service  = s3  :: s3,
          region         :: binary(),
          id             :: binary(),
          path           :: binary()
         }).

-type arn() :: #arn_v1{}.

-record(statement, {
          sid = undefined :: binary(), % had better use uuid: should be UNIQUE
          effect = deny :: allow | deny,
          principal  = [] :: [{binary(), [binary()]}],
          action     = [] :: [ s3_object_action() | s3_bucket_action() ] | '*',
          not_action = [] :: [ s3_object_action() | s3_bucket_action() ] | '*',
          resource =   [] :: [ arn() ] | '*',
          condition_block = [] :: [ condition_pair() ]
         }).

-record(policy_v1, {
          version = <<"2008-10-17">> :: binary(),  % no other value is allowed than default
          id = undefined :: binary(),  % had better use uuid: should be UNIQUE
          statement = [] :: #statement{}
         }).

-type principal() :: '*'
                   | [{canonical_id, string()}|{aws, '*'}].

-define(POLICY, #policy_v1).
