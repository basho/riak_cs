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
          buckets=[] :: [moss_bucket()]}).
-type moss_user() :: #moss_user_v1{}.

-record(moss_bucket, {
          name :: string(),
          creation_date :: term(),
          acl :: acl_v1()}).

-record(moss_bucket_v1, {
          name :: string(),
          last_action :: created | deleted,
          creation_date :: string(),
          modification_time :: erlang:timestamp(),
          acl :: acl_v1()}).
-record(moss_bucket_v2, {
          name :: string(),
          last_action :: created | deleted,
          bucket_id :: binary(),
          creation_date :: string(),
          modification_time :: erlang:timestamp(),
          acl :: acl_v1()}).
-type moss_bucket() :: #moss_bucket_v2{}.
-type bucket_operation() :: create | delete | update_acl.
-type bucket_action() :: created | deleted.

-record(context, {auth_bypass :: atom(),
                  user :: moss_user(),
                  user_vclock :: term(),
                  bucket :: binary(),
                  requested_perm :: acl_perm()
                 }).

-record(key_context, {context :: #context{},
                      doc_metadata :: term(),
                      get_fsm_pid :: pid(),
                      putctype :: string(),
                      bucket :: binary(),
                      key :: list(),
                      moss_bucket :: false | #moss_bucket_v2{} | #moss_bucket_v1{},
                      size :: non_neg_integer()}).

-type acl_perm() :: 'READ' | 'WRITE' | 'READ_ACP' | 'WRITE_ACP' | 'FULL_CONTROL'.
-type acl_perms() :: [acl_perm()].
-type group_grant() :: 'AllUsers' | 'AuthUsers'.
-type acl_grantee() :: {string(), string()} | group_grant().
-type acl_grant() :: {acl_grantee(), acl_perms()}.
-record(acl_v1, {owner={"", ""} :: {string(), string()},
                 grants=[] :: [acl_grant()],
                 creation_time=now() :: erlang:timestamp()}).
-type acl_v1() :: #acl_v1{}.

-define(ACL, #acl_v1).
-define(MOSS_BUCKET, #moss_bucket_v2).
-define(MOSS_USER, #moss_user_v1).
-define(USER_BUCKET, <<"moss.users">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).
-define(FREE_BUCKET_MARKER, <<"0">>).
-define(DEFAULT_MAX_CONTENT_LENGTH, 5368709120). %% 5 GB
-define(DEFAULT_LFS_BLOCK_SIZE, 1048576).%% 1 MB
-define(XML_PROLOG, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").
-define(DEFAULT_STANCHION_IP, "127.0.0.1").
-define(DEFAULT_STANCHION_PORT, 8085).
-define(DEFAULT_STANCHION_SSL, true).
-define(MD_ACL, "X-Moss-Acl").
-define(EMAIL_INDEX, <<"email_bin">>).
-define(ID_INDEX, <<"c_id_bin">>).
-define(AUTH_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AuthenticatedUsers").
-define(ALL_USERS_GROUP, "http://acs.amazonaws.com/groups/global/AllUsers").
-define(LOG_DELIVERY_GROUP, "http://acs.amazonaws.com/groups/s3/LogDelivery").
