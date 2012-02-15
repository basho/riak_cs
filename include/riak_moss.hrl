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

-type bucket_acl() :: string().
-record(moss_bucket, {
          name :: string(),
          creation_date :: term(),
          acl :: bucket_acl()}).

-record(moss_bucket_v1, {
          name :: string(),
          last_action :: created | deleted,
          modification_time :: erlang:timestamp(),
          acl :: bucket_acl()}).
-type moss_bucket() :: #moss_bucket_v1{}.

-record(context, {auth_bypass :: atom(),
                  user :: #moss_user{}}).

-record(key_context, {context :: #context{},
                      doc_metadata :: term(),
                      get_fsm_pid :: pid(),
                      putctype :: string(),
                      bucket :: list(),
                      key :: list(),
                      size :: non_neg_integer()}).

-type acl_perm() :: 'READ' | 'WRITE' | 'READ_ACP' | 'WRITE_ACP' | 'FULL_CONTROL'.
-type acl_perms() :: [acl_perm()].
-type acl_grant() :: {{string(), string()}, acl_perms()}.
-record(acl_v1, {owner={"", ""} :: {string(), string()},
                 grants=[] :: [acl_grant()]}).
-type acl_v1() :: #acl_v1{}.

-define(ACL, #acl_v1).
-define(MOSS_BUCKET, #moss_bucket_v1).
-define(MOSS_USER, #moss_user_v1).
-define(USER_BUCKET, <<"moss.users">>).
-define(MAX_CONTENT_LENGTH, 5368709120). %% 5 GB
-define(DEFAULT_LFS_BLOCK_SIZE, 1048576).%% 1 MB
-define(XML_PROLOG, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").
-define(DEFAULT_STANCHION_IP, "127.0.0.1").
-define(DEFAULT_STANCHION_PORT, 8085).
-define(DEFAULT_STANCHION_SSL, true).
