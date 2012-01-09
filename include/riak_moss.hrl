
-record(moss_user, {
          name :: string(),
          key_id :: string(),
          key_secret :: string(),
          buckets = []}).
-type moss_user() :: #moss_user{}.

-record(moss_bucket, {
          name :: string(),
          creation_date :: term()}).
-type moss_bucket() :: #moss_bucket{}.

-record(context, {auth_bypass :: atom(),
                  user :: #moss_user{}}).

-record(key_context, {context :: #context{},
                      doc_metadata :: term(),
                      get_fsm_pid :: pid(),
                      putctype :: string(),
                      bucket :: list(),
                      key :: list(),
                      size :: non_neg_integer()}).

-define(USER_BUCKET, <<"moss.users">>).
-define(MAX_CONTENT_LENGTH, 5368709120). %% 5 GB
-define(DEFAULT_LFS_BLOCK_SIZE, 1048576).%% 1 MB
