
-record(moss_user, {
          name :: binary(),
          key_id :: binary(),
          key_secret :: binary(),
          buckets = []}).

-record(moss_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_bypass :: atom(),
                  auth_mod=riak_kv_passthru_auth :: atom(),
                  user :: #moss_user{}}).

-define(USER_BUCKET, <<"moss.users">>).

