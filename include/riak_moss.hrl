
-record(rs3_user, {
          name :: binary(),
          key_id :: binary(),
          key_secret :: binary(),
          buckets = []}).

-record(rs3_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_bypass :: atom(),
                  auth_mod=riak_kv_passthru_auth :: atom(),
                  user :: #rs3_user{}}).

-define(USER_BUCKET, <<"moss.users">>).

