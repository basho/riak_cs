
-record(moss_user, {
          name :: string(),
          key_id :: string(),
          key_secret :: string(),
          buckets = []}).

-record(moss_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_bypass :: atom(),
                  user :: #moss_user{}}).

-define(USER_BUCKET, <<"moss.users">>).
