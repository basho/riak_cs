
-record(rs3_user, {
          name :: binary(),
          key_id :: binary(),
          key_data :: binary(),
          buckets = []}).

-record(rs3_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_mod :: atom()}).

-define(USER_BUCKET, <<"moss.users">>).

