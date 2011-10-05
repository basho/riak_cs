
-record(rs3_user, {
          name :: binary(),
          key_id :: binary(),
          key_data :: binary(),
          buckets = []}).

-record(rs3_bucket, {
          name :: binary(), 
          creation_date :: term()}).

-define(USER_BUCKET, <<"moss.users">>).

