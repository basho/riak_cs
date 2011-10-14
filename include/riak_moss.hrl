
-record(rs3_user, {
          name=[] :: string(),
          key_id=[] :: string(),
          key_data=[] :: string(),
          buckets = []}).

-record(rs3_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_mod :: atom()}).

-define(USER_BUCKET, <<"moss.users">>).

