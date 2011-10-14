
-record(rs3_user, {
          name=[] :: string(),
          key_id=[] :: string(),
          key_data=[] :: string(),
          buckets = []}).

-record(rs3_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_bypass :: atom(),
                  auth_mod=riak_kv_passthru_auth :: atom()}).

-define(USER_BUCKET, <<"moss.users">>).

