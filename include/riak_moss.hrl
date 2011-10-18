
-record(moss_user, {
          name :: binary(),
          key_id :: binary(),
          key_secret :: binary(),
          buckets = []}).

-record(moss_bucket, {
          name :: binary(),
          creation_date :: term()}).

-record(context, {auth_bypass :: atom(),
                  user :: #moss_user{}}).

-record(key_context, {context :: #context{},
                      doc :: term(),
                      bucket :: list(),
                      key :: list()}).

-define(USER_BUCKET, <<"moss.users">>).
