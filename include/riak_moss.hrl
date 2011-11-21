
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

-record(key_context, {context :: #context{},
                      doc :: term(),
                      putctype :: string(),
                      bucket :: list(),
                      key :: list()}).

-record(lfs_manifest, {
    version :: atom(),
    uuid :: binary(),
    block_size :: integer(),
    bkey :: {term(), term()},
    content_length :: integer(),
    content_md5 :: term(),
    created :: term(),
    finished :: term(),
    active :: boolean(),
    blocks_remaining = sets:new()}).

-define(USER_BUCKET, <<"moss.users">>).
-define(MAX_CONTENT_LENGTH, 10485760).

