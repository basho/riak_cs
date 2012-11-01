%% see also: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html
%% non mandatory keys have `| undefined' as a
%% type option.
-record(list_objects_request_v1, {
        %% the name of the bucket
        name :: binary(),

        %% how many keys to return in the response
        max_keys :: non_neg_integer(),

        %% a 'starts-with' parameter
        prefix :: binary() | undefined,

        %% a binary to group keys by
        delimiter :: binary() | undefined,

        %% the key to start with
        marker :: binary() | undefined}).
-type list_object_request() :: #list_objects_request_v1{}.

-record(list_objects_response_v1, {
        %% Params just echoed back from the request --------------------------

        %% the name of the bucket
        name :: binary(),

        %% how many keys were requested to be
        %% returned in the response
        max_keys :: non_neg_integer(),

        %% a 'starts-with' parameter
        prefix :: binary() | undefined,

        %% a binary to group keys by
        delimiter :: binary() | undefined,

        %% the key to start with
        marker :: binary() | undefined,

        %% The actual response -----------------------------------------------
        is_truncated :: boolean(),

        contents :: list(list_objects_key_content()),

        common_prefixes :: list(list_objects_common_prefixes())}).
-type list_object_response() :: #list_objects_response_v1{}.

-record(list_objects_key_content_v1, {
        key :: binary(),
        last_modified :: term(),
        etag :: binary(),
        size :: non_neg_integer(),
        owner :: list_objects_owner(),
        storage_class :: binary()
        }).
-type list_objects_key_content() :: #list_objects_key_content_v1{}.

-record(list_objects_owner_v1, {
        id :: binary(),
        display_name :: binary()
        }).
-type list_objects_owner() :: #list_objects_owner_v1{}.

-record(list_objects_common_prefixes_v1, {
        prefix :: binary()
        }).
-type list_objects_common_prefixes() :: #list_objects_common_prefixes_v1{}.
