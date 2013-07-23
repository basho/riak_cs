%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

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
-define(LOREQ, #list_objects_request_v1).

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

        common_prefixes :: list_objects_common_prefixes()}).
-type list_object_response() :: #list_objects_response_v1{}.
-define(LORESP, #list_objects_response_v1).

-record(list_objects_key_content_v1, {
        key :: binary(),
        last_modified :: term(),
        etag :: binary(),
        size :: non_neg_integer(),
        owner :: list_objects_owner(),
        storage_class :: binary()}).
-type list_objects_key_content() :: #list_objects_key_content_v1{}.
-define(LOKC, #list_objects_key_content_v1).

-record(list_objects_owner_v1, {
        id :: binary(),
        display_name :: binary()}).
-type list_objects_owner() :: #list_objects_owner_v1{}.

-type list_objects_common_prefixes() :: list(binary()).

-define(LIST_OBJECTS_CACHE, list_objects_cache).
-define(ENABLE_CACHE, true).
-define(CACHE_TIMEOUT, timer:minutes(15)).
-define(MIN_KEYS_TO_CACHE, 2000).
-define(MAX_CACHE_BYTES, 104857600). % 100MB
-define(KEY_LIST_MULTIPLIER, 1.1).
-define(FOLD_OBJECTS_FOR_LIST_KEYS, false).
