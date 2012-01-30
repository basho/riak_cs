%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Tools for summing the storage a user has filled.

-module(riak_moss_storage).

-include("riak_moss.hrl").

-export([
         sum_user/2,
         sum_bucket/2
        ]).

%% @doc Sum the number of bytes stored in active files in all of the
%% given user's directories.  The result is a list of pairs of
%% `{BucketName, Bytes}`.
-spec sum_user(pid(), string()) -> {ok, [{string(), integer()}]}
                                 | {error, term()}.
sum_user(Riak, User) when is_binary(User) ->
    sum_user(Riak, binary_to_list(User));
sum_user(Riak, User) when is_list(User) ->
    case riak_moss_utils:get_user(User, Riak) of
        {ok, #moss_user{buckets=Buckets}} ->
            {ok, [ {B, sum_bucket(Riak, B)}
                   || #moss_bucket{name=B} <- Buckets ]};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Sum the number of bytes stored in active files in the named
%% bucket.  This assumes that the bucket exists; there will be no
%% difference in output between a non-existent bucket and an empty
%% one.  TODO: this could be a simple MapReduce query.
-spec sum_bucket(pid(), string()) -> integer() | {error, term()}.
sum_bucket(Riak, Bucket) when is_list(Bucket) ->
    sum_bucket(Riak, list_to_binary(Bucket));
sum_bucket(Riak, Bucket) when is_binary(Bucket) ->
    FullBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case riakc_pb_socket:list_keys(Riak, FullBucket) of
        {ok, Keys} ->
            lists:sum([ object_size(Riak, FullBucket, K) || K <- Keys ]);
        {error, Error} ->
            {error, Error}
    end.

%% @doc Retrieve the size of an active named file.  The returned size
%% is 0 if the file is not active.
-spec object_size(pid(), string(), string()) -> integer()
                                              | {error, term()}.
object_size(Riak, Bucket, Key) ->
    case riak_moss_utils:get_object(Bucket, Key, Riak) of
        {ok, Object} ->
            %% TODO: use Reid's sibling resolution code to choose
            %% correct value
            Manifest = binary_to_term(hd(riakc_obj:get_values(Object))),
            case riak_moss_lfs_utils:is_active(Manifest) of
                true ->
                    riak_moss_lfs_utils:content_length(Manifest);
                false ->
                    0
            end;
        {error, Error} ->
            {error, Error}
    end.
