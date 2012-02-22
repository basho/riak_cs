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
         sum_bucket/2,
         make_object/4,
         get_usage/4
        ]).
-export([
         object_size_map/3,
         object_size_reduce/2
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
        {ok, {?MOSS_USER{buckets=Buckets}, _VClock}} ->
            {ok, [ {B, sum_bucket(Riak, B)}
                   || ?MOSS_BUCKET{name=B} <- Buckets ]};
        {ok, #moss_user{buckets=Buckets}} ->
            %% TODO: does this old record match need to remain?
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
    Query = [{map, {modfun, riak_moss_storage, object_size_map},
              none, false},
             {reduce, {modfun, riak_moss_storage, object_size_reduce},
              none, true}],
    case riakc_pb_socket:mapred(Riak, FullBucket, Query) of
        {ok, Results} ->
            {1, [Sum]} = lists:keyfind(1, 1, Results),
            Sum;
        {error, Error} ->
            {error, Error}
    end.

object_size_map({error, notfound}, _, _) ->
    0;
object_size_map(Object, _, _) ->
    %% TODO: use Reid's sibling resolution code to choose
    %% correct value
    Manifest = binary_to_term(hd(riak_object:get_values(Object))),
    case riak_moss_lfs_utils:is_active(Manifest) of
        true ->
            [riak_moss_lfs_utils:content_length(Manifest)];
        false ->
            [0]
    end.

object_size_reduce(Sizes, _) ->
    [lists:sum(Sizes)].

%% @doc Retreive the number of seconds that should elapse between
%% archivings of storage stats.  This setting is controlled by the
%% `storage_archive_period' environment variable of the `riak_moss'
%% application.
-spec archive_period() -> {ok, integer()}|{error, term()}.
archive_period() ->
    case application:get_env(riak_moss, storage_archive_period) of
        {ok, AP} when is_integer(AP), AP > 0 ->
            {ok, AP};
        _ ->
            {error, "riak_moss:storage_archive_period was not an integer"}
    end.

make_object(User, BucketList, SampleStart, SampleEnd) ->
    {ok, Period} = archive_period(),
    Aggregate = aggregate_bucketlist(BucketList),
    rts:new_sample(?STORAGE_BUCKET, User, SampleStart, SampleEnd, Period,
                   Aggregate).

aggregate_bucketlist(BucketList) ->
    {BucketCount, TotalBytes} =
        lists:foldl(fun({_Name, Size}, {C, B}) ->
                            {C+1, B+Size}
                    end,
                    {0, 0},
                    BucketList),
    [{buckets, BucketCount},
     {bytes, TotalBytes}].

get_usage(Riak, User, Start, End) ->
    {ok, Period} = archive_period(),
    rts:find_samples(Riak, ?STORAGE_BUCKET, User, Start, End, Period).
