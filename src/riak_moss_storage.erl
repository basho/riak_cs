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
         make_object/5,
         get_usage/4
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

make_object(Time, User, BucketList, SampleStart, SampleEnd) ->
    Aggregate = aggregate_bucketlist(BucketList),
    MJSON = {struct, [{start_time, riak_moss_access:iso8601(SampleStart)},
                      {end_time, riak_moss_access:iso8601(SampleEnd)}
                      |Aggregate]},
    riakc_obj:new(?STORAGE_BUCKET,
                  sample_key(User, Time),
                  iolist_to_binary(mochijson2:encode(MJSON)),
                  "application/json").

aggregate_bucketlist(BucketList) ->
    {BucketCount, TotalBytes} =
        lists:foldl(fun({_Name, Size}, {C, B}) ->
                            {C+1, B+Size}
                    end,
                    {0, 0},
                    BucketList),
    [{buckets, BucketCount},
     {bytes, TotalBytes}].

sample_key(User, Time) ->
    iolist_to_binary([User,".",riak_moss_access:iso8601(Time)]).

get_usage(User, Start, End, Riak) ->
    Times = times_covering(Start, End),
    UsageAdder = usage_adder(User, Riak),
    lists:foldl(UsageAdder, {[], []}, Times).

usage_adder(User, Riak) ->
    fun(Time, {Usage, Errors}) ->
            case riakc_pb_socket:get(Riak, ?STORAGE_BUCKET,
                                     sample_key(User, Time)) of
                {ok, Object} ->
                    NewUsages = [element(2, {struct, _}=mochijson2:decode(V))
                                 || V <- riakc_obj:get_values(Object)],
                    {NewUsages++Usage, Errors};
                {error, notfound} ->
                    %% this is normal - we ask for all possible
                    %% archives, and just deal with the ones that exist
                    {Usage, Errors};
                {error, Error} ->
                    {Usage, [{Time, Error}|Errors]}
            end
    end.

times_covering(Start, End) ->
    case riak_moss_storage_d:read_storage_schedule() of
        [] ->
            %% no schedule means no idea where to look
            [];
        Schedule ->
            {Day,{SH,SM,_}} = Start,
            case {SH, SM} < hd(Schedule) of
                true ->
                    %% start with last archive of previous day
                    {DayBefore,_} = calendar:gregorian_seconds_to_datetime(
                                      calendar:datetime_to_gregorian_seconds(
                                        Start)-24*60*60),
                    [{H, M}|_] = lists:reverse(Schedule),
                    First = {DayBefore, {H, M, 0}},
                    Rest = Schedule;
                false ->
                    %% start somewhere in day
                    {Early, [{H,M}|Late]} =
                        lists:splitwith(
                          fun({H,M}) ->
                                  H < SH orelse
                                           (H == SH andalso M < SM)
                          end),
                    First = {Day, {H, M, 0}},
                    Rest = Late++Early++[{H,M}]
            end,
            times_covering1(End, Rest, [First])
    end.

times_covering1(End, [{H,M}|_]=Schedule, [{SameDay,_}|_]=Acc) ->
    case {H,M} =< lists:last(Schedule) of
        true ->
            %% new day
            NewTime = calendar:gregorian_seconds_to_datetime(
                        calendar:datetime_to_gregorian_seconds(
                          {SameDay, {H,M,0}})+24*60*60);
        false ->
            %% same day
            NewTime = {SameDay, {H, M, 0}}
    end,
    case NewTime > End of
        true ->
            Acc;
        false ->
            times_covering1(End, tl(Schedule)++[{H,M}], [NewTime|Acc])
    end.
