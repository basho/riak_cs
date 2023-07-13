%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

%% @doc Tools for summing the storage a user has filled.

-module(riak_cs_storage).

-include("riak_cs.hrl").

-export([
         sum_user/4,
         sum_bucket/3,
         make_object/4,
         get_usage/5,
         archive_period/0
        ]).

-export([
         object_size_map/3,
         object_size_reduce/2
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Sum the number of bytes stored in active files in all of the
%% given user's directories.  The result is a list of pairs of
%% `{BucketName, Bytes}'.
-spec sum_user(riak_client(), string(), boolean(), erlang:timestamp()) ->
                      {ok, [{string(), integer()}]}
                          | {error, term()}.
sum_user(RcPid, User, Detailed, LeewayEdge) when is_binary(User) ->
    sum_user(RcPid, binary_to_list(User), Detailed, LeewayEdge);
sum_user(RcPid, User, Detailed, LeewayEdge) when is_list(User) ->
    case riak_cs_iam:find_user(#{key_id => User}, RcPid) of
        {ok, {UserRecord, _UserObj}} ->
            Buckets = riak_cs_bucket:get_buckets(UserRecord),
            BucketUsages = [maybe_sum_bucket(User, B, Detailed, LeewayEdge) ||
                               B <- Buckets],
            {ok, BucketUsages};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Output a log when calculating total usage of a bucket.
%%      This log is *very* important because unless this log
%%      there are no other way for operator to know a calculation
%%      which riak_cs_storage_d failed.
maybe_sum_bucket(User, ?RCS_BUCKET{name = Name}, Detailed, LeewayEdge) ->
    case sum_bucket(Name, Detailed, LeewayEdge) of
        {struct, _} = BucketUsage -> {Name, BucketUsage};
        {error, _} = E ->
            logger:error("failed to calculate usage of bucket \"~s\" of user \"~s\". Reason: ~p",
                         [Name, User, E]),
            {Name, iolist_to_binary(io_lib:format("~p", [E]))}
    end.

%% @doc Calculate summary for the bucket.
%%
%% Basic calculation includes the count and the number of total bytes
%%  for available files in the named bucket. Detailed one adds some
%%  more summary information for the bucket.  This assumes that the
%%  bucket exists; there will be no difference in output between a
%%  non-existent bucket and an empty one.
%%
%% The result is a mochijson structure with two fields: `Objects',
%% which is the number of objects that were counted in the bucket, and
%% `Bytes', which is the total size of all of those objects.  More
%% fields are included for detailed calculation.
-spec sum_bucket(binary(), boolean(), non_neg_integer()) ->
                        {struct, [{binary(), integer()}]}
                            | {error, term()}.
sum_bucket(BucketName, Detailed, LeewayEdge) ->
    Query = case Detailed of
                false ->
                    [{map, {modfun, riak_cs_storage, object_size_map},
                      [do_prereduce], false},
                     {reduce, {modfun, riak_cs_storage, object_size_reduce},
                      none, true}];
                true ->
                    [{map, {modfun, riak_cs_storage_mr, bucket_summary_map},
                      [do_prereduce, {leeway_edge, LeewayEdge}], false},
                     {reduce, {modfun, riak_cs_storage_mr, bucket_summary_reduce},
                      none, true}]
            end,
    %% We cannot reuse RcPid because different bucket may use different bag.
    %% This is why each sum_bucket/1 call retrieves new client on every bucket.
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    try
        ok = riak_cs_riak_client:set_bucket_name(RcPid, BucketName),
        {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
        ManifestBucket = riak_cs_utils:to_bucket_name(objects, BucketName),
        Input = case riak_cs_config:use_2i_for_storage_calc() of
                    true -> {index, ManifestBucket, <<"$bucket">>,
                             <<0>>, riak_cs_utils:big_end_key()};
                    false -> ManifestBucket
                end,
        Timeout = riak_cs_config:storage_calc_timeout(),
        case riak_cs_pbc:mapred(ManifestPbc, Input, Query, Timeout,
                                [riakc, mapred_storage]) of
            {ok, MRRes} ->
                extract_summary(MRRes, Detailed);
            {error, Error} ->
                {error, Error}
        end
    after
        riak_cs_riak_client:checkin(RcPid)
    end.

object_size_map(Obj, KD, Args) ->
    riak_cs_storage_mr:object_size_map(Obj, KD, Args).

object_size_reduce(Values, Args) ->
    riak_cs_storage_mr:object_size_reduce(Values, Args).

extract_summary(MRRes, false) ->
    {1, [{Objects, Bytes}]} = lists:keyfind(1, 1, MRRes),
    {struct, [{<<"Objects">>, Objects},
              {<<"Bytes">>, Bytes}]};
extract_summary(MRRes, true) ->
    Summary = case lists:keyfind(1, 1, MRRes) of
                  {1, [[]]} -> riak_cs_storage_mr:empty_summary();
                  {1, [NonEmptyValue]} -> NonEmptyValue
              end,
    {struct, detailed_result_json_struct(Summary, [])}.

detailed_result_json_struct([], Acc) ->
    Acc;
detailed_result_json_struct([{{K1, K2}, V} | Rest], Acc) ->
    JsonKey = case {K1, K2} of
                  {user, K2} ->
                      list_to_binary(riak_cs_utils:camel_case(K2));
                  {K1, K2} ->
                      list_to_binary([riak_cs_utils:camel_case(K1),
                                      riak_cs_utils:camel_case(K2)])
              end,
    detailed_result_json_struct(Rest, [{JsonKey, V} | Acc]).

%% @doc Retreive the number of seconds that should elapse between
%% archivings of storage stats.  This setting is controlled by the
%% `storage_archive_period' environment variable of the `riak_cs'
%% application.
-spec archive_period() -> {ok, integer()}|{error, term()}.
archive_period() ->
    case application:get_env(riak_cs, storage_archive_period) of
        {ok, AP} when is_integer(AP), AP > 0 ->
            {ok, AP};
        _ ->
            {error, "riak_cs:storage_archive_period was not an integer"}
    end.

make_object(User, BucketList, SampleStart, SampleEnd) ->
    {ok, Period} = archive_period(),
    rts:new_sample(?STORAGE_BUCKET, User, SampleStart, SampleEnd, Period,
                   BucketList).

-spec get_usage(riak_client(), string(),
                boolean(),
                calendar:datetime(),
                calendar:datetime()) -> {list(), list()}.
get_usage(RcPid, User, AdminAccess, Start, End) ->
    {ok, Period} = archive_period(),
    RtsPuller = riak_cs_riak_client:rts_puller(
                  RcPid, ?STORAGE_BUCKET, User, [riakc, get_storage]),
    {Samples, Errors} = rts:find_samples(RtsPuller, Start, End, Period),
    case AdminAccess of
        true -> {Samples, Errors};
        _ -> {[filter_internal_usage(Sample, []) || Sample <- Samples], Errors}
    end.

filter_internal_usage([], Acc) ->
    lists:reverse(Acc);
filter_internal_usage([{K, _V}=T | Rest], Acc)
  when K =:= <<"StartTime">> orelse K =:= <<"EndTime">> ->
    filter_internal_usage(Rest, [T|Acc]);
filter_internal_usage([{Bucket, {struct, UsageList}} | Rest], Acc) ->
    Objects = lists:keyfind(<<"Objects">>, 1, UsageList),
    Bytes = lists:keyfind(<<"Bytes">>, 1, UsageList),
    filter_internal_usage(Rest, [{Bucket, {struct, [Objects, Bytes]}} | Acc]);
filter_internal_usage([{_Bucket, _ErrorBin}=T | Rest], Acc) ->
    filter_internal_usage(Rest, [T|Acc]).
