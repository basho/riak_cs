%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
         get_usage/4,
         archive_period/0
        ]).

-export([
         object_size_map/3,
         object_size_reduce/2
        ]).

-ifdef(TEST).
-compile(export_all).
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
    case riak_cs_user:get_user(User, RcPid) of
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
-spec maybe_sum_bucket(string(), cs_bucket(), boolean(), erlang:timestamp()) ->
                              {binary(), [{binary(), integer()}]} |
                              {binary(), binary()}.
maybe_sum_bucket(User, ?RCS_BUCKET{name=Name} = Bucket, Detailed, LeewayEdge)
  when is_list(Name) ->
    maybe_sum_bucket(User, Bucket?RCS_BUCKET{name=list_to_binary(Name)},
                     Detailed, LeewayEdge);
maybe_sum_bucket(User, ?RCS_BUCKET{name=Name} = _Bucket, Detailed, LeewayEdge)
  when is_binary(Name) ->
    case sum_bucket(Name, Detailed, LeewayEdge) of
        {struct, _} = BucketUsage -> {Name, BucketUsage};
        {error, _} = E ->
            _ = lager:error("failed to calculate usage of "
                            "bucket '~s' of user '~s'. Reason: ~p",
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
-spec sum_bucket(binary(), boolean(), erlang:timestamp()) ->
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
        case riakc_pb_socket:mapred(ManifestPbc, Input, Query, Timeout) of
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
    {1, [Summary]} = lists:keyfind(1, 1, MRRes),
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

get_usage(Riak, User, Start, End) ->
    {ok, Period} = archive_period(),
    rts:find_samples(Riak, ?STORAGE_BUCKET, User, Start, End, Period).
