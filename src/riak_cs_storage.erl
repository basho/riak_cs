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

%% @doc Tools for summing the storage a user has filled.

-module(riak_cs_storage).

-include("riak_cs.hrl").

-export([
         sum_user/2,
         sum_bucket/1,
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
-spec sum_user(riak_client(), string()) -> {ok, [{string(), integer()}]}
                                 | {error, term()}.
sum_user(RcPid, User) when is_binary(User) ->
    sum_user(RcPid, binary_to_list(User));
sum_user(RcPid, User) when is_list(User) ->
    case riak_cs_user:get_user(User, RcPid) of
        {ok, {UserRecord, _UserObj}} ->
            Buckets = riak_cs_bucket:get_buckets(UserRecord),
            BucketUsages = [maybe_sum_bucket(User, B) || B <- Buckets],
            {ok, BucketUsages};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Output a log when calculating total usage of a bucket.
%%      This log is *very* important because unless this log
%%      there are no other way for operator to know a calculation
%%      which riak_cs_storage_d failed.
-spec maybe_sum_bucket(string(), cs_bucket()) ->
                              {binary(), [{binary(), integer()}]} |
                              {binary(), binary()}.
maybe_sum_bucket(User, ?RCS_BUCKET{name=Name} = Bucket) when is_list(Name) ->
    maybe_sum_bucket(User, Bucket?RCS_BUCKET{name=list_to_binary(Name)});
maybe_sum_bucket(User, ?RCS_BUCKET{name=Name} = _Bucket) when is_binary(Name) ->
    case sum_bucket(Name) of
        {struct, _} = BucketUsage -> {Name, BucketUsage};
        {error, _} = E ->
            _ = lager:error("failed to calculate usage of "
                            "bucket '~s' of user '~s'. Reason: ~p",
                            [Name, User, E]),
            {Name, iolist_to_binary(io_lib:format("~p", [E]))}
    end.

%% @doc Sum the number of bytes stored in active files in the named
%% bucket.  This assumes that the bucket exists; there will be no
%% difference in output between a non-existent bucket and an empty
%% one.
%%
%% The result is a mochijson structure with two fields: `Objects',
%% which is the number of objects that were counted in the bucket, and
%% `Bytes', which is the total size of all of those objects.
-spec sum_bucket(binary()) -> {struct, [{binary(), integer()}]}
                                   | {error, term()}.
sum_bucket(BucketName) ->
    Query = [{map, {modfun, riak_cs_storage, object_size_map},
              [do_prereduce], false},
             {reduce, {modfun, riak_cs_storage, object_size_reduce},
              none, true}],
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
            {ok, Results} ->
                {1, [{Objects, Bytes}]} = lists:keyfind(1, 1, Results),
                {struct, [{<<"Objects">>, Objects},
                          {<<"Bytes">>, Bytes}]};
            {error, Error} ->
                {error, Error}
        end
    after
        riak_cs_riak_client:checkin(RcPid)
    end.

object_size_map({error, notfound}, _, _) ->
    [];
object_size_map(Object, _, _) ->
    Handler = fun(Resolved) -> object_size(Resolved) end,
    riak_cs_utils:maybe_process_resolved(Object, Handler, []).

object_size(Resolved) ->
    {MPparts, MPbytes} = count_multipart_parts(Resolved),
    case riak_cs_manifest_utils:active_manifest(Resolved) of
        {ok, ?MANIFEST{content_length=Length}} ->
            [{1 + MPparts, Length + MPbytes}];
        _ ->
            [{MPparts, MPbytes}]
    end.

object_size_reduce(Sizes, _) ->
    {Objects,Bytes} = lists:unzip(Sizes),
    [{lists:sum(Objects),lists:sum(Bytes)}].

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

-spec count_multipart_parts([{cs_uuid(), lfs_manifest()}]) ->
                                   {non_neg_integer(), non_neg_integer()}.
count_multipart_parts(Resolved) ->
    lists:foldl(fun count_multipart_parts/2, {0, 0}, Resolved).

-spec count_multipart_parts({cs_uuid(), lfs_manifest()},
                            {non_neg_integer(), non_neg_integer()}) ->
                                   {non_neg_integer(), non_neg_integer()}.
count_multipart_parts({_UUID, ?MANIFEST{props=Props, state=writing} = M},
                      {MPparts, MPbytes} = Acc)
  when is_list(Props) ->
    case proplists:get_value(multipart, Props) of
        ?MULTIPART_MANIFEST{parts=Ps} = _  ->
            {MPparts + length(Ps),
             MPbytes + lists:sum([P?PART_MANIFEST.content_length ||
                                     P <- Ps])};
        undefined ->
            %% Maybe not a multipart
            Acc;
        Other ->
            %% strange thing happened
            _ = lager:log(warning, self(),
                          "strange writing multipart manifest detected at ~p: ~p",
                          [M?MANIFEST.bkey, Other]),
            Acc
    end;
count_multipart_parts(_, Acc) ->
    %% Other state than writing, won't be counted
    %% active manifests will be counted later
    Acc.

-ifdef(TEST).


object_size_map_test_() ->
    M0 = ?MANIFEST{state=active, content_length=25},
    M1 = ?MANIFEST{state=active, content_length=35},
    M2 = ?MANIFEST{state=writing, props=undefined, content_length=42},
    M3 = ?MANIFEST{state=writing, props=pocketburger, content_length=234},
    M4 = ?MANIFEST{state=writing, props=[{multipart,undefined}],
                   content_length=23434},
    M5 = ?MANIFEST{state=writing, props=[{multipart,pocketburger}],
                   content_length=23434},

    [?_assertEqual([{1,25}], object_size([{uuid,M0}])),
     ?_assertEqual([{1,35}], object_size([{uuid2,M2},{uuid1,M1}])),
     ?_assertEqual([{1,35}], object_size([{uuid2,M3},{uuid1,M1}])),
     ?_assertEqual([{1,35}], object_size([{uuid2,M4},{uuid1,M1}])),
     ?_assertEqual([{1,35}], object_size([{uuid2,M5},{uuid1,M1}]))].

count_multipart_parts_test_() ->
    ZeroZero = {0, 0},
    ValidMPManifest = ?MULTIPART_MANIFEST{parts=[?PART_MANIFEST{content_length=10}]},
    [?_assertEqual(ZeroZero,
                   count_multipart_parts({<<"pocketburgers">>,
                                          ?MANIFEST{props=pocketburgers, state=writing}},
                                         ZeroZero)),
     ?_assertEqual(ZeroZero,
                   count_multipart_parts({<<"pocketburgers">>,
                                          ?MANIFEST{props=pocketburgers, state=iamyourfather}},
                                         ZeroZero)),
     ?_assertEqual(ZeroZero,
                   count_multipart_parts({<<"pocketburgers">>,
                                          ?MANIFEST{props=[], state=writing}},
                                         ZeroZero)),
     ?_assertEqual(ZeroZero,
                   count_multipart_parts({<<"pocketburgers">>,
                                          ?MANIFEST{props=[{multipart, pocketburger}], state=writing}},
                                         ZeroZero)),
     ?_assertEqual({1, 10},
                   count_multipart_parts({<<"pocketburgers">>,
                                          ?MANIFEST{props=[{multipart, ValidMPManifest}], state=writing}},
                                         ZeroZero))
    ].

-endif.
