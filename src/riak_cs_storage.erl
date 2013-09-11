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
         sum_bucket/2,
         make_object/4,
         get_usage/4,
         archive_period/0
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
    case riak_cs_utils:get_user(User, Riak) of
        {ok, {?RCS_USER{buckets=Buckets}, _UserObj}} ->
            {ok, [ {B, sum_bucket(Riak, B)}
                   || ?RCS_BUCKET{name=B} <- Buckets ]};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Sum the number of bytes stored in active files in the named
%% bucket.  This assumes that the bucket exists; there will be no
%% difference in output between a non-existent bucket and an empty
%% one.
%%
%% The result is a mochijson structure with two fields: `Objects',
%% which is the number of objects that were counted in the bucket, and
%% `Bytes', which is the total size of all of those objects.
-spec sum_bucket(pid(), string() | binary()) -> term() | {error, term()}.
sum_bucket(Riak, Bucket) when is_list(Bucket) ->
    sum_bucket(Riak, list_to_binary(Bucket));
sum_bucket(Riak, Bucket) when is_binary(Bucket) ->
    FullBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    Query = [{map, {modfun, riak_cs_storage, object_size_map},
              [do_prereduce], false},
             {reduce, {modfun, riak_cs_storage, object_size_reduce},
              none, true}],
    case riakc_pb_socket:mapred(Riak, FullBucket, Query) of
        {ok, Results} ->
            {1, [{Objects, Bytes}]} = lists:keyfind(1, 1, Results),
            {struct, [{<<"Objects">>, Objects},
                      {<<"Bytes">>, Bytes}]};
        {error, Error} ->
            {error, Error}
    end.

object_size_map({error, notfound}, _, _) ->
    [];
object_size_map(Object, _, _) ->
    try
        AllManifests = [ binary_to_term(V)
                         || V <- riak_object:get_values(Object) ],
        Resolved = riak_cs_manifest_resolution:resolve(AllManifests),
        {MPparts, MPbytes} = count_multipart_parts(Resolved),
        case riak_cs_manifest_utils:active_manifest(Resolved) of
            {ok, ?MANIFEST{content_length=Length}} ->
                [{1 + MPparts, Length + MPbytes}];
            _ ->
                [{MPparts, MPbytes}]
        end
    catch _:_ ->
            []
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

count_multipart_parts(Resolved) ->
    lists:foldl(fun count_multipart_parts/2, {0, 0}, Resolved).

count_multipart_parts({_UUID, M}, {MPparts, MPbytes} = Acc) ->
    case {M?MANIFEST.state, proplists:get_value(multipart, riak_cs_manifest_utils:fix_props(M?MANIFEST.props))} of
        {writing, MP} ->
            Ps = MP?MULTIPART_MANIFEST.parts,
            {MPparts + length(Ps),
             MPbytes + lists:sum([P?PART_MANIFEST.content_length ||
                                     P <- Ps])};
        _ ->
            Acc
    end.
