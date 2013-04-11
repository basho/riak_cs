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

-module(riak_cs_list_objects_fsm_v2).

-include("riak_cs.hrl").
-include("list_objects.hrl").

-compile(export_all).
-export([list_objects/2]).

-spec list_objects(pid(), list_object_request()) -> list_object_response().
list_objects(RiakcPid, Request) ->
    RiakObjects = make_2i_request(RiakcPid, Request),
    Manifests = [riak_cs_utils:manifests_from_riak_object(O) ||
                 O <- RiakObjects],
    Active = map_active_manifests(Manifests),
    Filtered = exclude_marker(Request, Active),
    KeyContent = lists:map(fun riak_cs_list_objects:manifest_to_keycontent/1,
                           Filtered),
    case KeyContent of
        [] ->
            riak_cs_list_objects:new_response(Request, false, [], []);
        _Else ->
            riak_cs_list_objects:new_response(Request, true, [], KeyContent)
    end.

-spec make_2i_request(pid(), list_object_request()) -> [riakc_obj:riakc_obj()].
make_2i_request(RiakcPid, Request=?LOREQ{name=BucketName,
                                         max_keys=MaxKeys}) ->
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, BucketName),
    StartKey = make_start_key(Request),
    EndKey = big_end_key(128),
    Opts = [{return_terms, true}, {max_results, MaxKeys}, {stream, true}],
    {ok, ReqID} = riakc_pb_socket:get_index_range(RiakcPid,
                                                  ManifestBucket,
                                                  <<"$key">>,
                                                  StartKey,
                                                  EndKey,
                                                  Opts),
    receive_objects(ReqID).

-spec receive_objects(term()) -> list().
receive_objects(ReqID) ->
    receive_objects(ReqID, []).

receive_objects(ReqId, Acc) ->
    receive
        {ReqId, {objects, List}} ->
            receive_objects(ReqId, Acc ++ List);
        {ReqId, done} ->
            Acc;
        {ReqId, {error, Reason}} ->
            lager:error("yikes, error ~p", [Reason]),
            throw({list_objects_error, Reason});
        Else ->
            throw({unknown_message, Else})
    end.

-spec make_start_key(list_object_request()) -> binary().
make_start_key(?LOREQ{marker=undefined}) ->
    <<0:8/integer>>;
make_start_key(?LOREQ{marker=Marker}) ->
    Marker.

big_end_key(NumBytes) ->
    MaxByte = <<255:8/integer>>,
    iolist_to_binary([MaxByte || _ <- lists:seq(1, NumBytes)]).

-spec map_active_manifests([orddict:orddict()]) -> list(lfs_manifest()).
map_active_manifests(Manifests) ->
    ActiveTuples = [riak_cs_manifest_utils:active_manifest(M) ||
                    M <- Manifests],
    [A || {ok, A} <- ActiveTuples].

-spec exclude_marker(list_object_request(), list()) -> list().
exclude_marker(?LOREQ{marker=undefined}, Objects) ->
    Objects;
exclude_marker(_Request, []) ->
    [];
exclude_marker(?LOREQ{marker=Marker}, [H | T]=Objects) ->
    case element(2, H?MANIFEST.bkey) == Marker of
        true ->
            T;
        false ->
            Objects
    end.

-spec skip_past_prefix_and_delimiter(binary()) -> binary().
skip_past_prefix_and_delimiter(<<>>) ->
    <<0:8/integer>>;
skip_past_prefix_and_delimiter(Key) ->
    PrefixSize = byte_size(Key) - 1,
    <<Prefix:PrefixSize/binary, LastByte/binary>> = Key,
    NextByte = next_byte(LastByte),
    <<Prefix/binary, NextByte/binary>>.

-spec next_byte(binary()) -> binary().
next_byte(<<Integer:8/integer>>=Byte) when Integer == 255 ->
    Byte;
next_byte(<<Integer:8/integer>>) ->
    <<(Integer+1):8/integer>>.
