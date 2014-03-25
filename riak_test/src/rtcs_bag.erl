%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(rtcs_bag).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("riak_cs.hrl").

assert_object_in_expected_bag(Bucket, Key, UploadType,
                              AllBags, ExpectedManifestBags, ExpectedBlockBags) ->
    {UUID, M} = assert_manifest_in_single_bag(Bucket, Key,
                                             ExpectedManifestBags,
                                             AllBags -- ExpectedManifestBags),
    ok = assert_block_in_single_bag(Bucket, {UUID, M}, UploadType,
                                    ExpectedBlockBags, AllBags -- ExpectedBlockBags),
    ok.

assert_manifest_in_single_bag(Bucket, Key, ExpectedBags, NotExistingBags) ->
    RiakBucket = <<"0o:", (stanchion_utils:md5(Bucket))/binary>>,
    case assert_only_in_single_bag(ExpectedBags, NotExistingBags, RiakBucket, Key) of
        {error, Reason} ->
            lager:error("assert_manifest_in_single_bag for ~w/~w error: ~p",
                        [Bucket, Key, Reason]),
            {error, {Bucket, Key, Reason}};
        Object ->
            [[{UUID, M}]] = [binary_to_term(V) || V <- riakc_obj:get_values(Object)],
            {UUID, M}
    end.

assert_block_in_single_bag(Bucket, {UUID, Manifest}, UploadType,
                           ExpectedBags, NotExistingBags) ->
    RiakBucket = <<"0b:", (stanchion_utils:md5(Bucket))/binary>>,
    RiakKey = case UploadType of
                  normal ->
                      <<UUID/binary, 0:32>>;
                  multipart ->
                      %% Take UUID of the first block of the first part manifest
                      MpM = proplists:get_value(multipart, Manifest?MANIFEST.props),
                      PartUUID = (hd(MpM?MULTIPART_MANIFEST.parts))?PART_MANIFEST.part_id,
                      <<PartUUID/binary, 0:32>>
              end,
    case assert_only_in_single_bag(ExpectedBags, NotExistingBags,
                                   RiakBucket, RiakKey) of
        {error, Reason} ->
            lager:error("assert_block_in_single_bag for ~w/~w[~w] error: ~p",
                        [Bucket, UUID, UploadType, Reason]),
            {error, {Bucket, {UploadType, UUID, Manifest}, Reason}};
        _Object ->
            ok
    end.


-spec assert_only_in_single_bag(ExpectedBags::[binary()], NotExistingBags::[binary()],
                                RiakBucket::binary(), RiakKey::binary()) ->
                                       riakc_obj:riakc_obj().
%% Assert BKey
%% - exists onc and only one bag in ExpectedBags and
%% - does not exists in NotExistingBags.
%% Also returns a riak object which is found in ExpectedBags.
assert_only_in_single_bag(ExpectedBags, NotExistingBags, RiakBucket, RiakKey) ->
    case assert_in_expected_bags(ExpectedBags, RiakBucket, RiakKey, []) of
        {error, Reason} ->
            {error, Reason};
        Obj ->
            case assert_not_in_other_bags(NotExistingBags, RiakBucket, RiakKey) of
                {error, Reason2} ->
                    {error, Reason2};
                _ ->
                    Obj
            end
    end.

assert_in_expected_bags([], _RiakBucket, _RiakKey, []) ->
    not_found_in_expected_bags;
assert_in_expected_bags([], _RiakBucket, _RiakKey, [Val]) ->
    Val;
assert_in_expected_bags([ExpectedBag | Rest], RiakBucket, RiakKey, Acc) ->
    case get_riakc_obj(ExpectedBag, RiakBucket, RiakKey) of
        {ok, Object} ->
            lager:info("~p/~p is found at ~s", [RiakBucket, RiakKey, ExpectedBag]),
            assert_in_expected_bags(Rest, RiakBucket, RiakKey, [Object|Acc]);
        {error, notfound} ->
            assert_in_expected_bags(Rest, RiakBucket, RiakKey, Acc)
    end.

assert_not_in_other_bags([], _RiakBucket, _RiakKey) ->
    ok;
assert_not_in_other_bags([NotExistingBag | Rest], RiakBucket, RiakKey) ->
    case get_riakc_obj(NotExistingBag, RiakBucket, RiakKey) of
        {error, notfound} ->
            assert_not_in_other_bags(Rest, RiakBucket, RiakKey);
        Res ->
            lager:info("~p/~p is found at ~s", [RiakBucket, RiakKey, NotExistingBag]),
            {error, {found_in_unexpected_bag, NotExistingBag, Res}}
    end.

get_riakc_obj(Bag, B, K) ->
    Riakc = rt:pbc(Bag),
    Result = riakc_pb_socket:get(Riakc, B, K),
    riakc_pb_socket:stop(Riakc),
    Result.
