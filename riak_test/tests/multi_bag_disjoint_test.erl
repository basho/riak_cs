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

-module(multi_bag_disjoint_test).

%% @doc `riak_test' module for testing multi bag disjoint configuration

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
%% -include_lib("riak_cs/include/riak_cs.hrl").
-include("riak_cs.hrl").

-define(TEST_BUCKET,   "riak-test-bucket").
-define(KEY_NORMAL,    "key_normal").
-define(KEY_MULTIPART, "key_multipart").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup1x1x1(config()),
    bag_input(),
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    assert_object_in_each_bag(RiakNodes, UserConfig, normal),
    assert_object_in_each_bag(RiakNodes, UserConfig, multipart),
    pass.

config() ->
    CustomConfig = 
        [{multi_bag,
          [
           {"bag-A", "127.0.0.1", 10017},
           {"bag-B", "127.0.0.1", 10027},
           {"bag-C", "127.0.0.1", 10037}
          ]},
         {default_bag, [{manifest, "bag-A"},
                        {block,    "bag-A"}]}],
    [{cs, rtcs:cs_config(CustomConfig)}].

weights() ->
    [
     {<<"manifest">>, [
                       [{<<"id">>, <<"bag-B">>}, {<<"weight">>, 100}]
                      ]},
     {<<"block">>, [
                    [{<<"id">>, <<"bag-C">>}, {<<"weight">>, 100}]
                   ]}
    ].

bag_input() ->
    InputRes = rtcs:bag_input(1, mochijson2:encode(weights())),
    lager:info("riak-cs-mc input result: ~s", [InputRes]).

assert_object_in_each_bag(RiakNodes, UserConfig, UploadType) ->
    {Bucket, Key, Content} = upload(UserConfig, UploadType),
    assert_whole_content(Bucket, Key, Content, UserConfig),
    [MasterBag, BagB, BagC] = RiakNodes,
    {UUID, M} = assert_manifest_in_single_bag([BagB], [MasterBag, BagC], Bucket, Key),
    ok = assert_block_in_single_bag([BagC], [MasterBag, BagB],
                                    Bucket, UploadType, UUID, M),
    ok.

upload(UserConfig, normal) ->
    Content = crypto:rand_bytes(mb(4)),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_NORMAL, Content, UserConfig),
    {?TEST_BUCKET, ?KEY_NORMAL, Content};
upload(UserConfig, multipart) ->
    Content = rtcs_multipart:multipart_upload(?TEST_BUCKET, ?KEY_MULTIPART,
                                              [mb(10), mb(5), mb(9) + 123, mb(6), 400],
                                              UserConfig),
    {?TEST_BUCKET, ?KEY_MULTIPART, Content}.

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

assert_whole_content(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).

assert_manifest_in_single_bag(ExpectedBags, NotExistingBags, Bucket, Key) ->
    RiakBucket = <<"0o:", (stanchion_utils:md5(Bucket))/binary>>,
    Object = assert_only_in_single_bag(ExpectedBags, NotExistingBags, RiakBucket, Key),
    [[{UUID, M}]] = [binary_to_term(V) || V <- riakc_obj:get_values(Object)],
    {UUID, M}.

assert_block_in_single_bag(ExpectedBags, NotExistingBags, Bucket, UploadType, UUID, Manifest) ->
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
    _Object = assert_only_in_single_bag(ExpectedBags, NotExistingBags,
                                        RiakBucket, RiakKey),
    ok.


-spec assert_only_in_single_bag(ExpectedBags::[binary()], NotExistingBags::[binary()],
                                RiakBucket::binary(), RiakKey::binary()) ->
                                       riakc_obj:riakc_obj().
%% Assert BKey
%% - exists onc and only one bag in ExpectedBags and
%% - does not exists in NotExistingBags.
%% Also returns a riak object which is found in ExpectedBags.
assert_only_in_single_bag(ExpectedBags, NotExistingBags, RiakBucket, RiakKey) ->
    Obj = assert_in_expected_bags(ExpectedBags, RiakBucket, RiakKey, []),
    assert_not_in_other_bags(NotExistingBags, RiakBucket, RiakKey),
    Obj.

assert_in_expected_bags([], _RiakBucket, _RiakKey, []) ->
    throw(not_found_in_expected_bags);
assert_in_expected_bags([], _RiakBucket, _RiakKey, [Val]) ->
    Val;
assert_in_expected_bags([ExpectedBag | Rest], RiakBucket, RiakKey, Acc) ->
    case get_riakc_obj(ExpectedBag, RiakBucket, RiakKey) of
        {ok, Object} ->
            assert_in_expected_bags(Rest, RiakBucket, RiakKey, [Object|Acc]);
        {error, notfound} ->
            assert_in_expected_bags(Rest, RiakBucket, RiakKey, Acc)
    end.

assert_not_in_other_bags([], _RiakBucket, _RiakKey) ->
    ok;
assert_not_in_other_bags([NotExistingBag | Rest], RiakBucket, RiakKey) ->
    {error, notfound} = get_riakc_obj(NotExistingBag, RiakBucket, RiakKey),
    assert_not_in_other_bags(Rest, RiakBucket, RiakKey).

get_riakc_obj(Bag, B, K) ->
    Riakc = rt:pbc(Bag),
    Result = riakc_pb_socket:get(Riakc, B, K),
    riakc_pb_socket:stop(Riakc),
    Result.
