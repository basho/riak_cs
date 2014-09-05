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

-module(gc_tests).

%% @doc `riak_test' module for testing garbage collection

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_cs.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET,        "riak-test-bucket").
-define(TEST_KEY,          "riak_test_key").
-define(TEST_KEY_MP,        "riak_test_mp").
-define(TEST_KEY_BAD_STATE, "riak_test_key_bad_state").


confirm() ->
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(1),
    %% Set up to grep logs to verify messages
    rt:setup_log_capture(hd(CSNodes)),

    {GCKey, {BKey, UUID}} = setup_obj(RiakNodes, UserConfig),
    ok = verify_gc_run(hd(CSNodes), GCKey),
    ok = verify_riak_object_remaining_for_bad_key(RiakNodes, GCKey, {BKey, UUID}),
    pass.

setup_obj(RiakNodes, UserConfig) ->
    %% Setup bucket
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    %% Put and delete some objects
    [begin
         Block = crypto:rand_bytes(Size),
         Key = ?TEST_KEY ++ Suffix,
         erlcloud_s3:put_object(?TEST_BUCKET, Key, Block, UserConfig),
         erlcloud_s3:delete_object(?TEST_BUCKET, Key, UserConfig)
     end || {Suffix, Size} <- [{"1", 100}, {"2", 200}, {"3", 0}]],

    %% Put and delete, but modified to pretend it is in wrong state
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?TEST_KEY_BAD_STATE, SingleBlock, UserConfig),
    erlcloud_s3:delete_object(?TEST_BUCKET, ?TEST_KEY_BAD_STATE, UserConfig),
    %% Change the state in the manifest in gc bucket to active.
    %% See https://github.com/basho/riak_cs/issues/827#issuecomment-54567839
    Pbc = rtcs:pbc(RiakNodes, gc_bucket, not_used),
    {ok, GCKeys} = riakc_pb_socket:list_keys(Pbc, ?GC_BUCKET),
    BKey = {list_to_binary(?TEST_BUCKET), list_to_binary(?TEST_KEY_BAD_STATE)},
    lager:info("Changing state to active ~p, ~p", [?TEST_BUCKET, ?TEST_KEY_BAD_STATE]),
    {ok, GCKey, UUID} = change_state_to_active(Pbc, BKey, GCKeys),

    %% Put and delete some more objects
    [begin
         Block = crypto:rand_bytes(Size),
         Key = ?TEST_KEY ++ Suffix,
         erlcloud_s3:put_object(?TEST_BUCKET, Key, Block, UserConfig),
         erlcloud_s3:delete_object(?TEST_BUCKET, Key, UserConfig)
     end || {Suffix, Size} <- [{"Z", 0}, {"Y", 150}, {"X", 1}]],

    riakc_pb_socket:stop(Pbc),
    {GCKey, {BKey, UUID}}.

change_state_to_active(_Pbc, TargetBKey, []) ->
    lager:warning("Target BKey ~p not found in GC bucket", [TargetBKey]),
    {error, notfound};
change_state_to_active(Pbc, TargetBKey, [GCKey|Rest]) ->
    {ok, Obj0} = riakc_pb_socket:get(Pbc, ?GC_BUCKET, GCKey),
    Manifests = twop_set:to_list(binary_to_term(riakc_obj:get_value(Obj0))),
    case [{UUID, M?MANIFEST{state=active}} ||
              {UUID, M} <- Manifests,
              M?MANIFEST.bkey =:= TargetBKey] of
        [] ->
            change_state_to_active(Pbc, TargetBKey, Rest);
        [{TargetUUID, TargetManifest}] ->
            lager:info("Target BKey ~p found in GC bucket ~p", [TargetBKey, GCKey]),
            NewManifestSet = lists:foldl(fun twop_set:add_element/2, twop_set:new(),
                                         [{TargetUUID, TargetManifest?MANIFEST{state = active}} |
                                          lists:keydelete(TargetUUID, 1, Manifests)]),
            UpdObj = riakc_obj:update_value(Obj0, term_to_binary(NewManifestSet)),
            ok = riakc_pb_socket:put(Pbc, UpdObj),
            lager:info("Bad state manifests have been put at ~p: ~p~n",
                       [GCKey, twop_set:to_list(NewManifestSet)]),
            {ok, GCKey, TargetUUID}
    end.

verify_gc_run(Node, GCKey) ->
    rtcs:gc(1, "set-leeway 1"),
    timer:sleep(2000),
    rtcs:gc(1, "batch"),
    true = rt:expect_in_log(Node,
                            "Invalid state manifest in GC bucket at <<\""
                            ++ binary_to_list(GCKey)
                            ++ "\">>: "),
    true = rt:expect_in_log(Node,
                            "Finished garbage collection: \\d+ seconds, "
                            "\\d batch_count, 0 batch_skips, "
                            "7 manif_count, 4 block_count"),
    ok.

%% Verify riak objects in gc buckets, manifest, block are all remaining.
verify_riak_object_remaining_for_bad_key(RiakNodes, GCKey, {{Bucket, Key}, UUID}) ->
    {ok, _BlockObj} = rc_helper:get_riakc_obj(RiakNodes, blocks, Bucket, {UUID, 0}),
    {ok, _ManifestObj} = rc_helper:get_riakc_obj(RiakNodes, objects, Bucket, Key),

    Pbc = rtcs:pbc(RiakNodes, gc_bucket, not_used),
    {ok, FileSetObj} = riakc_pb_socket:get(Pbc, ?GC_BUCKET, GCKey),
    Manifests = twop_set:to_list(binary_to_term(riakc_obj:get_value(FileSetObj))),
    {UUID, Manifest} = lists:keyfind(UUID, 1, Manifests),
    riakc_pb_socket:stop(Pbc),
    lager:info("BAD manifest in GC bucket remains, stand off orphan manfiests/blocks: ~p",
               [Manifest]),
    ok.
