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
    NumNodes = 1,
    {UserConfig, {RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(NumNodes),
    %% Set up to grep logs to verify messages
    rt:setup_log_capture(hd(CSNodes)),

    rtcs:gc(1, "set-interval infinity"),
    rtcs:gc(1, "set-leeway 1"),
    rtcs:gc(1, "cancel"),

    lager:info("Test GC run under an invalid state manifest..."),
    {GCKey, {BKey, UUID}} = setup_obj(RiakNodes, UserConfig),
    %% Ensure the leeway has expired
    timer:sleep(2000),
    ok = verify_gc_run(hd(CSNodes), GCKey),
    ok = verify_riak_object_remaining_for_bad_key(RiakNodes, GCKey, {BKey, UUID}),

    lager:info("Test repair script (repair_gc_bucket.erl) with more invlaid states..."),
    ok = put_more_bad_keys(RiakNodes, UserConfig),
    %% Ensure the leeway has expired
    timer:sleep(2000),
    RiakIDs = rtcs:riak_id_per_cluster(NumNodes),
    [repair_gc_bucket(ID) || ID <- RiakIDs],
    ok = verify_gc_run2(hd(CSNodes)),

    %% Determinisitc GC test

    %% Create keys not to be deleted
    setup_normal_obj([{"spam", 42}, {"ham", 65536}, {"egg", 7}], UserConfig),
    timer:sleep(1000), %% Next timestamp...

    %% Create keys to be deleted
    Start = os:timestamp(),
    [begin
         setup_normal_obj([{"hop", 42}, {"step", 65536}, {"jump", 7}], UserConfig),
         timer:sleep(2000)
     end || _ <- lists:seq(0,3) ],
    End = os:timestamp(),

    timer:sleep(1000), %% Next timestamp...
    setup_normal_obj([{"spam", 42}, {"ham", 65536}, {"egg", 7}], UserConfig),

    verify_partial_gc_run(hd(CSNodes), RiakNodes, Start, End),
    pass.

setup_normal_obj(ObjSpecs, UserConfig) ->
    %% Put and delete some objects
    [begin
         Block = crypto:rand_bytes(Size),
         Key = ?TEST_KEY ++ Suffix,
         erlcloud_s3:put_object(?TEST_BUCKET, Key, Block, UserConfig),
         erlcloud_s3:delete_object(?TEST_BUCKET, Key, UserConfig)
     end || {Suffix, Size} <- ObjSpecs].

setup_obj(RiakNodes, UserConfig) ->
    %% Setup bucket
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    setup_normal_obj([{"1", 100}, {"2", 200}, {"3", 0}], UserConfig),

    %% Put and delete, but modified to pretend it is in wrong state
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?TEST_KEY_BAD_STATE, SingleBlock, UserConfig),
    erlcloud_s3:delete_object(?TEST_BUCKET, ?TEST_KEY_BAD_STATE, UserConfig),
    %% Change the state in the manifest in gc bucket to active.
    %% See https://github.com/basho/riak_cs/issues/827#issuecomment-54567839
    GCPbc = rtcs:pbc(RiakNodes, objects, ?TEST_BUCKET),
    {ok, GCKeys} = riakc_pb_socket:list_keys(GCPbc, ?GC_BUCKET),
    BKey = {list_to_binary(?TEST_BUCKET), list_to_binary(?TEST_KEY_BAD_STATE)},
    lager:info("Changing state to active ~p, ~p", [?TEST_BUCKET, ?TEST_KEY_BAD_STATE]),
    {ok, GCKey, UUID} = change_state_to_active(GCPbc, BKey, GCKeys),

    %% Put and delete some more objects
    setup_normal_obj([{"Z", 0}, {"Y", 150}, {"X", 1}], UserConfig),

    riakc_pb_socket:stop(GCPbc),
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
            NewManifestSet =
                lists:foldl(fun twop_set:add_element/2, twop_set:new(),
                            [{TargetUUID,
                              TargetManifest?MANIFEST{
                                               state = active,
                                               delete_marked_time=undefined,
                                               delete_blocks_remaining=undefined}} |
                             lists:keydelete(TargetUUID, 1, Manifests)]),
            UpdObj = riakc_obj:update_value(Obj0, term_to_binary(NewManifestSet)),
            ok = riakc_pb_socket:put(Pbc, UpdObj),
            lager:info("Bad state manifests have been put at ~p: ~p~n",
                       [GCKey, twop_set:to_list(NewManifestSet)]),
            {ok, GCKey, TargetUUID}
    end.

put_more_bad_keys(RiakNodes, UserConfig) ->
    %% Put and delete some objects
    [begin
         Block = crypto:rand_bytes(10),
         Key = ?TEST_KEY ++ integer_to_list(Suffix),
         erlcloud_s3:put_object(?TEST_BUCKET, Key, Block, UserConfig),
         erlcloud_s3:delete_object(?TEST_BUCKET, Key, UserConfig)
     end || Suffix <- lists:seq(100, 199)],
    GCPbc = rtcs:pbc(RiakNodes, objects, ?TEST_BUCKET),
    {ok, GCKeys} = riakc_pb_socket:list_keys(GCPbc, ?GC_BUCKET),
    BadGCKeys = put_more_bad_keys(GCPbc, GCKeys, []),
    lager:info("Bad state manifests have been put at ~p", [BadGCKeys]),
    ok.

put_more_bad_keys(_Pbc, [], BadGCKeys) ->
    BadGCKeys;
put_more_bad_keys(Pbc, [GCKey|Rest], BadGCKeys) ->
    case riakc_pb_socket:get(Pbc, ?GC_BUCKET, GCKey) of
        {error, notfound} ->
            put_more_bad_keys(Pbc, Rest, BadGCKeys);
        {ok, Obj0} ->
            Manifests = twop_set:to_list(binary_to_term(riakc_obj:get_value(Obj0))),
            NewManifests = [{UUID, M?MANIFEST{state = active,
                                              delete_marked_time=undefined,
                                              delete_blocks_remaining=undefined}} ||
                               {UUID, M} <- Manifests],
            NewManifestSet =
                lists:foldl(fun twop_set:add_element/2, twop_set:new(), NewManifests),
            UpdObj = riakc_obj:update_value(Obj0, term_to_binary(NewManifestSet)),
            ok = riakc_pb_socket:put(Pbc, UpdObj),
            put_more_bad_keys(Pbc, Rest, [GCKey | BadGCKeys])
    end.

repair_gc_bucket(RiakNodeID) ->
    PbPort = integer_to_list(rtcs:pb_port(RiakNodeID)),
    Res = rtcs:repair_gc_bucket(1, "--host 127.0.0.1 --port " ++ PbPort ++
                                " --leeway-seconds 1 --page-size 5 --debug"),
    Lines = binary:split(list_to_binary(Res), [<<"\n">>], [global]),
    lager:info("Repair script result: ==== BEGIN", []),
    [lager:info("~s", [L]) || L <- Lines],
    lager:info("Repair script result: ==== END", []),
    ok.

verify_gc_run(Node, GCKey) ->
    rtcs:gc(1, "batch 1"),
    lager:info("Check log, warning for invalid state and info for GC finish"),
    true = rt:expect_in_log(Node,
                            "Invalid state manifest in GC bucket at <<\""
                            ++ binary_to_list(GCKey) ++ "\">>, "
                            ++ "bucket=<<\"" ++ ?TEST_BUCKET ++ "\">> "
                            ++ "key=<<\"" ++ ?TEST_KEY_BAD_STATE ++ "\">>: "),
    true = rt:expect_in_log(Node,
                            "Finished garbage collection: \\d+ seconds, "
                            "\\d batch_count, 0 batch_skips, "
                            "7 manif_count, 4 block_count"),
    ok.

verify_gc_run2(Node) ->
    rtcs:gc(1, "batch 1"),
    lager:info("Check collected count =:= 101, 1 from setup_obj, "
               "100 from put_more_bad_keys."),
    true = rt:expect_in_log(Node,
                            "Finished garbage collection: \\d+ seconds, "
                            "\\d+ batch_count, 0 batch_skips, "
                            "101 manif_count, 101 block_count"),
    ok.

%% Verify riak objects in gc buckets, manifest, block are all remaining.
verify_riak_object_remaining_for_bad_key(RiakNodes, GCKey, {{Bucket, Key}, UUID}) ->
    {ok, _BlockObj} = rc_helper:get_riakc_obj(RiakNodes, blocks, Bucket, {Key, UUID, 0}),
    {ok, _ManifestObj} = rc_helper:get_riakc_obj(RiakNodes, objects, Bucket, Key),

    GCPbc = rtcs:pbc(RiakNodes, objects, Bucket),
    {ok, FileSetObj} = riakc_pb_socket:get(GCPbc, ?GC_BUCKET, GCKey),
    Manifests = twop_set:to_list(binary_to_term(riakc_obj:get_value(FileSetObj))),
    {UUID, Manifest} = lists:keyfind(UUID, 1, Manifests),
    riakc_pb_socket:stop(GCPbc),
    lager:info("As expected, BAD manifest in GC bucket remains,"
               " stand off orphan manfiests/blocks: ~p", [Manifest]),
    ok.

verify_partial_gc_run(CSNode, RiakNodes,
                      {MegaSec0, Sec0, _},
                      {MegaSec1, Sec1, _}) ->
    Start0 = MegaSec0 * 1000000 + Sec0,
    End0 = MegaSec1 * 1000000 + Sec1,
    Interval = erlang:max(1, (End0 - Start0) div 3),
    Starts = [ {Start0 + N * Interval, Start0 + (N+1) * Interval}
               || N <- lists:seq(0, 3) ],
    [begin
         %% We have to clear log as the message 'Finished garbage
         %% col...' has been output many times before, during this
         %% test.
         rtcs:reset_log(CSNode),

         lager:debug("GC: (start, end) = (~p, ~p)", [S0, E0]),
         S = rtcs:iso8601(S0),
         E = rtcs:iso8601(E0),
         BatchCmd = "batch -s " ++ S ++ " -e " ++ E,
         rtcs:gc(1, BatchCmd),

         true = rt:expect_in_log(CSNode,
                                 "Finished garbage collection: \\d+ seconds, "
                                 "\\d+ batch_count, 0 batch_skips, "
                                 "\\d+ manif_count, \\d+ block_count")
     end || {S0, E0} <- Starts],
    lager:info("GC target period: (~p, ~p)", [Start0, End0]),
    %% Reap!
    timer:sleep(3000),
    GCPbc = rtcs:pbc(RiakNodes, objects, ?TEST_BUCKET),
    {ok, Keys} = riakc_pb_socket:list_keys(GCPbc, ?GC_BUCKET),
    lager:debug("Keys: ~p", [Keys]),
    StartKey = list_to_binary(integer_to_list(Start0)),
    EndKey = list_to_binary(integer_to_list(End0)),
    HPF = fun(Key) when EndKey < Key -> true; (_Key) -> false end,
    LPF = fun(Key) when Key < StartKey -> true; (_Key) -> false end,

    lager:debug("Remaining Keys: ~p", [Keys]),
    lager:debug("HPF result: ~p", [lists:filter(HPF, Keys)]),
    lager:debug("LPF result: ~p", [lists:filter(LPF, Keys)]),
    ?assertEqual(3, length(lists:filter(HPF, Keys))),
    ?assertEqual(3, length(lists:filter(LPF, Keys))),
    ?assertEqual([], lists:filter(fun(Key) -> HPF(Key) andalso LPF(Key) end, Keys)),
    ok.
