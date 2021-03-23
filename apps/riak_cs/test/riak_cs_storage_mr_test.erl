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

%% @doc Test for storage MR functions

-module(riak_cs_storage_mr_test).

-compile(export_all).
-compile(nowarn_export_all).

-include("riak_cs.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(M, (m())?MANIFEST).
-define(PM, (pm())?PART_MANIFEST).

bucket_summary_map_test_() ->
    LeewayEdge = {1400, 0, 0},
    OBB = fun(K, Ms) ->
                  History = [{M?MANIFEST.uuid, M} || M <- Ms],
                  [Res] = riak_cs_storage_mr:bucket_summary_map(
                            riak_object:new(History),
                            kd, [do_prereduce, {leeway_edge, LeewayEdge}]),
                  {proplists:get_value({K, objects}, Res),
                   proplists:get_value({K, bytes}, Res),
                   proplists:get_value({K, blocks}, Res)} end,
    MostActive = ?M{state=active, content_length=mb(10)},
    LessActive = ?M{state=active, content_length=mb(20),
                    write_start_time={1000, 0, 0}},
    NewWriting   = ?M{state=writing, content_length=mb(10),
                      write_start_time={1500, 0, 0}},
    OldWriting   = ?M{state=writing, content_length=mb(10),
                      write_start_time={1300, 0, 0}},
    NewPd   = ?M{state=pending_delete, content_length=mb(10),
                 delete_marked_time={1500, 0, 0}},
    OldPd   = ?M{state=pending_delete, content_length=mb(10),
                 delete_marked_time={1300, 0, 0}},
    NewSd = NewPd?MANIFEST{state=scheduled_delete},
    OldSd = OldPd?MANIFEST{state=scheduled_delete},
    MpM = ?MULTIPART_MANIFEST{parts=[?PM{content_length=mb(30)},
                                     ?PM{content_length=mb(30)},
                                     ?PM{content_length=40}]},
    MostActiveMP = ?M{state=active, content_length=mb(60)+40, props=[{multipart, MpM}]},
    LessActiveMP = ?M{state=active, content_length=mb(60)+40,
                     write_start_time={1000, 0, 0},
                     props=[{multipart, MpM}]},
    NewWritingMP = ?M{state=writing, write_start_time={1500, 0, 0},
                      props=[{multipart, MpM}]},
    OldWritingMP = ?M{state=writing, write_start_time={1300, 0, 0},
                      props=[{multipart, MpM}]},
    NewPdMP = ?M{state=pending_delete, delete_marked_time={1500, 0, 0},
                 props=[{multipart, MpM}]},
    OldPdMP = ?M{state=pending_delete, delete_marked_time={1300, 0, 0},
                 props=[{multipart, MpM}]},
    NewSdMP = NewPdMP?MANIFEST{state=scheduled_delete},
    OldSdMP = OldPdMP?MANIFEST{state=scheduled_delete},
    Combination = [MostActive, LessActiveMP, NewWriting, OldWriting,
                   OldWritingMP, NewPd, OldPdMP, NewPd, OldPdMP,
                   NewSd, OldSd, NewSdMP, OldSdMP],
    [
     %% Empty
     ?_assertEqual([],
                   [{K, V} || {K, V} <- riak_cs_storage_mr:bucket_summary_map(
                                          riak_object:new([]),
                                          kd, [do_prereduce, {leeway_edge, LeewayEdge}]),
                              V =/= 0]),

     %% Active only
     ?_assertEqual({1, mb(10),    10}, OBB(user,   [MostActive])),
     ?_assertEqual({1, mb(10),    10}, OBB(active, [MostActive])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(user,   [MostActiveMP])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(active, [MostActiveMP])),
     %% Old active
     ?_assertEqual({1, mb(10),    10}, OBB(user,             [MostActive, LessActive])),
     ?_assertEqual({1, mb(10),    10}, OBB(active,           [MostActive, LessActive])),
     ?_assertEqual({1, mb(20),    20}, OBB(active_invisible, [MostActive, LessActive])),
     ?_assertEqual({1, mb(10),    10}, OBB(user,             [MostActive, LessActiveMP])),
     ?_assertEqual({1, mb(10),    10}, OBB(active,           [MostActive, LessActiveMP])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(active_invisible, [MostActive, LessActiveMP])),

     %% Writing MP
     ?_assertEqual({3, mb( 60)+40,  61}, OBB(writing_multipart, [NewWritingMP])),
     ?_assertEqual({3, mb( 60)+40,  61}, OBB(user,              [NewWritingMP])),
     ?_assertEqual({3, mb( 60)+40,  61}, OBB(writing_multipart, [OldWritingMP])),
     ?_assertEqual({3, mb( 60)+40,  61}, OBB(user,              [OldWritingMP])),
     ?_assertEqual({6, mb(120)+80, 122}, OBB(writing_multipart, [NewWritingMP, OldWritingMP])),
     ?_assertEqual({6, mb(120)+80, 122}, OBB(user,              [NewWritingMP, OldWritingMP])),
     ?_assertEqual({0,          0,   0}, OBB(writing_new,       [NewWritingMP, OldWritingMP])),
     ?_assertEqual({0,          0,   0}, OBB(writing_old,       [NewWritingMP, OldWritingMP])),

     %% Writing non-MP
     ?_assertEqual({1, mb(10),    10}, OBB(writing_new, [NewWriting])),
     ?_assertEqual({1, mb(10),    10}, OBB(writing_old, [OldWriting])),
     ?_assertEqual({1, mb(10),    10}, OBB(writing_new, [NewWriting, OldWriting])),
     ?_assertEqual({1, mb(10),    10}, OBB(writing_old, [NewWriting, OldWriting])),
     %% PD
     ?_assertEqual({1, mb(10),    10}, OBB(pending_delete_new, [NewPd])),
     ?_assertEqual({1, mb(10),    10}, OBB(pending_delete_old, [OldPd])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(pending_delete_new, [NewPdMP])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(pending_delete_old, [OldPdMP])),
     %% SD
     ?_assertEqual({1, mb(10),    10}, OBB(scheduled_delete_new, [NewSd])),
     ?_assertEqual({1, mb(10),    10}, OBB(scheduled_delete_old, [OldSd])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(scheduled_delete_new, [NewSdMP])),
     ?_assertEqual({1, mb(60)+40, 61}, OBB(scheduled_delete_old, [OldSdMP])),

     %% Combination
     ?_assertEqual({4, mb(70)+40,   71}, OBB(user,                Combination)),
     ?_assertEqual({1, mb(60)+40,   61}, OBB(active_invisible,    Combination)),
     ?_assertEqual({3, mb(60)+40,   61}, OBB(writing_multipart,   Combination)),
     ?_assertEqual({1, mb(10),      10}, OBB(writing_new,         Combination)),
     ?_assertEqual({2, mb(20),      20}, OBB(pending_delete_new,  Combination)),
     ?_assertEqual({2, mb(120)+80, 122}, OBB(pending_delete_old,  Combination)),
     ?_assertEqual({2, mb(70)+40,   71}, OBB(scheduled_delete_new, Combination)),
     ?_assertEqual({2, mb(70)+40,   71}, OBB(scheduled_delete_old, Combination))
    ].

m() ->
    ?MANIFEST{uuid=uuid:get_v4(),
              content_length=0,
              write_start_time={1200, 0, 0},
              block_size=riak_cs_lfs_utils:block_size()}.

pm() ->
    ?PART_MANIFEST{block_size=riak_cs_lfs_utils:block_size()}.

mb(MB) -> MB * 1024 * 1024.

bucket_summary_reduce_test() ->
    ?assertEqual([[]],
                 riak_cs_storage_mr:bucket_summary_reduce([[]], args)),
    ?assertEqual([[{a, 1}]],
                 riak_cs_storage_mr:bucket_summary_reduce([[{a, 1}]], args)),
    ?assertEqual([[{a, 1}, {b, 1}]],
                 riak_cs_storage_mr:bucket_summary_reduce([[{a, 1}, {b, 1}]], args)),
    ?assertEqual([[{a, 1}, {b, 1}]],
                 riak_cs_storage_mr:bucket_summary_reduce([[{a, 1}],
                                                           [{b, 1}]], args)),
    ?assertEqual([[{a, 3}]],
                 riak_cs_storage_mr:bucket_summary_reduce([[{a, 1}],
                                                           [{a, 2}]], args)),
    ?assertEqual([[{a, 3}, {b, 6}]],
                 riak_cs_storage_mr:bucket_summary_reduce([[{a, 1}, {b, 1}],
                                                           [{a, 2}, {b, 5}]], args)).

object_size_map_test_() ->
    M0 = ?MANIFEST{state=active, content_length=25},
    M1 = ?MANIFEST{state=active, content_length=35},
    M2 = ?MANIFEST{state=writing, props=undefined, content_length=42},
    M3 = ?MANIFEST{state=writing, props=pocketburger, content_length=234},
    M4 = ?MANIFEST{state=writing, props=[{multipart,undefined}],
                   content_length=23434},
    M5 = ?MANIFEST{state=writing, props=[{multipart,pocketburger}],
                   content_length=23434},

    [?_assertEqual([{1,25}], riak_cs_storage_mr:object_size([{uuid,M0}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M2},{uuid1,M1}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M3},{uuid1,M1}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M4},{uuid1,M1}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M5},{uuid1,M1}]))].

count_multipart_parts_test_() ->
    ZeroZero = {0, 0},
    ValidMPManifest = ?MULTIPART_MANIFEST{parts=[?PART_MANIFEST{content_length=10}]},
    [?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=pocketburgers, state=writing}}])),
     ?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=pocketburgers, state=iamyourfather}}])),
     ?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=[], state=writing}}])),
     ?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=[{multipart, pocketburger}], state=writing}}])),
     ?_assertEqual({1, 10},
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=[{multipart, ValidMPManifest}], state=writing}}]))
    ].
