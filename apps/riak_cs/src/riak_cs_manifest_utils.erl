%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc Module for choosing and manipulating lists (well, orddict) of manifests

-module(riak_cs_manifest_utils).

-include("riak_cs.hrl").
-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([overwritten_UUIDs/1,
         manifests_to_gc/2,
         prune/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Extract all manifests that are not "the most active"
%%      and not actively writing (within the leeway period).
-spec overwritten_UUIDs(orddict:orddict()) -> term().
overwritten_UUIDs(Dict) ->
    case rcs_common_manifest_utils:active_manifest(Dict) of
        {error, no_active_manifest} ->
            [];
        {ok, Active} ->
            lists:foldl(overwritten_UUIDs_active_fold_helper(Active),
                        [],
                        orddict:to_list(Dict))
    end.

-spec overwritten_UUIDs_active_fold_helper(lfs_manifest()) ->
    fun(({binary(), lfs_manifest()}, [binary()]) -> [binary()]).
overwritten_UUIDs_active_fold_helper(Active) ->
    fun({UUID, Manifest}, Acc) ->
            update_acc(UUID, Manifest, Acc, Active =:= Manifest)
    end.

-spec update_acc(binary(), lfs_manifest(), [binary()], boolean()) ->
    [binary()].
update_acc(_UUID, _Manifest, Acc, true) ->
    Acc;
update_acc(UUID, ?MANIFEST{state = active}, Acc, false) ->
    [UUID | Acc];
update_acc(UUID, Manifest=?MANIFEST{state = writing}, Acc, _) ->
    LBWT = Manifest?MANIFEST.last_block_written_time,
    WST = Manifest?MANIFEST.write_start_time,
    acc_leeway_helper(UUID, Acc, LBWT, WST);
update_acc(_, _, Acc, _) ->
   Acc.

-spec acc_leeway_helper(binary(), [binary()],
                        undefined | erlang:timestamp(),
                        undefined | erlang:timestamp()) ->
    [binary()].
acc_leeway_helper(UUID, Acc, undefined, WST) ->
    acc_leeway_helper(UUID, Acc, WST);
acc_leeway_helper(UUID, Acc, LBWT, _) ->
    acc_leeway_helper(UUID, Acc, LBWT).

-spec acc_leeway_helper(binary(), [binary()], undefined | erlang:timestamp()) ->
    [binary()].
acc_leeway_helper(UUID, Acc, Time) ->
    handle_leeway_elaped_time(leeway_elapsed(Time), UUID, Acc).

-spec handle_leeway_elaped_time(boolean(), binary(), [binary()]) ->
    [binary()].
handle_leeway_elaped_time(true, UUID, Acc) ->
    [UUID | Acc];
handle_leeway_elaped_time(false, _UUID, Acc) ->
    Acc.

%% @doc Return a list of manifests that are either
%% in `PendingDeleteUUIDs' or are in the `pending_delete'
%% state and have been there for longer than the retry
%% interval.
-spec manifests_to_gc([cs_uuid()], orddict:orddict()) -> [cs_uuid_and_manifest()].
manifests_to_gc(PendingDeleteUUIDs, Manifests) ->
    FilterFun = pending_delete_helper(PendingDeleteUUIDs),
    orddict:to_list(orddict:filter(FilterFun, Manifests)).

%% @private
%% Return a function for use in `orddict:filter/2'
%% that will return true if the manifest key is
%% in `UUIDs' or the manifest should be retried
%% moving to the GC bucket
-spec pending_delete_helper([binary()]) ->
    fun((binary(), lfs_manifest()) -> boolean()).
pending_delete_helper(UUIDs) ->
    fun(Key, Manifest) ->
            lists:member(Key, UUIDs) orelse retry_manifest(Manifest)
    end.

%% @private
%% Return true if this manifest should be retried
%% moving to the GC bucket
-spec retry_manifest(lfs_manifest()) -> boolean().
retry_manifest(?MANIFEST{state = pending_delete,
                         delete_marked_time = MarkedTime}) ->
    retry_from_marked_time(MarkedTime, os:timestamp());
retry_manifest(_Manifest) ->
    false.

%% @private
%% Return true if the time elapsed between
%% `MarkedTime' and `Now' is greater than
%% `riak_cs_gc:gc_retry_interval()'.
-spec retry_from_marked_time(erlang:timestamp(), erlang:timestamp()) ->
    boolean().
retry_from_marked_time(MarkedTime, Now) ->
    NowSeconds = riak_cs_utils:second_resolution_timestamp(Now),
    MarkedTimeSeconds = riak_cs_utils:second_resolution_timestamp(MarkedTime),
    NowSeconds > (MarkedTimeSeconds + riak_cs_gc:gc_retry_interval()).

%% @doc Remove all manifests that require pruning,
%%      see needs_pruning() for definition of needing pruning.
-spec prune(orddict:orddict()) -> orddict:orddict().
prune(Dict) ->
    MaxCount = riak_cs_gc:max_scheduled_delete_manifests(),
    rcs_common_manifest_utils:prune(Dict, erlang:timestamp(), MaxCount, riak_cs_gc:leeway_seconds()).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec leeway_elapsed(undefined | erlang:timestamp()) -> boolean().
leeway_elapsed(undefined) ->
    false;
leeway_elapsed(Timestamp) ->
    Now = riak_cs_utils:second_resolution_timestamp(os:timestamp()),
    Now > (riak_cs_utils:second_resolution_timestamp(Timestamp) + riak_cs_gc:leeway_seconds()).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_mani_helper() ->
    riak_cs_lfs_utils:new_manifest(
      <<"bucket">>, <<"key">>, <<"1.0">>, <<"uuid">>,
      100, %% content-length
      <<"ctype">>,
      undefined, %% md5
      orddict:new(),
      10,
      undefined,
      [],
      undefined,
      undefined).

manifest_test_() ->
    {setup,
        fun setup/0,
        fun cleanup/1,
        [fun wrong_state_for_pruning/0,
         fun wrong_state_for_pruning_2/0,
         fun does_need_pruning/0,
         fun not_old_enough_for_pruning/0]
    }.

setup() ->
    ok.

cleanup(_Ctx) ->
    ok.

wrong_state_for_pruning() ->
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=active},
    ?assert(not rcs_common_manifest_utils:needs_pruning(Mani2, erlang:timestamp(), 5)).

wrong_state_for_pruning_2() ->
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=pending_delete},
    ?assert(not rcs_common_manifest_utils:needs_pruning(Mani2, erlang:timestamp(), 5)).

does_need_pruning() ->
    application:set_env(riak_cs, leeway_seconds, 1),
    %% 1000000 second diff
    ScheduledDeleteTime = {1333,985708,445136},
    Now = {1334,985708,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=scheduled_delete,
                                scheduled_delete_time=ScheduledDeleteTime},
    ?assert(rcs_common_manifest_utils:needs_pruning(Mani2, Now, 5)).

not_old_enough_for_pruning() ->
    application:set_env(riak_cs, leeway_seconds, 2),
    %$ 1 second diff
    ScheduledDeleteTime = {1333,985708,445136},
    Now = {1333,985709,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=scheduled_delete,
                                scheduled_delete_time=ScheduledDeleteTime},
    ?assert(not rcs_common_manifest_utils:needs_pruning(Mani2, Now, 5)).

-endif.
