%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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
-export([new_dict/2,
         active_manifest/1,
         active_manifests/1,
         active_and_writing_manifests/1,
         overwritten_UUIDs/1,
         deleted_while_writing/1,
         mark_pending_delete/2,
         mark_deleted/2,
         mark_scheduled_delete/2,
         manifests_to_gc/2,
         prune/1,
         prune/3,
         upgrade_wrapped_manifests/1,
         upgrade_manifest/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return a new orddict of manifest (only
%% one in this case). Used when storing something
%% in Riak when the previous GET returned notfound,
%% so we're (maybe) creating a new object.
-spec new_dict(binary(), lfs_manifest()) -> orddict:orddict().
new_dict(UUID, Manifest) ->
    orddict:store(UUID, Manifest, orddict:new()).

%% @doc Return the current active manifest
%% from an orddict of manifests.
-spec active_manifest(orddict:orddict()) -> {ok, lfs_manifest()} | {error, no_active_manifest}.
active_manifest(Dict) ->
    live_manifest(lists:foldl(fun most_recent_active_manifest/2,
            {no_active_manifest, undefined}, orddict_values(Dict))).

%% @doc Ensure the manifest hasn't been deleted during upload.
-spec live_manifest(tuple()) -> {ok, lfs_manifest()} | {error, no_active_manifest}.
live_manifest({no_active_manifest, _}) ->
    {error, no_active_manifest};
live_manifest({Manifest, undefined}) ->
    {ok, Manifest};
live_manifest({Manifest, DeleteTime}) ->
    case DeleteTime > Manifest?MANIFEST.write_start_time of
        true ->
            {error, no_active_manifest};
        false ->
            {ok, Manifest}
    end.

%% @doc Return all active manifests
-spec active_manifests(orddict:orddict()) -> [lfs_manifest()] | [].
active_manifests(Dict) ->
    lists:filter(fun manifest_is_active/1, orddict_values(Dict)).

%% @doc Return a list of all manifests in the
%% `active' or `writing' state
-spec active_and_writing_manifests(orddict:orddict()) -> [{binary(), lfs_manifest()}].
active_and_writing_manifests(Dict) ->
    orddict:to_list(filter_manifests_by_state(Dict, [active, writing])).

%% @doc Extract all manifests that are not "the most active"
%%      and not actively writing (within the leeway period).
-spec overwritten_UUIDs(orddict:orddict()) -> term().
overwritten_UUIDs(Dict) ->
    case active_manifest(Dict) of
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
update_acc(UUID, ?MANIFEST{state=active}, Acc, false) ->
    [UUID | Acc];
update_acc(UUID, Manifest=?MANIFEST{state=writing}, Acc, _) ->
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

%% @doc Return `Dict' with the manifests in
%% `UUIDsToMark' with their state changed to
%% `pending_delete' and {deleted, true} added to props.
-spec mark_deleted(orddict:orddict(), list(binary())) ->
    orddict:orddict().
mark_deleted(Dict, UUIDsToMark) ->
    MapFun = fun(K, V) ->
            case lists:member(K, UUIDsToMark) of
                true ->
                    V?MANIFEST{state=pending_delete,
                               delete_marked_time=os:timestamp(),
                               props=[{deleted, true} | V?MANIFEST.props]};
                false ->
                    V
            end
    end,
    orddict:map(MapFun, Dict).

%% @doc Return `Dict' with the manifests in
%% `UUIDsToMark' with their state changed to
%% `pending_delete'
-spec mark_pending_delete(orddict:orddict(), list(binary())) ->
    orddict:orddict().
mark_pending_delete(Dict, UUIDsToMark) ->
    MapFun = fun(K, V) ->
            case lists:member(K, UUIDsToMark) of
                true ->
                    V?MANIFEST{state=pending_delete,
                               delete_marked_time=os:timestamp()};
                false ->
                    V
            end
    end,
    orddict:map(MapFun, Dict).

%% @doc Return `Dict' with the manifests in
%% `UUIDsToMark' with their state changed to
%% `scheduled_delete'
-spec mark_scheduled_delete(orddict:orddict(), list(cs_uuid())) ->
                                   orddict:orddict().
mark_scheduled_delete(Dict, UUIDsToMark) ->
    MapFun = fun(K, V) ->
            case lists:member(K, UUIDsToMark) of
                true ->
                    V?MANIFEST{state=scheduled_delete,
                               scheduled_delete_time=os:timestamp()};
                false ->
                    V
            end
    end,
    orddict:map(MapFun, Dict).

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
retry_manifest(?MANIFEST{state=pending_delete,
                         delete_marked_time=MarkedTime}) ->
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
    prune(Dict, erlang:timestamp(), MaxCount).

-spec prune(orddict:orddict(),
            erlang:timestamp(),
            unlimited | non_neg_integer()) -> orddict:orddict().
prune(Dict, Time, MaxCount) ->
    Filtered = orddict:filter(fun (_Key, Value) -> not needs_pruning(Value, Time) end,
                              Dict),
    prune_count(Filtered, MaxCount).

-spec prune_count(orddict:orddict(), unlimited | non_neg_integer()) ->
    orddict:orddict().
prune_count(Manifests, unlimited) ->
    Manifests;
prune_count(Manifests, MaxCount) ->
    ScheduledDelete = filter_manifests_by_state(Manifests, [scheduled_delete]),
    UUIDAndTime = [{M?MANIFEST.uuid, M?MANIFEST.scheduled_delete_time} ||
                   {_UUID, M} <- ScheduledDelete],
    case length(UUIDAndTime) > MaxCount of
        true ->
            SortedByTimeRecentFirst = lists:keysort(2, UUIDAndTime),
            UUIDsToPrune = sets:from_list([UUID || {UUID, _ScheduledDeleteTime} <-
                                           lists:nthtail(MaxCount, SortedByTimeRecentFirst)]),
            orddict:filter(fun (UUID, _Value) -> not sets:is_element(UUID, UUIDsToPrune) end,
                       Manifests);
        false ->
            Manifests
    end.

-spec upgrade_wrapped_manifests([orddict:orddict()]) -> [orddict:orddict()].
upgrade_wrapped_manifests(ListofOrdDicts) ->
    DictMapFun = fun(_Key, Value) -> upgrade_manifest(Value) end,
    MapFun = fun(Value) -> orddict:map(DictMapFun, Value) end,
    lists:map(MapFun, ListofOrdDicts).

%% @doc Upgrade the manifest to the most recent
%% version of the manifest record. This is so that
%% _most_ of the codebase only has to deal with
%% the most recent version of the record.
-spec upgrade_manifest(lfs_manifest() | #lfs_manifest_v2{}) -> lfs_manifest().
upgrade_manifest(#lfs_manifest_v2{block_size=BlockSize,
                                 bkey=Bkey,
                                 metadata=Metadata,
                                 created=Created,
                                 uuid=UUID,
                                 content_length=ContentLength,
                                 content_type=ContentType,
                                 content_md5=ContentMd5,
                                 state=State,
                                 write_start_time=WriteStartTime,
                                 last_block_written_time=LastBlockWrittenTime,
                                 write_blocks_remaining=WriteBlocksRemaining,
                                 delete_marked_time=DeleteMarkedTime,
                                 last_block_deleted_time=LastBlockDeletedTime,
                                 delete_blocks_remaining=DeleteBlocksRemaining,
                                 acl=Acl,
                                 props=Properties,
                                 cluster_id=ClusterID}) ->

    upgrade_manifest(?MANIFEST{block_size=BlockSize,
                               bkey=Bkey,
                               metadata=Metadata,
                               created=Created,
                               uuid=UUID,
                               content_length=ContentLength,
                               content_type=ContentType,
                               content_md5=ContentMd5,
                               state=State,
                               write_start_time=WriteStartTime,
                               last_block_written_time=LastBlockWrittenTime,
                               write_blocks_remaining=WriteBlocksRemaining,
                               delete_marked_time=DeleteMarkedTime,
                               last_block_deleted_time=LastBlockDeletedTime,
                               delete_blocks_remaining=DeleteBlocksRemaining,
                               acl=Acl,
                               props=Properties,
                               cluster_id=ClusterID});

upgrade_manifest(?MANIFEST{props=Props}=M) ->
    M?MANIFEST{props=fixup_props(Props)}.

-spec fixup_props(undefined | list()) -> list().
fixup_props(undefined) ->
    [];
fixup_props(Props) when is_list(Props) ->
    Props.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Filter an orddict manifests and accept only manifests whose
%% current state is specified in the `AcceptedStates' list.
-spec filter_manifests_by_state(orddict:orddict(), [atom()]) -> orddict:orddict().
filter_manifests_by_state(Dict, AcceptedStates) ->
    AcceptManifest =
        fun(_, ?MANIFEST{state=State}) ->
                lists:member(State, AcceptedStates)
        end,
    orddict:filter(AcceptManifest, Dict).

-spec leeway_elapsed(undefined | erlang:timestamp()) -> boolean().
leeway_elapsed(undefined) ->
    false;
leeway_elapsed(Timestamp) ->
    Now = riak_cs_utils:second_resolution_timestamp(os:timestamp()),
    Now > (riak_cs_utils:second_resolution_timestamp(Timestamp) + riak_cs_gc:leeway_seconds()).

orddict_values(OrdDict) ->
    [V || {_K, V} <- orddict:to_list(OrdDict)].

manifest_is_active(?MANIFEST{state=active}) -> true;
manifest_is_active(_Manifest) -> false.

-spec delete_time(lfs_manifest()) -> erlang:timestamp() | undefined.
delete_time(Manifest) ->
    case proplists:is_defined(deleted, Manifest?MANIFEST.props) of
        true ->
            Manifest?MANIFEST.delete_marked_time;
        false ->
            undefined
    end.

%% @doc Return all active manifests that have timestamps before the latest deletion
%% This happens when a manifest is still uploading while it is deleted. The upload
%% is allowed to complete, but is not visible afterwards.
-spec deleted_while_writing(orddict:orddict()) -> [binary()].
deleted_while_writing(Manifests) ->
    ManifestList = orddict_values(Manifests),
    DeleteTime = latest_delete_time(ManifestList),
    find_deleted_active_manifests(ManifestList, DeleteTime).

-spec find_deleted_active_manifests([lfs_manifest()], term()) -> [cs_uuid()].
find_deleted_active_manifests(_Manifests, undefined) ->
    [];
find_deleted_active_manifests(Manifests, DeleteTime) ->
    [M?MANIFEST.uuid || M <- Manifests, M?MANIFEST.state =:= active,
                        M?MANIFEST.write_start_time < DeleteTime].

-spec latest_delete_time([lfs_manifest()]) -> term() | undefined.
latest_delete_time(Manifests) ->
    lists:foldl(fun(M, Acc) ->
                    DeleteTime = delete_time(M),
                    later(DeleteTime, Acc)
                end, undefined, Manifests).

%% @doc Return the later of two times, accounting for undefined
-spec later(undefined | erlang:timestamp(), undefined | erlang:timestamp()) ->
    undefined | erlang:timestamp().
later(undefined, undefined) ->
    undefined;
later(undefined, DeleteTime2) ->
    DeleteTime2;
later(DeleteTime1, undefined) ->
    DeleteTime1;
later(DeleteTime1, DeleteTime2) ->
    case DeleteTime1 > DeleteTime2 of
        true ->
            DeleteTime1;
        false ->
            DeleteTime2
    end.

%% NOTE: This is a foldl function, initial acc = {no_active_manifest, undefined}
%% Return the most recent active manifest as well as the most recent manifest delete time
-spec most_recent_active_manifest(lfs_manifest(), {
    no_active_manifest | lfs_manifest(), undefined | erlang:timestamp()}) ->
        {no_active_manifest | lfs_manifest(), erlang:timestamp() | undefined}.
most_recent_active_manifest(Manifest=?MANIFEST{state=scheduled_delete},
                            {MostRecent, undefined}) ->
    {MostRecent, delete_time(Manifest)};
most_recent_active_manifest(Manifest=?MANIFEST{state=scheduled_delete},
                            {MostRecent, DeleteTime}) ->
    Dt=delete_time(Manifest),
    {MostRecent, later(Dt, DeleteTime)};
most_recent_active_manifest(Manifest=?MANIFEST{state=pending_delete},
                            {MostRecent, undefined}) ->
    {MostRecent, delete_time(Manifest)};
most_recent_active_manifest(Manifest=?MANIFEST{state=pending_delete},
                           {MostRecent, DeleteTime}) ->
    Dt=delete_time(Manifest),
    {MostRecent, later(Dt, DeleteTime)};
most_recent_active_manifest(Manifest=?MANIFEST{state=active},
                            {no_active_manifest, undefined}) ->
    {Manifest, undefined};
most_recent_active_manifest(Man1=?MANIFEST{state=active},
                            {Man2=?MANIFEST{state=active}, DeleteTime})
    when Man1?MANIFEST.write_start_time > Man2?MANIFEST.write_start_time ->
        {Man1, DeleteTime};
most_recent_active_manifest(_Man1=?MANIFEST{state=active},
                            {Man2=?MANIFEST{state=active}, DeleteTime}) ->
    {Man2, DeleteTime};
most_recent_active_manifest(Man1=?MANIFEST{state=active},
                            {no_active_manifest, DeleteTime}) ->
    {Man1, DeleteTime};
most_recent_active_manifest(_Man1, {Man2=?MANIFEST{state=active}, DeleteTime}) ->
    {Man2, DeleteTime};
most_recent_active_manifest(_Manifest, {MostRecent, DeleteTime}) ->
    {MostRecent, DeleteTime}.

-spec needs_pruning(lfs_manifest(), erlang:timestamp()) -> boolean().
needs_pruning(?MANIFEST{state=scheduled_delete,
                              scheduled_delete_time=ScheduledDeleteTime}, Time) ->
    seconds_diff(Time, ScheduledDeleteTime) > riak_cs_gc:leeway_seconds();
needs_pruning(_Manifest, _Time) ->
    false.

seconds_diff(T2, T1) ->
    TimeDiffMicrosends = timer:now_diff(T2, T1),
    SecondsTime = TimeDiffMicrosends / (1000 * 1000),
    erlang:trunc(SecondsTime).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_mani_helper() ->
    riak_cs_lfs_utils:new_manifest(<<"bucket">>,
        <<"key">>,
        <<"uuid">>,
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
    ?assert(not needs_pruning(Mani2, erlang:timestamp())).

wrong_state_for_pruning_2() ->
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=pending_delete},
    ?assert(not needs_pruning(Mani2, erlang:timestamp())).

does_need_pruning() ->
    application:set_env(riak_cs, leeway_seconds, 1),
    %% 1000000 second diff
    ScheduledDeleteTime = {1333,985708,445136},
    Now = {1334,985708,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=scheduled_delete,
                                scheduled_delete_time=ScheduledDeleteTime},
    ?assert(needs_pruning(Mani2, Now)).

not_old_enough_for_pruning() ->
    application:set_env(riak_cs, leeway_seconds, 2),
    %$ 1 second diff
    ScheduledDeleteTime = {1333,985708,445136},
    Now = {1333,985709,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=scheduled_delete,
                                scheduled_delete_time=ScheduledDeleteTime},
    ?assert(not needs_pruning(Mani2, Now)).

-endif.
