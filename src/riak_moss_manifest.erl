%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module for choosing and manipulating lists (well, orddict) of manifests

-module(riak_moss_manifest).

-include("riak_moss.hrl").
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([new/2,
         active_manifest/1,
         active_and_writing_manifests/1,
         overwritten_UUIDs/1,
         mark_pending_delete/2,
         mark_scheduled_delete/2,
         pending_delete_manifests/1,
         prune/1,
         prune/2,
         upgrade_wrapped_manifests/1,
         upgrade_manifest/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return a new orddict of manifest (only
%% one in this case). Used when storing something
%% in Riak when the previous GET returned notfound,
%% so we're (maybe) creating a new object.
-spec new(binary(), lfs_manifest()) -> term().
new(UUID, Manifest) ->
    orddict:store(UUID, Manifest, orddict:new()).

%% @doc Return the current active manifest
%% from an orddict of manifests.
-spec active_manifest(orddict:orddict()) -> {ok, lfs_manifest()} | {error, no_active_manifest}.
active_manifest(Manifests) ->
    case lists:foldl(fun most_recent_active_manifest/2, no_active_manifest, orddict_values(Manifests)) of
        no_active_manifest ->
            {error, no_active_manifest};
        Manifest ->
            {ok, Manifest}
    end.

%% @doc Return a list of all manifests in the
%% `active' or `writing' state
-spec active_and_writing_manifests(term()) -> [lfs_manifest()].
active_and_writing_manifests(Manifests) ->
    filter_manifests_by_state(Manifests, [active, writing]).

%% @doc Mark all active manifests
%% that are not "the most active"
%% as pending_delete
-spec overwritten_UUIDs(term()) -> term().
overwritten_UUIDs(Manifests) ->
    case active_manifest(Manifests) of
        {error, no_active_manifest} ->
            FoldFun =
                fun ({_, ?MANIFEST{state=State}}, Acc) when State =:= writing ->
                        Acc;
                    ({UUID, _}, Acc) ->
                        [UUID | Acc]
                end;
        {ok, Active} ->
            FoldFun =
                fun ({UUID, Elem}, Acc) ->
                        case Elem of
                            Active ->
                                Acc;
                            Elem=?MANIFEST{state=active} ->
                                [UUID | Acc];
                            Elem=?MANIFEST{state=writing,
                                           last_block_written_time=undefined,
                                           write_start_time=WST} ->
                                case leeway_elapsed(WST) of
                                    true ->
                                        [UUID | Acc];
                                    false ->
                                        Acc
                                end;
                            Elem=?MANIFEST{state=writing,
                                           last_block_written_time=LBWT} ->
                                case leeway_elapsed(LBWT) of
                                    true ->
                                        [UUID | Acc];
                                    false ->
                                        Acc
                                end;
                            _ ->
                                Acc
                        end
                end
    end,
    lists:foldl(FoldFun, [], orddict:to_list(Manifests)).

%% @doc Return `Manifests' with the manifests in
%% `UUIDsToMark' with their state changed to
%% `pending_delete'
-spec mark_pending_delete(orddict:orddict(), list(binary())) ->
    orddict:orddict().
mark_pending_delete(Manifests, UUIDsToMark) ->
    MapFun = fun(K, V) ->
            case lists:member(K, UUIDsToMark) of
                true ->
                    V?MANIFEST{state=pending_delete,
                               delete_marked_time=os:timestamp()};
                false ->
                    V
            end
    end,
    orddict:map(MapFun, Manifests).

%% @doc Return `Manifests' with the manifests in
%% `UUIDsToMark' with their state changed to
%% `scheduled_delete'
-spec mark_scheduled_delete(orddict:orddict(), list(binary())) ->
    orddict:orddict().
mark_scheduled_delete(Manifests, UUIDsToMark) ->
    MapFun = fun(K, V) ->
            case lists:member(K, UUIDsToMark) of
                true ->
                    V?MANIFEST{state=scheduled_delete,
                               scheduled_delete_time=os:timestamp()};
                false ->
                    V
            end
    end,
    orddict:map(MapFun, Manifests).

%% @doc Return the current `pending_delete' manifests
%% from an orddict of manifests.
-spec pending_delete_manifests(term()) -> [{binary(), lfs_manifest()}].
pending_delete_manifests(Manifests) ->
    lists:filter(fun pending_delete_manifest/1, Manifests).

-spec prune(orddict:orddict(binary(), lfs_manifest())) ->
    orddict:orddict(binary(), lfs_manifest()).
prune(Manifests) ->
    prune(Manifests, erlang:now()).

-spec prune(orddict:orddict(binary(), lfs_manifest()), erlang:timestamp()) ->
    orddict:orddict(binary(), lfs_manifest()).
prune(Manifests, Time) ->
    [KV || {_K, V}=KV <- Manifests, not (needs_pruning(V, Time))].

upgrade_wrapped_manifests(ListofOrdDicts) ->
    DictMapFun = fun (_Key, Value) -> upgrade_manifest(Value) end,
    MapFun = fun (Value) -> orddict:map(DictMapFun, Value) end,
    lists:map(MapFun, ListofOrdDicts).

%% TODO:
%% not sure if there is a better spec for this,
%% since the input record could be one of several
%% versions.

%% @doc Upgrade the manifest to the most recent
%% version of the manifest record. This is so that
%% _most_ of the codebase only has to deal with
%% the most recent version of the record.
-spec upgrade_manifest(term()) -> lfs_manifest().
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

    ?MANIFEST{block_size=BlockSize,
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
              cluster_id=ClusterID};

upgrade_manifest(?MANIFEST{}=M) ->
    M.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Filter a list of manifests and accept only manifests whose
%% current state is specified in the `AcceptedStates' list.
-spec filter_manifests_by_state([lfs_manifest()], [atom()]) -> [lfs_manifest()].
filter_manifests_by_state(Manifests, AcceptedStates) ->
    AcceptManifest =
        fun ({_, ?MANIFEST{state=State}}) ->
                lists:member(State, AcceptedStates)
        end,
    lists:filter(AcceptManifest, Manifests).

-spec leeway_elapsed(undefined | erlang:timestamp()) -> boolean().
leeway_elapsed(undefined) ->
    false;
leeway_elapsed(Timestamp) ->
    Now = riak_moss_utils:timestamp(os:timestamp()),
    Now > (riak_moss_utils:timestamp(Timestamp) + riak_cs_gc:leeway_seconds()).

orddict_values(OrdDict) ->
    %% orddict's are by definition
    %% represented as lists, so no
    %% need to call orddict:to_list,
    %% which actually is the identity
    %% func
    [V || {_K, V} <- OrdDict].

most_recent_active_manifest(Manifest=?MANIFEST{state=active}, no_active_manifest) ->
    Manifest;
most_recent_active_manifest(_Manfest, no_active_manifest) ->
    no_active_manifest;
most_recent_active_manifest(Man1=?MANIFEST{state=active}, Man2=?MANIFEST{state=active}) ->
    case Man1?MANIFEST.write_start_time > Man2?MANIFEST.write_start_time of
        true -> Man1;
        false -> Man2
    end;
most_recent_active_manifest(Man1=?MANIFEST{state=active}, _Man2) -> Man1;
most_recent_active_manifest(_Man1, Man2=?MANIFEST{state=active}) -> Man2.

-spec needs_pruning(lfs_manifest(), erlang:timestamp()) -> boolean().
needs_pruning(?MANIFEST{state=scheduled_delete,
                              scheduled_delete_time=ScheduledDeleteTime}, Time) ->
    seconds_diff(Time, ScheduledDeleteTime) > riak_cs_gc:leeway_seconds();
needs_pruning(_Manifest, _Time) ->
    false.

pending_delete_manifest({_, ?MANIFEST{state=pending_delete,
                                      last_block_deleted_time=undefined}}) ->
    true;
pending_delete_manifest({_, ?MANIFEST{last_block_deleted_time=undefined}}) ->
    false;
pending_delete_manifest({_, ?MANIFEST{state=scheduled_delete,
                                      last_block_deleted_time=LBDTime}}) ->
    %% If a manifest is `scheduled_delete' and the amount of time
    %% specified by the retry interval has elapsed since a file block
    %% was last deleted, then reschedule it for deletion.
    LBDSeconds = riak_moss_utils:timestamp(LBDTime),
    Now = riak_moss_utils:timestamp(os:timestamp()),
    Now > (LBDSeconds + riak_cs_gc:gc_retry_interval());
pending_delete_manifest(_) ->
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
    riak_moss_lfs_utils:new_manifest(<<"bucket">>,
        <<"key">>,
        <<"uuid">>,
        100, %% content-length
        <<"ctype">>,
        undefined, %% md5
        orddict:new(),
        10,
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
    ?assert(not needs_pruning(Mani2, erlang:now())).

wrong_state_for_pruning_2() ->
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=pending_delete},
    ?assert(not needs_pruning(Mani2, erlang:now())).

does_need_pruning() ->
    application:set_env(riak_moss, leeway_seconds, 1),
    %% 1000000 second diff
    ScheduledDeleteTime = {1333,985708,445136},
    Now = {1334,985708,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=scheduled_delete,
                                scheduled_delete_time=ScheduledDeleteTime},
    ?assert(needs_pruning(Mani2, Now)).

not_old_enough_for_pruning() ->
    application:set_env(riak_moss, leeway_seconds, 2),
    %$ 1 second diff
    ScheduledDeleteTime = {1333,985708,445136},
    Now = {1333,985709,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani?MANIFEST{state=scheduled_delete,
                                scheduled_delete_time=ScheduledDeleteTime},
    ?assert(not needs_pruning(Mani2, Now)).

-endif.
