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
         mark_overwritten/1,
         pending_delete_manifests/1,
         prune/1,
         prune/2]).

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
-spec active_manifest(term()) -> {ok, lfs_manifest()} | {error, no_active_manifest}.
active_manifest(Manifests) ->
    case lists:foldl(fun most_recent_active_manifest/2, no_active_manifest, orddict_values(Manifests)) of
        no_active_manifest ->
            {error, no_active_manifest};
        Manifest ->
            {ok, Manifest}
    end.

%% @doc Mark all active manifests
%% that are not "the most active"
%% as pending_delete
-spec mark_overwritten(term()) -> term().
mark_overwritten(Manifests) ->
    case active_manifest(Manifests) of
        {error, no_active_manifest} ->
            Manifests;
        {ok, Active} ->
            orddict:map(fun(_Key, Value) ->
                    if
                        Value == Active ->
                            Active;
                        Value#lfs_manifest_v2.state == active ->
                            Value#lfs_manifest_v2{state=pending_delete,
                                                  delete_marked_time=erlang:now()};
                        true ->
                            Value
                    end end,
                    Manifests)
    end.

%% @doc Return the current `pending_delete' manifests
%% from an orddict of manifests.
-spec pending_delete_manifests(term()) -> [lfs_manifest()].
pending_delete_manifests(Manifests) ->
    lists:foldl(fun pending_delete_manifest/2, [], Manifests).

-spec prune(list(lfs_manifest())) -> list(lfs_manifest()).
prune(Manifests) ->
    prune(Manifests, erlang:now()).

-spec prune(list(lfs_manifest()), erlang:timestamp()) -> list(lfs_manifest()).
prune(Manifests, Time) ->
    [KV || {_K, V}=KV <- Manifests, not (needs_pruning(V, Time))].

%%%===================================================================
%%% Internal functions
%%%===================================================================

orddict_values(OrdDict) ->
    %% orddict's are by definition
    %% represented as lists, so no
    %% need to call orddict:to_list,
    %% which actually is the identity
    %% func
    [V || {_K, V} <- OrdDict].

most_recent_active_manifest(Manifest=#lfs_manifest_v2{state=active}, no_active_manifest) ->
    Manifest;
most_recent_active_manifest(_Manfest, no_active_manifest) ->
    no_active_manifest;
most_recent_active_manifest(Man1=#lfs_manifest_v2{state=active}, Man2=#lfs_manifest_v2{state=active}) ->
    case Man1#lfs_manifest_v2.write_start_time > Man2#lfs_manifest_v2.write_start_time of
        true -> Man1;
        false -> Man2
    end;
most_recent_active_manifest(Man1=#lfs_manifest_v2{state=active}, _Man2) -> Man1;
most_recent_active_manifest(_Man1, Man2=#lfs_manifest_v2{state=active}) -> Man2.

-spec needs_pruning(lfs_manifest(), erlang:timestamp()) -> boolean().
needs_pruning(#lfs_manifest_v2{state=deleted,
                            delete_blocks_remaining=[],
                            last_block_deleted_time=DeleteTime}, Time) ->
    seconds_diff(Time, DeleteTime) > delete_tombstone_time();
needs_pruning(_Manifest, _Time) ->
    false.

pending_delete_manifest({_, Manifest=#lfs_manifest_v2{state=pending_delete}},
                        PDManifests) ->
    [Manifest | PDManifests];
pending_delete_manifest(_, _PDManifests) ->
    _PDManifests.

seconds_diff(T2, T1) ->
    TimeDiffMicrosends = timer:now_diff(T2, T1),
    SecondsTime = TimeDiffMicrosends / (1000 * 1000),
    erlang:trunc(SecondsTime).

-spec delete_tombstone_time() -> pos_integer().
delete_tombstone_time() ->
    case application:get_env(riak_moss, delete_tombstone_time) of
        undefined ->
            ?DEFAULT_DELETE_TOMBSTONE_TIME;
        {ok, Time} ->
            Time
    end.

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
    Mani2 = Mani#lfs_manifest_v2{state=active},
    ?assert(not needs_pruning(Mani2, erlang:now())).

wrong_state_for_pruning_2() ->
    Mani = new_mani_helper(),
    Mani2 = Mani#lfs_manifest_v2{state=pending_delete},
    ?assert(not needs_pruning(Mani2, erlang:now())).

does_need_pruning() ->
    application:set_env(riak_moss, delete_tombstone_time, 1),
    %% 1000000 second diff
    DeleteTime = {1333,985708,445136},
    Now = {1334,985708,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani#lfs_manifest_v2{state=deleted,
                                 delete_blocks_remaining=[],
                                 last_block_deleted_time=DeleteTime},
    ?assert(needs_pruning(Mani2, Now)).

not_old_enough_for_pruning() ->
    application:set_env(riak_moss, delete_tombstone_time, 2),
    %$ 1 second diff
    DeleteTime = {1333,985708,445136},
    Now = {1333,985709,445136},
    Mani = new_mani_helper(),
    Mani2 = Mani#lfs_manifest_v2{state=deleted,
                                 last_block_deleted_time=DeleteTime},
    ?assert(not needs_pruning(Mani2, Now)).

-endif.
