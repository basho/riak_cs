%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module for choosing and manipulating lists (well, orddict) of manifests

-module(riak_moss_manifest).

-include("riak_moss.hrl").

%% export Public API
-export([new/2,
         active_manifest/1,
         mark_overwritten/1,
         need_gc/1,
         prune/1]).

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


%% @doc Return a <TBD> of the
%% manfiests that need to be
%% garbage collected.
-spec need_gc(term()) -> term().
need_gc(Manifests) ->
    [Id || {Id, Manifest} <- orddict:to_list(Manifests),
        needs_gc(Manifest)].

%% TODO: for pruning we're likely
%% going to want to add some app.config
%% stuff for how long to keep around
%% fully deleted manifests, just like
%% we do with vclocks.
-spec prune(term()) -> term().
prune(Manifests) ->
    [KV || {_K, V}=KV <- Manifests, not (needs_pruning(V))].

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

-spec needs_gc(lfs_manifest()) -> boolean().
needs_gc(#lfs_manifest_v2{state=pending_delete,
                                  delete_marked_time=DeleteMarkedTime,
                                  last_block_deleted_time=undefined}) ->
    seconds_diff(erlang:now(), DeleteMarkedTime) > delete_leeway_time();
needs_gc(#lfs_manifest_v2{state=pending_delete,
                                  last_block_deleted_time=LastBlockWrittenTime}) ->
    seconds_diff(erlang:now(), LastBlockWrittenTime) > retry_delete_time();
needs_gc(_Manifest) ->
    false.

-spec needs_pruning(lfs_manifest()) -> boolean().
needs_pruning(#lfs_manifest_v2{state=deleted,
                            delete_blocks_remaining=[],
                            last_block_deleted_time=DeleteTime}) ->
    seconds_diff(erlang:now(), DeleteTime) > delete_tombstone_time();
needs_pruning(_Manifest) ->
    false.

seconds_diff(T2, T1) ->
    TimeDiffMicrosends = timer:now_diff(T2, T1),
    SecondsTime = TimeDiffMicrosends / (1000 * 1000),
    erlang:trunc(SecondsTime).

-spec delete_leeway_time() -> pos_integer().
delete_leeway_time() ->
    case application:get_env(riak_moss, delete_leeway_time) of
        undefined ->
            ?DEFAULT_DELETE_LEEWAY_TIME;
        {ok, Time} ->
            Time
    end.

-spec delete_tombstone_time() -> pos_integer().
delete_tombstone_time() ->
    case application:get_env(riak_moss, delete_tombstone_time) of
        undefined ->
            ?DEFAULT_DELETE_TOMBSTONE_TIME;
        {ok, Time} ->
            Time
    end.

-spec retry_delete_time() -> pos_integer().
retry_delete_time() ->
    case application:get_env(riak_moss, retry_delete_time) of
        undefined ->
            ?DEFAULT_RETRY_DELETE_TIME;
        {ok, Time} ->
            Time
    end.
