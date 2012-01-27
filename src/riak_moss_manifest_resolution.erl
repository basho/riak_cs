%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to resolve siblings with manifest records

-module(riak_moss_manifest_resolution).

%% export Public API
-export([resolve/1]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Take a list of siblings
%% and resolve them to a single
%% value. In this case, siblings
%% and values are dictionaries whose
%% keys are UUIDs and whose values
%% are manifests.
-spec resolve(list()) -> term().
resolve(Siblings) ->
    lists:foldl(fun resolve_dicts/2, dict:new(), Siblings).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Take two dictionaries
%% of manifests and resolve them.
%% @private
-spec resolve_dicts(term(), term()) -> term().
resolve_dicts(A, B) ->
    dict:merge(fun resolve_manifests/2, A, B).

%% @doc Take two manifests with
%% the same UUID and resolve them
%% @private
-spec resolve_manifests(term(), term()) -> term().
resolve_manifests(A, B) ->
    AState = riak_moss_lfs_utils:manifest_state(A),
    BState = riak_moss_lfs_utils:manifest_state(B),
    resolve_manifests(AState, BState, A, B).

%% @doc Return a new, resolved manifest.
%% The first two args are the state that
%% manifest A and B are in, respectively.
%% The third and fourth args, A, B, are the
%% manifests themselves.
%% @private
-spec resolve_manifests(atom(), atom(), term(), term()) -> term().
resolve_manifests(writing, writing, A, B) ->
    BlocksWritten = resolve_written_blocks(A, B),
    LastWritten = resolve_last_written(A, B),
    NewMani1 = riak_moss_lfs_utils:update_blocks(A, BlocksWritten),
    riak_moss_lfs_utils:update_last_written(NewMani1, LastWritten);

resolve_manifests(writing, active, _A, B) -> B;
resolve_manifests(writing, pending_delete, _A, B) -> B;
resolve_manifests(writing, deleted, _A, B) -> B;

%% purposely throw a function clause
%% exception if the manifests aren't
%% equivalent
resolve_manifests(active, active, A, A) -> A;
resolve_manifests(active, pending_delete, _A, B) -> B;
resolve_manifests(active, deleted, _A, B) -> B;

resolve_manifests(pending_delete, pending_delete, A, B) ->
    BlocksLeftToDelete = resolve_deleted_blocks(A, B),
    LastDeleted = resolve_last_deleted(A, B),
    NewMani1 = riak_moss_lfs_utils:update_blocks_deleted(A, BlocksLeftToDelete),
    riak_moss_lfs_utils:update_last_deleted(NewMani1, LastDeleted);
resolve_manifests(pending_delete, deleted, _A, B) -> B;

resolve_manifests(deleted, deleted, A, A) -> A;
resolve_manifests(deleted, deleted, A, B) ->
    %% should this deleted date
    %% be different than the last block
    %% deleted date? I'm think yes, technically.
    LastDeleted = resolve_last_deleted(A, B),
    riak_moss_lfs_utils:update_last_deleted(A, LastDeleted).

%% TODO
resolve_written_blocks(_A, _B) -> ok.

%% TODO
resolve_deleted_blocks(_A, _B) -> ok.

%% TODO
resolve_last_written(_A, _B) -> ok.

%% TODO
resolve_last_deleted(_A, _B) -> ok.

latest_date(A, B) when A > B -> A;
latest_date(_A, B) -> B.
