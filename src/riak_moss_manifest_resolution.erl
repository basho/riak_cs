%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to resolve siblings with manifest records

-module(riak_moss_manifest_resolution).

-include("riak_moss.hrl").

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
    lists:foldl(fun resolve_dicts/2, orddict:new(), Siblings).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Take two dictionaries
%% of manifests and resolve them.
%% @private
-spec resolve_dicts(term(), term()) -> term().
resolve_dicts(A, B) ->
    orddict:merge(fun resolve_manifests/3, A, B).

%% @doc Take two manifests with
%% the same UUID and resolve them
%% @private
-spec resolve_manifests(term(), term(), term()) -> term().
resolve_manifests(_Key, A, B) ->
    AState = A#lfs_manifest_v2.state,
    BState = B#lfs_manifest_v2.state,
    resolve_manifests(AState, BState, A, B).

%% @doc Return a new, resolved manifest.
%% The first two args are the state that
%% manifest A and B are in, respectively.
%% The third and fourth args, A, B, are the
%% manifests themselves.
%% @private
-spec resolve_manifests(atom(), atom(), term(), term()) -> term().
resolve_manifests(writing, writing, A, B) ->
    WriteBlocksRemaining = resolve_written_blocks(A, B),
    LastBlockWrittenTime = resolve_last_written_time(A, B),
    A#lfs_manifest_v2{write_blocks_remaining=WriteBlocksRemaining, last_block_written_time=LastBlockWrittenTime};

resolve_manifests(writing, active, _A, B) -> B;
resolve_manifests(active, writing, A, B) ->
    resolve_manifests(writing, active, B, A);

resolve_manifests(writing, pending_delete, _A, B) -> B;
resolve_manifests(pending_delete, writing, A, B) ->
    resolve_manifests(writing, pending_delete, B, A);

resolve_manifests(writing, scheduled_delete, _A, B) -> B;
resolve_manifests(scheduled_delete, writing, A, B) ->
    resolve_manifests(writing, scheduled_delete, B, A);

resolve_manifests(writing, deleted, _A, B) -> B;
resolve_manifests(deleted, writing, A, B) ->
   resolve_manifests(writing, deleted, B, A);

%% Check for and handle differing ACLs, but otherwise purposely throw
%% a function clause exception if the manifests aren't equivalent
resolve_manifests(active, active, A, A) -> A;
resolve_manifests(active,
                  active,
                  A1=#lfs_manifest_v2{acl=A1Acl},
                  A2=#lfs_manifest_v2{acl=A2Acl}) when A1Acl =/= A2Acl ->
    case A1Acl?ACL.creation_time >= A2Acl?ACL.creation_time of
        true ->
            A1;
        false ->
            A2
    end;

resolve_manifests(active, pending_delete, _A, B) -> B;
resolve_manifests(pending_delete, active, A, B) ->
    resolve_manifests(active, pending_delete, B, A);

resolve_manifests(active, scheduled_delete, _A, B) -> B;
resolve_manifests(scheduled_delete, active, A, B) ->
    resolve_manifests(active, scheduled_delete, B, A);

resolve_manifests(active, deleted, _A, B) -> B;
resolve_manifests(deleted, active, A, B) ->
    resolve_manifests(active, deleted, B, A);

resolve_manifests(pending_delete, pending_delete, A, B) ->
    BlocksLeftToDelete = resolve_deleted_blocks(A, B),
    LastDeletedTime = resolve_last_deleted_time(A, B),
    A#lfs_manifest_v2{delete_blocks_remaining=BlocksLeftToDelete,
                      last_block_deleted_time=LastDeletedTime};
resolve_manifests(pending_delete, scheduled_delete, _A, B) -> B;
resolve_manifests(scheduled_delete, pending_delete, A, B) ->
    resolve_manifests(pending_delete, scheduled_delete, B, A);
resolve_manifests(pending_delete, deleted, _A, B) -> B;
resolve_manifests(deleted, pending_delete, A, B) ->
    resolve_manifests(pending_delete, deleted, B, A);

resolve_manifests(scheduled_delete, scheduled_delete, A, B) ->
    BlocksLeftToDelete = resolve_deleted_blocks(A, B),
    LastDeletedTime = resolve_last_deleted_time(A, B),
    A#lfs_manifest_v2{delete_blocks_remaining=BlocksLeftToDelete,
                      last_block_deleted_time=LastDeletedTime};
resolve_manifests(scheduled_delete, deleted, _A, B) -> B;
resolve_manifests(deleted, scheduled_delete, A, B) ->
    resolve_manifests(scheduled_delete, deleted, B, A);

resolve_manifests(deleted, deleted, A, A) -> A;
resolve_manifests(deleted, deleted, A, B) ->
    %% should this deleted date
    %% be different than the last block
    %% deleted date? I'm think yes, technically.
    LastBlockDeletedTime = resolve_last_deleted_time(A, B),
    A#lfs_manifest_v2{last_block_deleted_time=LastBlockDeletedTime}.

resolve_written_blocks(A, B) ->
    AWritten = A#lfs_manifest_v2.write_blocks_remaining,
    BWritten = B#lfs_manifest_v2.write_blocks_remaining,
    ordsets:intersection(AWritten, BWritten).

resolve_deleted_blocks(A, B) ->
    ADeleted = A#lfs_manifest_v2.delete_blocks_remaining,
    BDeleted = B#lfs_manifest_v2.delete_blocks_remaining,
    safe_intersection(ADeleted, BDeleted).

%% NOTE:
%% There was a bit of a gaff
%% and delete_blocks_remaining
%% was not set to an ordset
%% when the state was set to
%% pending_delete, so we have
%% to account for it being
%% `undefined`
safe_intersection(undefined, undefined) ->
    %% if these are both
    %% undefined, then
    %% neither have ever had
    %% delete_blocks_remaining set
    %% as something meaningful,
    %% so don't just change it
    %% to they empty set.
    undefined;
safe_intersection(A, undefined) ->
    safe_intersection(A, []);
safe_intersection(undefined, B) ->
    safe_intersection([], B);
safe_intersection(A, B) ->
    ordsets:intersection(A, B).

resolve_last_written_time(A, B) ->
    ALastWritten = A#lfs_manifest_v2.last_block_written_time,
    BLastWritten = B#lfs_manifest_v2.last_block_written_time,
    latest_date(ALastWritten, BLastWritten).

resolve_last_deleted_time(A, B) ->
    ALastDeleted = A#lfs_manifest_v2.last_block_deleted_time,
    BLastDeleted = B#lfs_manifest_v2.last_block_deleted_time,
    latest_date(ALastDeleted, BLastDeleted).

latest_date(A, B) -> erlang:max(A, B).
