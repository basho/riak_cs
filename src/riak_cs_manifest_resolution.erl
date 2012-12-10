%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to resolve siblings with manifest records

-module(riak_cs_manifest_resolution).

-include("riak_cs.hrl").

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
    AState = state_to_stage_number(A?MANIFEST.state),
    BState = state_to_stage_number(B?MANIFEST.state),
    resolve_manifests(AState, BState, A, B).

state_to_stage_number(writing)          -> 10;
state_to_stage_number(active)           -> 20;
state_to_stage_number(pending_delete)   -> 30;
state_to_stage_number(scheduled_delete) -> 40.

%% @doc Return a new, resolved manifest.
%% The first two args are the state that
%% manifest A and B are in, respectively.
%% The third and fourth args, A, B, are the
%% manifests themselves.
%% @private
-spec resolve_manifests(integer(), integer(), term(), term()) -> term().

resolve_manifests(StageA, StageB, A, _B) when StageA > StageB ->
    A;
resolve_manifests(StageA, StageB, _A, B) when StageB > StageA ->
    B;
resolve_manifests(StageX, StageX, A, A) ->
    A;
resolve_manifests(_, _,
                  ?MANIFEST{state = writing} = A,
                  ?MANIFEST{state = writing} = B) ->
    WriteBlocksRemaining = resolve_written_blocks(A, B),
    LastBlockWrittenTime = resolve_last_written_time(A, B),
    Props = resolve_props(A, B),
    A?MANIFEST{write_blocks_remaining=WriteBlocksRemaining,
               last_block_written_time=LastBlockWrittenTime,
               props = Props};

%% Check for and handle differing ACLs, but otherwise purposely throw
%% a function clause exception if the manifests aren't equivalent
resolve_manifests(_, _,
                  A1=?MANIFEST{acl=A1Acl},
                  A2=?MANIFEST{acl=A2Acl}) when A1Acl =/= A2Acl ->
    case A1Acl?ACL.creation_time >= A2Acl?ACL.creation_time of
        true ->
            A1;
        false ->
            A2
    end;

resolve_manifests(_, _,
                  ?MANIFEST{state = pending_delete} = A,
                  ?MANIFEST{state = pending_delete} = B) ->
    BlocksLeftToDelete = resolve_deleted_blocks(A, B),
    LastDeletedTime = resolve_last_deleted_time(A, B),
    A?MANIFEST{delete_blocks_remaining=BlocksLeftToDelete,
                      last_block_deleted_time=LastDeletedTime};

resolve_manifests(_, _,
                  ?MANIFEST{state = scheduled_delete} = A,
                  ?MANIFEST{state = scheduled_delete} = B) ->
    BlocksLeftToDelete = resolve_deleted_blocks(A, B),
    LastDeletedTime = resolve_last_deleted_time(A, B),
    A?MANIFEST{delete_blocks_remaining=BlocksLeftToDelete,
                      last_block_deleted_time=LastDeletedTime}.

resolve_written_blocks(A, B) ->
    AWritten = A?MANIFEST.write_blocks_remaining,
    BWritten = B?MANIFEST.write_blocks_remaining,
    ordsets:intersection(AWritten, BWritten).

resolve_deleted_blocks(A, B) ->
    ADeleted = A?MANIFEST.delete_blocks_remaining,
    BDeleted = B?MANIFEST.delete_blocks_remaining,
    safe_intersection(ADeleted, BDeleted).

resolve_props(A, B) ->
    Ps_A = A?MANIFEST.props,
    Ps_B = B?MANIFEST.props,
    lists:foldl(fun resolve_a_prop/2,
                {Ps_A, Ps_B, []},
                [fun resolve_prop_multiprop/2]).

resolve_a_prop(Resolver, {Ps_A, Ps_B, Ps_merged}) ->
    Resolver(Ps_A, Ps_B) ++ Ps_merged.

resolve_prop_multiprop(Ps_A, Ps_B) ->
    case {proplists:get_value(multipart, Ps_A),
          proplists:get_value(multipart, Ps_B)} of
        {undefined, undefined} ->
            [];
        {undefined, B} ->
            [{multipart, B}];
        {A, undefined} ->
            [{multipart, A}];
        {A, B} ->
            Completes = ordsets:union(A?MULTIPART_MANIFEST.complete_requests,
                                      B?MULTIPART_MANIFEST.complete_requests),
            Aborts = ordsets:union(A?MULTIPART_MANIFEST.abort_requests,
                                   B?MULTIPART_MANIFEST.abort_requests),
            Parts = ordsets:union(A?MULTIPART_MANIFEST.parts,
                                  B?MULTIPART_MANIFEST.parts),
            DParts = ordsets:union(A?MULTIPART_MANIFEST.done_parts,
                                  B?MULTIPART_MANIFEST.done_parts),
            MM = A?MULTIPART_MANIFEST{complete_requests = Completes,
                                      abort_requests = Aborts,
                                      parts = Parts, done_parts = DParts},
            [{multipart, MM}]
    end.

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
    ALastWritten = A?MANIFEST.last_block_written_time,
    BLastWritten = B?MANIFEST.last_block_written_time,
    latest_date(ALastWritten, BLastWritten).

resolve_last_deleted_time(A, B) ->
    ALastDeleted = A?MANIFEST.last_block_deleted_time,
    BLastDeleted = B?MANIFEST.last_block_deleted_time,
    latest_date(ALastDeleted, BLastDeleted).

latest_date(A, B) -> erlang:max(A, B).
