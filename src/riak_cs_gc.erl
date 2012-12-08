%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Utility module for garbage collection of files.

-module(riak_cs_gc).

-include("riak_cs.hrl").
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([decode_and_merge_siblings/2,
         gc_interval/0,
         gc_retry_interval/0,
         gc_active_manifests/3,
         gc_specific_manifests/3,
         epoch_start/0,
         leeway_seconds/0,
         timestamp/0]).

%%%===================================================================
%%% Public API
%%%===================================================================

gc_active_manifests(Manifests, RiakObject, RiakcPid) ->
    ActiveManifests = riak_cs_manifest_utils:active_and_writing_manifests(Manifests),
    ActiveUUIDs = [UUID || {UUID, _} <- ActiveManifests],
    GCManiResponse = gc_specific_manifests(ActiveUUIDs,
                                           RiakObject,
                                           RiakcPid),
    return_active_uuids_from_gc_response(GCManiResponse,
                                         ActiveUUIDs).

%% @private
return_active_uuids_from_gc_response({ok, _RiakObject}, ActiveUUIDs) ->
    {ok, ActiveUUIDs};
return_active_uuids_from_gc_response({error, _Error}=Error, _ActiveUUIDs) ->
    Error.

%% @private
-spec gc_specific_manifests(UUIDsToMark :: [binary()],
                   RiakObject :: riakc_obj:riak_object(),
                   RiakcPid :: pid()) ->
    {error, term()} | {ok, riakc_obj:riak_object()}.
gc_specific_manifests(UUIDsToMark, RiakObject, RiakcPid) ->
    MarkedResult = mark_as_pending_delete(UUIDsToMark,
                                          RiakObject,
                                          RiakcPid),
    handle_mark_as_pending_delete(MarkedResult, UUIDsToMark, RiakcPid).

%% @private
-spec handle_mark_as_pending_delete({ok, riakc_obj:riak_object()},
                                    [binary()],
                                    pid()) ->
    {error, term()} | {ok, riakc_obj:riak_object()};

    ({error, term()}, [binary()], pid()) ->
    {error, term()} | {ok, riakc_obj:riak_object()}.
handle_mark_as_pending_delete({ok, RiakObject}, UUIDsToMark, RiakcPid) ->
    Manifests = riak_cs_utils:manifests_from_riak_object(RiakObject),
    PDManifests = riak_cs_manifest_utils:manifests_to_gc(UUIDsToMark, Manifests),
    MoveResult = move_manifests_to_gc_bucket(PDManifests, RiakcPid),
    PDUUIDs = [UUID || {UUID, _} <- PDManifests],
    handle_move_result(MoveResult, RiakObject, PDUUIDs, RiakcPid);
handle_mark_as_pending_delete({error, Error}=Error, _UUIDsToMark, _RiakcPid) ->
    _ = lager:warning("Failed to mark as pending_delete, reason: ~p", [Error]),
    Error.

%% @private
-spec handle_move_result(ok | {error, term()},
                         riakc_obj:riak_object(),
                         [binary()],
                         pid()) ->
    {ok, riakc_obj:riak_object()} | {error, term()}.
handle_move_result(ok, RiakObject, PDUUIDs, RiakcPid) ->
    mark_as_scheduled_delete(PDUUIDs, RiakObject, RiakcPid);
handle_move_result({error, _Error}=Error, _RiakObject, _PDUUIDs, _RiakcPid) ->
    Error.

%% @doc Return the number of seconds to wait after finishing garbage
%% collection of a set of files before starting the next.
-spec gc_interval() -> non_neg_integer().
gc_interval() ->
    case application:get_env(riak_cs, gc_interval) of
        undefined ->
            ?DEFAULT_GC_INTERVAL;
        {ok, Interval} ->
            Interval
    end.

%% @doc Return the number of seconds to wait before rescheduling a
%% `pending_delete' manifest for garbage collection.
-spec gc_retry_interval() -> non_neg_integer().
gc_retry_interval() ->
    case application:get_env(riak_cs, gc_retry_interval) of
        undefined ->
            ?DEFAULT_GC_RETRY_INTERVAL;
        {ok, RetryInterval} ->
            RetryInterval
    end.

%% @doc Return the start of GC epoch represented as a binary.
%% This is the time that the GC daemon uses to  begin collecting keys
%% from the `riak-cs-gc' bucket.
-spec epoch_start() -> binary().
epoch_start() ->
    case application:get_env(riak_cs, epoch_start) of
        undefined ->
            ?EPOCH_START;
        {ok, EpochStart} ->
            EpochStart
    end.

%% @doc Return the minimum number of seconds a file manifest waits in
%% the `scheduled_delete' state before being garbage collected.
-spec leeway_seconds() -> non_neg_integer().
leeway_seconds() ->
    case application:get_env(riak_cs, leeway_seconds) of
        undefined ->
            ?DEFAULT_LEEWAY_SECONDS;
        {ok, LeewaySeconds} ->
            LeewaySeconds
    end.

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp() -> non_neg_integer().
timestamp() ->
    riak_cs_utils:timestamp(os:timestamp()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Mark a list of manifests as `pending_delete' based upon the
%% UUIDs specified.
-spec mark_as_pending_delete([binary()], riakc_obj:riak_object(), pid()) ->
    {ok, riakc_obj:riak_object()} | {error, term()}.
mark_as_pending_delete(UUIDsToMark, RiakObject, RiakcPid) ->
    mark_manifests(RiakObject, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_pending_delete/2,
                   RiakcPid).

%% @doc Mark a list of manifests as `scheduled_delete' based upon the
%% UUIDs specified.
-spec mark_as_scheduled_delete([binary()], riakc_obj:riak_object(), pid()) ->
    {ok, riakc_obj:riak_object()} | {error, term()}.
mark_as_scheduled_delete(UUIDsToMark, RiakObject, RiakcPid) ->
    mark_manifests(RiakObject, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_scheduled_delete/2,
                   RiakcPid).


%% @doc Call a `riak_cs_manifest_utils' function on a set of manifests
%% to update the state of the manifests specified by `UUIDsToMark'
%% and then write the updated values to riak.
-spec mark_manifests(riakc_obj:riak_object(), [binary()], fun(), pid()) ->
                    {ok, riakc_obj:riak_object()} | {error, term()}.
mark_manifests(RiakObject, UUIDsToMark, ManiFunction, RiakcPid) ->
    Manifests = riak_cs_utils:manifests_from_riak_object(RiakObject),
    Marked = ManiFunction(Manifests, UUIDsToMark),
    UpdObj0 = riakc_obj:update_value(RiakObject, term_to_binary(Marked)),
    UpdObj = riak_cs_manifest_fsm:update_md_with_multipart_2i(
               UpdObj0, Marked,
               riakc_obj:bucket(UpdObj0), riakc_obj:key(UpdObj0)),

    %% use [returnbody] so that we get back the object
    %% with vector clock. This allows us to do a PUT
    %% again without having to re-retrieve the object
    riak_cs_utils:put(RiakcPid, UpdObj, [return_body]).

%% @doc Copy data for a list of manifests to the
%% `riak-cs-gc' bucket to schedule them for deletion.
-spec move_manifests_to_gc_bucket([lfs_manifest()], pid()) ->
    ok | {error, term()}.
move_manifests_to_gc_bucket(Manifests, RiakcPid) ->
    Key = generate_key(),
    ManifestSet = build_manifest_set(Manifests),
    ObjectToWrite = case riakc_pb_socket:get(RiakcPid, ?GC_BUCKET, Key) of
        {error, notfound} ->
            %% There was no previous value, so we'll
            %% create a new riak object and write it
            riakc_obj:new(?GC_BUCKET, Key, term_to_binary(ManifestSet));
        {ok, PreviousObject} ->
            %% There is a value currently stored here,
            %% so resolve all the siblings and add the
            %% new set in as well. Write this
            %% value back to riak
            Resolved = decode_and_merge_siblings(PreviousObject, ManifestSet),
            riakc_obj:update_value(PreviousObject, term_to_binary(Resolved))
    end,

    %% Create a set from the list of manifests
    _ = lager:debug("Manifests scheduled for deletion: ~p", [ManifestSet]),
    riak_cs_utils:put(RiakcPid, ObjectToWrite).

-spec build_manifest_set([lfs_manifest()]) -> twop_set:twop_set().
build_manifest_set(Manifests) ->
    lists:foldl(fun twop_set:add_element/2, twop_set:new(), Manifests).

%% @doc Generate a key for storing a set of manifests in the
%% garbage collection bucket.
-spec generate_key() -> binary().
generate_key() ->
    list_to_binary(
      integer_to_list(
        timestamp() + leeway_seconds())).

%% @doc Given a list of riakc_obj-flavored object (with potentially
%%      many siblings and perhaps a tombstone), decode and merge them.
-spec decode_and_merge_siblings(riakc_obj:riakc_obj(), twop_set:twop_set()) ->
      twop_set:twop_set().
decode_and_merge_siblings(Obj, OtherManifestSets) ->
    Some = [binary_to_term(V) || {_, V}=Content <- riakc_obj:get_contents(Obj),
                                 not riak_cs_utils:has_tombstone(Content)],
    twop_set:resolve([OtherManifestSets | Some]).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
