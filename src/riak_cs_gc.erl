%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Utility module for garbage collection of files.

-module(riak_cs_gc).

-include("riak_moss.hrl").
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([gc_interval/0,
         gc_retry_interval/0,
         gc_manifests/6,
         leeway_seconds/0,
         timestamp/0]).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec gc_manifests(binary(), binary(), [{binary(), lfs_manifest()}], [binary()],
    riakc_obj:riakc_obj(), pid()) ->
    {ok, term()} | {error, term()}.
gc_manifests(Bucket, Key, Manifests, UUIDsToMark, RiakObject, RiakcPid) ->
    MarkedResult = mark_as_pending_delete(Manifests,
                                          UUIDsToMark,
                                          RiakObject,
                                          RiakcPid),
    PDManifests = get_pending_delete_manifests(MarkedResult),
    MoveResult = move_manifests_to_gc_bucket(PDManifests, RiakcPid),
    mark_as_scheduled_delete(MoveResult,
                             Bucket,
                             Key,
                             [UUID || {UUID, _} <- PDManifests],
                             RiakcPid).

%% @doc Return the number of seconds to wait after finishing garbage
%% collection of a set of files before starting the next.
-spec gc_interval() -> non_neg_integer().
gc_interval() ->
    case application:get_env(riak_moss, gc_interval) of
        undefined ->
            ?DEFAULT_GC_INTERVAL;
        {ok, Interval} ->
            Interval
    end.

%% @doc Return the number of seconds to wait before rescheduling a
%% `pending_delete' manifest for garbage collection.
-spec gc_retry_interval() -> non_neg_integer().
gc_retry_interval() ->
    case application:get_env(riak_moss, gc_retry_interval) of
        undefined ->
            ?DEFAULT_GC_RETRY_INTERVAL;
        {ok, RetryInterval} ->
            RetryInterval
    end.

%% @doc Return the minimum number of seconds a file manifest waits in
%% the `scheduled_delete' state before being garbage collected.
-spec leeway_seconds() -> non_neg_integer().
leeway_seconds() ->
    case application:get_env(riak_moss, leeway_seconds) of
        undefined ->
            ?DEFAULT_LEEWAY_SECONDS;
        {ok, LeewaySeconds} ->
            LeewaySeconds
    end.

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp() -> non_neg_integer().
timestamp() ->
    riak_moss_utils:timestamp(os:timestamp()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Mark a list of manifests as `pending_delete' based upon the
%% UUIDs specified.
mark_as_pending_delete(Manifests, UUIDsToMark, RiakObject, RiakcPid) ->
    mark_manifests({ok, RiakObject, Manifests},
                   UUIDsToMark,
                   mark_pending_delete,
                   RiakcPid).

%% @doc Mark a list of manifests as `scheduled_delete' based upon the
%% UUIDs specified.
mark_as_scheduled_delete(ok, Bucket, Key, UUIDsToMark, RiakcPid) ->
    Manifests = riak_moss_utils:get_manifests(RiakcPid, Bucket, Key),
    {Result, _} = mark_manifests(Manifests,
                                 UUIDsToMark,
                                 mark_scheduled_delete,
                                 RiakcPid),
    Result;
mark_as_scheduled_delete({error, _}=Error, _, _, _, _) ->
    Error.

%% @doc Call a `riak_moss_manifest' function on a set of manifests
%% to update the state of the manifests specified by `UUIDsToMark'
%% and then write the updated values to riak.
mark_manifests({ok, RiakObject, Manifests}, UUIDsToMark, ManiFunction, RiakcPid) ->
    Marked = riak_moss_manifest:ManiFunction(Manifests, UUIDsToMark),
    UpdObj = riakc_obj:update_value(RiakObject, term_to_binary(Marked)),
    PutResult = riak_moss_utils:put_with_no_meta(RiakcPid, UpdObj),
    {PutResult, Marked};
mark_manifests({error, _Reason}=Error, _, _, _) ->
    Error.

%% @doc Compile a list of `pending_delete' manifests.
-spec get_pending_delete_manifests({ok, [{binary(), lfs_manifest()}]} |
                                   {error, term()}) ->
                                          [{binary(), lfs_manifest()}].
get_pending_delete_manifests({ok, MarkedManifests}) ->
    riak_moss_manifest:pending_delete_manifests(MarkedManifests);
get_pending_delete_manifests({error, Reason}) ->
    _ = lager:warning("Failed to get pending_delete manifests. Reason: ~p",
                      [Reason]),
    [].

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
            DecodedPrevious = [binary_to_term(V) ||
                V <- riakc_obj:get_values(PreviousObject)],
            SetsToResolve = [ManifestSet | DecodedPrevious],
            Resolved = twop_set:resolve(SetsToResolve),
            riakc_obj:update_value(PreviousObject, term_to_binary(Resolved))
    end,

    %% Create a set from the list of manifests
    _ = lager:debug("Manifests scheduled for deletion: ~p", [ManifestSet]),
    riak_moss_utils:put_with_no_meta(RiakcPid, ObjectToWrite).

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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
