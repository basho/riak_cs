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
-export([key_to_slice/1,
         schedule_manifests/2,
         slice_from_time/1,
         slice_from_time_with_leeway/1,
         slice_to_key/1]).

%%%===================================================================
%%% Public API
%%%===================================================================

key_to_slice(<<Slice:64>>) ->
    Slice.

%% @doc Copy data for a list of manifests to the
%% `riak-cs-gc' bucket to schedule them for deletion.
-spec schedule_manifests([lfs_manifest()], pid()) -> ok | {error, term()}.
schedule_manifests(Manifests, RiakPid) ->
    %% Create a set from the list of manifests
    ManifestSet = build_manifest_set(twop_set:new(), Manifests),
    _ = lager:debug("Manifests scheduled for deletion: ~p", [ManifestSet]),
    %% Write the set to a timestamped key in the `riak-cs-gc' bucket
    Key = term_to_binary(erlang:now()),
    RiakObject = riakc_obj:new(?GC_BUCKET, Key, term_to_binary(ManifestSet)),
    riakc_pb_socket:put(RiakPid, RiakObject).

slice_from_time(Time) ->
    Secs = timestamp_to_seconds(Time),
    slice_from_seconds(Secs).

slice_from_time_with_leeway(Time) ->
    slice_from_time_with_leeway(Time, delete_leeway_slices()).

slice_to_key(Slice) ->
    <<Slice:64>>.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec build_manifest_set(twop_set:twop_set(), [lfs_manifest()]) -> twop_set:twop_set().
build_manifest_set(Set, []) ->
    Set;
build_manifest_set(Set, [HeadManifest | RestManifests]) ->
    UpdSet = twop_set:add_element(HeadManifest, Set),
    build_manifest_set(UpdSet, RestManifests).

delete_leeway_slices() ->
    case application:get_env(riak_moss, num_leeway_slices) of
        undefined ->
            ?DEFAULT_NUM_LEEWAY_SLICES;
        {ok, NumLeewaySlices} ->
            NumLeewaySlices
    end.


seconds_per_slice() ->
    case application:get_env(riak_moss, gc_seconds_per_slice) of
        undefined ->
            ?DEFAULT_GC_SECONDS_PER_SLICE;
        {ok, Seconds} ->
            Seconds
    end.

slice_from_seconds(Secs) ->
    Secs div seconds_per_slice().

slice_from_time_with_leeway(Time, NumLeewaySlices) ->
    SecondsAddition = NumLeewaySlices * seconds_per_slice(),
    TimeInSeconds = timestamp_to_seconds(Time),
    slice_from_seconds(TimeInSeconds + SecondsAddition).

timestamp_to_seconds({MegaSecs, Secs, _MicroSecs}) ->
    (MegaSecs * (1000 * 1000)) + Secs.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
