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
-export([delete_tombstone_time/0,
         gc_interval/0,
         gc_retry_interval/0,
         schedule_manifests/2,
         timestamp/0]).

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Return the minimum number of seconds a file manifest waits in
%% the `deleted' state before being removed from the file record.
-spec delete_tombstone_time() -> non_neg_integer().
delete_tombstone_time() ->
    case application:get_env(riak_moss, delete_tombstone_time) of
        undefined ->
            ?DEFAULT_DELETE_TOMBSTONE_TIME;
        {ok, TombstoneTime} ->
            TombstoneTime
    end.

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

%% @doc Copy data for a list of manifests to the
%% `riak-cs-gc' bucket to schedule them for deletion.
-spec schedule_manifests([lfs_manifest()], pid()) -> ok | {error, term()}.
schedule_manifests(Manifests, RiakPid) ->
    %% Create a set from the list of manifests
    ManifestSet = build_manifest_set(twop_set:new(), Manifests),
    _ = lager:debug("Manifests scheduled for deletion: ~p", [ManifestSet]),
    %% Write the set to a timestamped key in the `riak-cs-gc' bucket
    Key = generate_key(),
    RiakObject = riakc_obj:new(?GC_BUCKET, Key, term_to_binary(ManifestSet)),
    riakc_pb_socket:put(RiakPid, RiakObject).

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp() -> non_neg_integer().
timestamp() ->
    riak_moss_utils:timestamp(erlang:now()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec build_manifest_set(twop_set:twop_set(), [lfs_manifest()]) -> twop_set:twop_set().
build_manifest_set(Set, []) ->
    Set;
build_manifest_set(Set, [HeadManifest | RestManifests]) ->
    UpdSet = twop_set:add_element(HeadManifest, Set),
    build_manifest_set(UpdSet, RestManifests).

%% @doc Generate a key for storing a set of manifests in the
%% garbage collection bucket.
-spec generate_key() -> binary().
generate_key() ->
    list_to_binary(
      integer_to_list(
        timestamp() + leeway_seconds())).

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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
