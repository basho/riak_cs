%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Functions to support garbage collection of files.

-module(riak_cs_gc).

-include("riak_moss.hrl").
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([schedule_manifests/2]).

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Copy data for a list of manifests to the
%% `riak-cs-gc' bucket to schedule them for deletion.
-spec schedule_manifests([lfs_manifest()], pid()) -> ok | {error, term()}.
schedule_manifests(Manifests, RiakPid) ->
    %% Create a set from the list of manifests
    ManifestSet = build_manifest_set(twop_set:new(), Manifests),
    %% Write the set to a timestamped key in the `riak-cs-gc' bucket
    Key = term_to_binary(erlang:now()),
    RiakObject = riakc_obj:new(?GC_BUCKET, Key, term_to_binary(ManifestSet)),
    riakc_pb_socket:put(RiakPid, RiakObject).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec build_manifest_set(twop_set:twop_set(), [lfs_manifest()]) -> twop_set:twop_set().
build_manifest_set(Set, []) ->
    Set;
build_manifest_set(Set, [HeadManifest | RestManifests]) ->
    UpdSet = twop_set:add_element(HeadManifest, Set),
    build_manifest_set(UpdSet, RestManifests).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
