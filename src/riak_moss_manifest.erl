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

%% @doc Return a new orddict of Manifests
%% with any deleted and need-to-be-pruned
%% manifests removed
%% TODO: write the function... :)

%% TODO: for pruning we're likely
%% going to want to add some app.config
%% stuff for how long to keep around
%% fully deleted manifests, just like
%% we do with vclocks.
-spec prune(term()) -> term().
prune(_Manifests) ->
    ok.

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
