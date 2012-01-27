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
-spec resolve_dicts(term(), term()) -> term().
resolve_dicts(A, B) ->
    dict:merge(fun resolve_manifests/2, A, B).

%% @doc Take two manifests with
%% the same UUID and resolve them
-spec resolve_manifests(term(), term()) -> term().
resolve_manifests(A, B) ->
    AState = riak_moss_lfs_utils:manifest_state(A),
    BState = riak_moss_lfs_utils:manifest_state(B),
    resolve_manifests(AState, BState, A, B).

resolve_manifests(_AState, _BState, _A, _B) -> ok.
