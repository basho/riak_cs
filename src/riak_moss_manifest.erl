%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module for choosing and manipulating lists (well, orddict) of manifests

-module(riak_moss_manifest).

-include("riak_moss.hrl").

%% export Public API
-export([active_manifest/1,
         prune/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the current active manifest
%% from an orddict of manifests.
-spec active_manifest(term()) -> lfs_manifest().
active_manifest(_Manifests) ->
    ok.

%% @doc Return a new orddict of Manifests
%% with any deleted and need-to-be-pruned
%% manifests removed
-spec prune(term()) -> term().
prune(_Manifests) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
