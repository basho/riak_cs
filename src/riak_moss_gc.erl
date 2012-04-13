%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module for coordinating the garbage collection of blocks

-module(riak_moss_gc).

-include("riak_moss.hrl").

-export([maybe_gc_and_prune/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Launch delete FSMs for any manifests
%% that need deleting. Then, remove
%% manifets that need to be pruned
-spec maybe_gc_and_prune([{binary(), lfs_manifest()}]) ->
    [{binary(), lfs_manifest()}].
maybe_gc_and_prune(Manifests) ->
    %% start GC
    To_GC = riak_moss_manifest:need_gc(Manifests),
    lists:foreach(fun start_gc/1, To_GC),

    %% return pruned
    riak_moss_manifest:prune(Manifests).

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_gc(_Manifest) -> ok.
