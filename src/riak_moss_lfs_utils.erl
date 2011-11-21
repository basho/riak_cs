%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------


-module(riak_moss_lfs_utils).

-include("riak_moss.hrl").

-export([object_or_manifest/1,
         remove_chunk/2,
         still_waiting/1]).

%% @doc Returns whether or not
%%      a value is a normal object,
%%      or a manifest document
is_manifest(Value) ->
    is_record(Value, lfs_manifest).

%% @doc Remove a chunk from the
%%      chunks field of State
remove_chunk(_State, _Chunk) ->
    ok.

%% @doc Return true or false
%%      depending on whether
%%      we're still waiting
%%      to accumulate more chunks
still_waiting(_State) ->
    ok.
