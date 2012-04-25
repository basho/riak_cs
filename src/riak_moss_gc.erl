%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Utility module for Garbage Collection

-module(riak_moss_gc).

-include("riak_moss.hrl").

-export([
         slice_to_key/1,
         key_to_slice/1,
         slice_from_time/1,
         slice_from_time_with_leeway/1
         ]).

%%%===================================================================
%%% API
%%%===================================================================

slice_to_key(Slice) ->
    <<Slice:64>>.

key_to_slice(<<Slice:64>>) ->
    Slice.

slice_from_time(Time) ->
    Secs = timestamp_to_seconds(Time),
    slice_from_seconds(Secs).

slice_from_time_with_leeway(Time) ->
    slice_from_time_with_leeway(Time, delete_leeway_slices()).


%%%===================================================================
%%% Internal functions
%%%===================================================================

seconds_per_slice() ->
    case application:get_env(riak_moss, gc_seconds_per_slice) of
        undefined ->
            ?DEFAULT_GC_SECONDS_PER_SLICE;
        {ok, Seconds} ->
            Seconds
    end.

slice_from_time_with_leeway(Time, NumLeewaySlices) ->
    SecondsAddition = NumLeewaySlices * seconds_per_slice(),
    TimeInSeconds = timestamp_to_seconds(Time),
    slice_from_seconds(TimeInSeconds + SecondsAddition).

timestamp_to_seconds({MegaSecs, Secs, _MicroSecs}) ->
    (MegaSecs * (1000 * 1000)) + Secs.

slice_from_seconds(Secs) ->
    Secs div seconds_per_slice().

delete_leeway_slices() ->
    case application:get_env(riak_moss, num_leeway_slices) of
        undefined ->
            ?DEFAULT_NUM_LEEWAY_SLICES;
        {ok, NumLeewaySlices} ->
            NumLeewaySlices
    end.
