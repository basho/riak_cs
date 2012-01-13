%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Data format and lookup utilities for access logs (I/O usage
%% stats).
%%
%% Most of these function have to do with computing "slices" given a
%% configured period.  Access data is stored once per some
%% pre-configured number of seconds.  The period boundaries are
%% absolute thoughout the day, though.  That is, configuring the
%% period for one hour (3600 seconds) means that slices always start
%% and end on the hour.  Thus, the need to compute the start and end
%% of a slice that surrounds some given time.
-module(riak_moss_access).

-export([
         archive_period/0,
         slice_key/2,
         slice_containing/1,
         slices_filling/2,
         next_slice/2,
         make_object/3,
         iso8601/1,
         get_usage/3
        ]).

-include("riak_moss.hrl").

-export_type([slice/0]).

-type slice() :: {Start :: calendar:datetime(),
                  End :: calendar:datetime()}.

-define(NODEKEY, <<"moss_node">>).

%% @doc Retreive the number of seconds that should elapse between
%% archivings of access stats.  This setting is controlled by the
%% `access_archive_period' environment variable of the `riak_moss'
%% application.  An error is returned if the setting does not evenly
%% divide one day's worth of seconds (24*60*60=86400).
-spec archive_period() -> {ok, integer()}|{error, term()}.
archive_period() ->
    case application:get_env(riak_moss, access_archive_period) of
        {ok, AP} when is_integer(AP) ->
            case (24*60*60) rem AP of
                0 ->
                    %% require period boundaries to fall on day boundaries
                    {ok, AP};
                _ ->
                    {error, "riak_moss:archive_period"
                            " does not evenly divide one day"}
            end;
        _ ->
            {error, "riak_moss:archive_period was not an integer"}
    end.

%% @doc Retrive the Riak object key under which the access data is
%% stored for the given user during the given time slice.
-spec slice_key(riak_moss:user_key(), slice()) -> binary().
slice_key(User, {_Start, End}) ->
    iolist_to_binary([User,".",iso8601(End)]).

%% @doc Get the slice containing the given time.
-spec slice_containing(calendar:datetime()) -> slice().
slice_containing({{_,_,_},{_,_,_}}=Time) ->
    {ok, Period} = archive_period(),
    Sec = calendar:datetime_to_gregorian_seconds(Time),
    Rem = Sec rem Period,
    Start = calendar:gregorian_seconds_to_datetime(Sec-Rem),
    End = calendar:gregorian_seconds_to_datetime(Sec+Period-Rem),
    {Start, End}.

%% @doc Get the slice following a given slice.
-spec next_slice(Slice::slice(), integer()) -> slice().
next_slice({_,Prev}, Period) ->
    Sec = calendar:datetime_to_gregorian_seconds(Prev),
    Next = calendar:gregorian_seconds_to_datetime(Sec+Period),
    {Prev, Next}.

%% @doc Get the slices between `Start' and `End', inclusive.
-spec slices_filling(calendar:datetime(), calendar:datetime())
         -> slice().
slices_filling(Start, End) ->
    {ok, Period} = archive_period(),
    Last = slice_containing(End),
    lists:reverse(fill(Period, Last, [slice_containing(Start)])).

fill(_, {_,E}, [{_,E}|_]=Fill) ->
    Fill;
fill(Period, Last, [Prev|_]=Fill) ->
    Next = next_slice(Prev, Period),
    fill(Period, Last, [Next|Fill]).

%% @doc Create a Riak object for storing a user/slice's access data.
%% The list of stats (`Accesses') must contain a list of proplists.
%% The keys of the proplist must be either atoms or binaries, to be
%% encoded as JSON keys.  The values of the proplists must be numbers,
%% as the values for each key will be summed in the stored object.
-spec make_object(riak_moss:user_key(),
                  [[{atom()|binary(), number()}]],
                  slice())
         -> riakc_obj:riakc_obj().
make_object(User, Accesses, {Start, End}=Slice) ->
    Aggregate = aggregate_accesses(Accesses),
    MJSON = {struct, [{?NODEKEY, node()},
                      {start_time, riak_moss_access:iso8601(Start)},
                      {end_time, riak_moss_access:iso8601(End)}
                      |Aggregate]},
    riakc_obj:new(?ACCESS_BUCKET,
                  slice_key(User, Slice),
                  iolist_to_binary(mochijson2:encode(MJSON)),
                  "application/json").

aggregate_accesses(Accesses) ->
    lists:foldl(fun merge_stats/2, [], Accesses).

merge_stats({_User, Stats}, Acc) ->
    %% TODO: orddict conversion could be omitted if Stats was already
    %% an orddict
    orddict:merge(fun(_K, V1, V2) -> V1+V2 end,
                  Acc,
                  orddict:from_list(Stats)).

%% @doc Produce an ISO8601-compatible representation of the given time.
-spec iso8601(calendar:datetime()|erlang:timestamp()) -> binary().
iso8601({_,_,_}=Now) ->
    iso8601(calendar:now_to_universal_time(Now));
iso8601({{Y,M,D},{H,I,S}}) ->
    iolist_to_binary(
      io_lib:format("~4..0b~2..0b~2..0bT~2..0b~2..0b~2..0bZ",
                    [Y, M, D, H, I, S])).

%% @doc Produce a usage compilation for the given `User' between
%% `Start' and `End' times, inclusive.  The result is an orddict in
%% which the keys are MOSS node names.  The value for each key is a
%% list of samples.  Each sample is an orddict full of metrics.
%%
%% This implementation reads each slice object from riak, and does the
%% extraction/etc. on the client side.  It would be a a trivial
%% modification to do this via MapReduce instead.
-spec get_usage(riak_moss:user_key(),
                calendar:datetime(),
                calendar:datetime()) -> orddict:orddict().
get_usage(User, Start, End) ->
    case riak_moss_utils:riak_connection() of
        {ok, Riak} ->
            Slices = slices_filling(Start, End),
            UsageAdder = usage_adder(User, Riak),
            Usage = lists:foldl(UsageAdder, [], Slices),
            riakc_pb_socket:stop(Riak),
            {ok, Usage};
        {error, Reason} ->
            {error, Reason}
    end.

usage_adder(User, Riak) ->
    fun(Slice, Usage) ->
            case riakc_pb_socket:get(Riak, ?ACCESS_BUCKET,
                                     slice_key(User, Slice)) of
                {ok, Object} ->
                    lists:foldl(fun add_usage/2, Usage,
                                riakc_obj:get_values(Object));
                {error, notfound} ->
                    %% this is normal - we ask for all possible
                    %% slices, and just deal with the ones that exist
                    Usage;
                Error ->
                    %% TODO: maybe include this as a result instead?
                    lager:warning(
                      "Usage rollup encountered error on ~s: ~p",
                      [slice_key(User, Slice), Error]),
                    Usage
            end
    end.

add_usage(JSON, Usage) ->
    {struct, Access} = mochijson2:decode(JSON),
    {value, {?NODEKEY, Node}, Other} = lists:keytake(?NODEKEY, 1, Access),
    orddict:append(Node, Other, Usage).
