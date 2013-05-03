%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Riak Time Samples.

%% Each sample is stored as a JSON object with `start_time' and
%% `end_time' fields, expressed as strings in ISO8601 format.  The
%% object stored in riak is keyed by the start of the slice in which
%% the `Start' time falls.

%% Slices start at midnight and progress through the day (all times
%% are UTC).  If the period does not evenly divide the day, the final
%% slice will be truncated at midnight.

%% When multiple samples for a time+postfix are stored, each is
%% expressed as a sibling of the Riak object.  (TODO: compaction) It
%% is therefore important to ensure that `allow_mult=true' is set for
%% the bucket in which the samples are stored.

%% The `Data' argument passed to {@link new_sample/6} is expected to
%% be a proplist suitable for inclusion in the Mochijson2 structure
%% that includes the start and end times.

-module(rts).

-export([
         new_sample/6,
         find_samples/6,
         slice_containing/2,
         next_slice/2,
         iso8601/1
        ]).

-include("rts.hrl").
-ifdef(TEST).
-ifdef(EQC).
-compile([export_all]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([slice/0]).

-type datetime() :: calendar:datetime().
-type slice() :: {Start :: datetime(), End :: datetime()}.
-type riakc_pb_socket() :: pid().
-type mochijson2() :: term().

%% @doc Just create the new sample object (don't store it).
-spec new_sample(binary(), iolist(),
                 datetime(), datetime(),
                 integer(), mochijson2())
         -> riakc_obj:riakc_obj().
new_sample(Bucket, KeyPostfix, Start, End, Period, Data) ->
    Slice = slice_containing(Start, Period),
    Key = slice_key(Slice, KeyPostfix),
    MJSON = {struct, [{?START_TIME, iso8601(Start)},
                      {?END_TIME, iso8601(End)}
                      |Data]},
    Body = iolist_to_binary(mochijson2:encode(MJSON)),
    riakc_obj:new(Bucket, Key, Body, "application/json").

%% @doc Fetch all of the samples from riak that overlap the specified
%% time period for the given key postfix.
%%
%% This implementation reads each slice object from riak, and does the
%% extraction/etc. on the client side.  It would be a a trivial
%% modification to do this via MapReduce instead.
-spec find_samples(riakc_pb_socket(), binary(), iolist(),
                   datetime(), datetime(), integer()) ->
         {Samples::[mochijson2()], Errors::[{slice(), Reason::term()}]}.
find_samples(Riak, Bucket, KeyPostfix, Start, End, Period) ->
    Slices = slices_filling(Start, End, Period),
    Puller = sample_puller(Riak, Bucket, KeyPostfix),
    {Samples, Errors} = lists:foldl(Puller, {[], []}, Slices),
    {lists:filter(sample_in_bounds(Start, End), Samples), Errors}.

%% @doc Make a thunk that lists:filter can use to filter samples for a
%% given time period.  Samples are stored in groups, an a user may
%% request some, but not all, samples from a group.
-spec sample_in_bounds(datetime(), datetime())
         -> fun( (list()) -> boolean() ).
sample_in_bounds(Start, End) ->
    Start8601 = iso8601(Start),
    End8601 = iso8601(End),
    fun(Sample) ->
            {?START_TIME, SampleStart}
                = lists:keyfind(?START_TIME, 1, Sample),
            {?END_TIME, SampleEnd}
                = lists:keyfind(?END_TIME, 1, Sample),
            (SampleStart < End8601) and (SampleEnd > Start8601)
    end.

%% @doc Make a thunk that looks up samples for a given bucket+prefix.
-spec sample_puller(riakc_pb_socket(), binary(), iolist()) -> fun().
sample_puller(Riak, Bucket, Postfix) ->
    fun(Slice, {Samples, Errors}) ->
            case riakc_pb_socket:get(
                   Riak, Bucket, slice_key(Slice, Postfix)) of
                {ok, Object} ->
                    RawSamples =
                        [ catch element(2, {struct,_}=mochijson2:decode(V))
                          || V <- riakc_obj:get_values(Object) ],
                    {NewSamples, EncodingErrors} =
                        lists:partition(fun({'EXIT',_}) -> false;
                                           (_)          -> true
                                        end,
                                        RawSamples),
                    {NewSamples++Samples,
                     [{Slice, {encoding, length(EncodingErrors)}}
                      || EncodingErrors /= []]
                     ++Errors};
                {error, notfound} ->
                    %% this is normal - we ask for all possible
                    %% archives, and just deal with the ones that exist
                    {Samples, Errors};
                {error, Error} ->
                    {Samples, [{Slice, Error}|Errors]}
            end
    end.

%% @doc Make the key for this slice+postfix. Note: this must be the
%% actual slice, not just any two times (the times are not realigned
%% to slice boundaries before making the key).
-spec slice_key(slice(), iolist()) -> binary().
slice_key({SliceStart, _}, Postfix) ->
    iolist_to_binary([iso8601(SliceStart),".",Postfix]).

%% @doc Get the slice containing the `Time', given a period.
-spec slice_containing(datetime(), integer()) -> slice().
slice_containing({{_,_,_},{H,M,S}}=Time, Period) ->
    Rem = ((H*60+M)*60+S) rem Period,
    Sec = dtgs(Time),
    cut_at_midnight({gsdt(Sec-Rem), gsdt(Sec+Period-Rem)}).

%% @doc Get the slice following the one given.
-spec next_slice(Slice::slice(), integer()) -> slice().
next_slice({_,Prev}, Period) ->
    cut_at_midnight({Prev, gsdt(dtgs(Prev)+Period)}).

%% @doc ensure that slices do not leak across day boundaries
-spec cut_at_midnight(slice()) -> slice().
cut_at_midnight({{SameDay,_},{SameDay,_}}=Slice) ->
    Slice;
cut_at_midnight({Start,{NextDay,_}}) ->
    %% TODO: this is broken if Period is longer than 1 day
    {Start, {NextDay, {0,0,0}}}.

%% @doc Get all slices covering the period from `Start' to `End'.
-spec slices_filling(datetime(), datetime(), integer())
         -> [slice()].
slices_filling(Start, End, Period) when Start > End ->
    slices_filling(End, Start, Period);
slices_filling(Start, End, Period) ->
    {_, Last} = slice_containing(End, Period),
    lists:reverse(fill(Period, Last, [slice_containing(Start, Period)])).

%% @doc Add slices to `Fill' until we've covered `Last'.
-spec fill(integer(), datetime(), [slice()]) -> [slice()].
fill(_, Last, [{_,Latest}|_]=Fill) when Latest >= Last ->
    %% just in case our iterative math is borked, checking >= instead
    %% of == should guarantee we stop anyway
    Fill;
fill(Period, Last, [Prev|_]=Fill) ->
    Next = next_slice(Prev, Period),
    fill(Period, Last, [Next|Fill]).

%% @doc convenience
-spec dtgs(datetime()) -> integer().
dtgs(DT) -> calendar:datetime_to_gregorian_seconds(DT).
-spec gsdt(integer()) -> datetime().
gsdt(S)  -> calendar:gregorian_seconds_to_datetime(S).

%% @doc Produce an ISO8601-compatible representation of the given time.
-spec iso8601(calendar:datetime()) -> binary().
iso8601({{Y,M,D},{H,I,S}}) ->
    iolist_to_binary(
      io_lib:format("~4..0b~2..0b~2..0bT~2..0b~2..0b~2..0bZ",
                    [Y, M, D, H, I, S])).

-ifdef(TEST).
-ifdef(EQC).

iso8601_test() ->
    true = eqc:quickcheck(iso8601_roundtrip_prop()).

%% make sure that iso8601 roundtrips with datetime
iso8601_roundtrip_prop() ->
    %% iso8601 & datetime don't actually care if the date was valid,
    %% but writing a valid generator was fun

    %% datetime/1 is exported from riak_cs_wm_usage when TEST and
    %% EQC are defined
    ?FORALL(T, datetime_g(),
            is_binary(iso8601(T)) andalso
                {ok, T} == riak_cs_wm_usage:datetime(iso8601(T))).


slice_containing_test() ->
    true = eqc:quickcheck(slice_containing_prop()).

%% make sure that slice_containing returns a slice that is the length
%% of the archive period, and actually surrounds the given time
slice_containing_prop() ->
    ?FORALL({T, I}, {datetime_g(), valid_period_g()},
            begin
                {S, {_,ET}=E} = slice_containing(T, I),

                ?WHENFAIL(
                   io:format(user, "Containing slice: {~p, ~p}~n", [S, E]),
                   %% slice actually surrounds time
                   S =< T andalso T =< E andalso
                   %% slice length is the configured period ...
                   (I == datetime_diff(S, E) orelse
                   %% ... or less if it's the last period of the day
                      (I > datetime_diff(S, E) andalso ET == {0,0,0})) andalso
                   %% start of slice is N periods from start of day
                   0 == datetime_diff(S, {element(1, S),{0,0,0}}) rem I)
            end).

datetime_diff(S, E) ->
    abs(calendar:datetime_to_gregorian_seconds(E)
        -calendar:datetime_to_gregorian_seconds(S)).

next_slice_test() ->
    true = eqc:quickcheck(next_slice_prop()).

%% make sure the "next" slice is starts at the end of the given slice,
%% and is the length of the configured period
next_slice_prop() ->
    ?FORALL({T, I}, {datetime_g(), valid_period_g()},
            begin
                {S1, E1} = slice_containing(T, I),
                {S2, {_,ET}=E2} = next_slice({S1, E1}, I),
                ?WHENFAIL(
                   io:format(user,
                             "Slice Containing: {~p, ~p}~n"
                             "Next Slice: {~p, ~p}~n",
                             [S1, E1, S2, E2]),
                   %% next starts when prev ended
                   S2 == E1 andalso
                   %% slice length is the configured period ...
                   (I == datetime_diff(S2, E2) orelse
                    %% ... or less if it's the last period of the day
                    (I >= datetime_diff(S2, E2) andalso ET == {0,0,0})))
            end).

slices_filling_test() ->
    true = eqc:quickcheck(slices_filling_prop()).

%% make sure that slices_filling produces a list of slices, where the
%% first slice contains the start time, the last slice contains the
%% last time, and the number of slices is equal to the number of
%% periods between the start of the first slice and the end of the
%% last slice; slices are not checked for contiguousness, since we
%% know that the function uses next_slice, and next_slice is tested
%% elsewhere
slices_filling_prop() ->
    ?FORALL({T0, I, M, R}, {datetime_g(), valid_period_g(), int(), int()},
            begin
                T1 = calendar:gregorian_seconds_to_datetime(
                       I*M+R+calendar:datetime_to_gregorian_seconds(T0)),
                Slices = slices_filling(T0, T1, I),
                [Early, Late] = lists:sort([T0, T1]),
                {SF,EF} = hd(Slices),
                {SL,EL} = lists:last(Slices),
                ?WHENFAIL(
                   io:format("SF: ~p~nT0: ~p~nEF: ~p~n~n"
                             "SL: ~p~nT1: ~p~nEL: ~p~n~n"
                             "# slices: ~p~n",
                             [SF, T0, EF, SL, T1, EL, Slices]),
                   eqc:conjunction(
                     [{start_first, SF =< Early},
                      {end_first, Early =< EF},
                      {start_last, SL =< Late},
                      {end_last, Late =< EL},
                      {count, length(Slices) ==
                           mochinum:int_ceil(datetime_diff(SF, EL) / I)}]))
            end).

make_object_test() ->
    true = eqc:quickcheck(make_object_prop()).

%% check that an archive object is in the right bucket, with a key
%% containing the end time and the username, with application/json as
%% the content type, and a value that is a JSON representation of the
%% sum of each access metric plus start and end times
make_object_prop() ->
    ?FORALL({Bucket, Postfix,
             T0, T1, Period},
            {string_g(), string_g(),
             datetime_g(), datetime_g(), valid_period_g()},
            begin
                {Start, End} = list_to_tuple(lists:sort([T0, T1])),
                {SliceStart,_} = slice_containing(Start, Period),
                Obj = new_sample(Bucket, Postfix, Start, End, Period, []),

                {struct, MJ} = mochijson2:decode(
                                 riakc_obj:get_update_value(Obj)),

                ?WHENFAIL(
                io:format(user, "keys: ~p~n", [MJ]),
                eqc:conjunction(
                     [{bucket, Bucket == riakc_obj:bucket(Obj)},
                      {key_user, 0 /= string:str(
                                        binary_to_list(riakc_obj:key(Obj)),
                                        binary_to_list(Postfix))},
                      {key_time, 0 /= string:str(
                                        binary_to_list(riakc_obj:key(Obj)),
                                        binary_to_list(iso8601(SliceStart)))},
                      {ctype, "application/json" ==
                           riakc_obj:md_ctype(
                             riakc_obj:get_update_metadata(Obj))},

                      {start_time, rts:iso8601(Start) ==
                           proplists:get_value(?START_TIME, MJ)},
                      {end_time, rts:iso8601(End) ==
                           proplists:get_value(?END_TIME, MJ)}]))
            end).

string_g() ->
    ?LET(L, ?SUCHTHAT(X, list(char()), X /= []), list_to_binary(L)).

%% generate a valid datetime tuple; years are 1970-2200, to keep them
%% more relevant-ish
datetime_g() ->
    ?LET({Y, M}, {choose(1970, 2200), choose(1, 12)},
         {{Y, M, valid_day_g(Y, M)},
          {choose(0, 23), choose(0, 59), choose(0, 59)}}).

valid_day_g(Year, 2) ->
    case {Year rem 4, Year rem 100, Year rem 400} of
        {_, _, 0} -> 29;
        {_, 0, _} -> 28;
        {0, _, _} -> 29;
        {_, _, _} -> 28
    end;
valid_day_g(_Year, Month) ->
    case lists:member(Month, [4, 6, 9, 11]) of
        true  -> 30;
        false -> 31
    end.

%% not exhaustive, but a good selection
valid_period_g() ->
    elements([1,10,100,
              2,4,8,16,32,
              3,9,27,
              6,18,54,
              12,24,48,96,
              60,600,3600,21600, % 1min, 10min, 1hr, 6hr
              86400,86401,86500]). % 1day, over 1 day (will be trunc'd)

-endif. % EQC
-endif. % TEST
