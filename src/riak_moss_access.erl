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
         datetime/1,
         get_usage/3,
         get_usage/4
        ]).

-include("riak_moss.hrl").
-ifdef(TEST).
-ifdef(EQC).
-compile([export_all]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

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
        {ok, AP} when is_integer(AP), AP > 0 ->
            case (24*60*60) rem AP of
                0 ->
                    %% require period boundaries to fall on day boundaries
                    {ok, AP};
                _ ->
                    {error, "riak_moss:access_archive_period"
                            " does not evenly divide one day"}
            end;
        _ ->
            {error, "riak_moss:access_archive_period was not an integer"}
    end.

%% @doc Retrive the Riak object key under which the access data is
%% stored for the given user during the given time slice.
-spec slice_key(riak_moss:user_key(), slice()) -> binary().
slice_key(User, {Start, _End}) ->
    %% logger may give us a slice that doesn't fill a whole archive
    %% slice, so realign it to make sure
    {_, RealEnd} = slice_containing(Start),
    iolist_to_binary([User,".",iso8601(RealEnd)]).

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
slices_filling(Start, End) when Start > End ->
    slices_filling(End, Start);
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

merge_stats(Stats, Acc) ->
    %% TODO: orddict conversion could be omitted if Stats was already
    %% an orddict
    orddict:merge(fun(_K, V1, V2) -> V1+V2 end,
                  Acc,
                  orddict:from_list(Stats)).

%% @doc Produce an ISO8601-compatible representation of the given time.
-spec iso8601(calendar:datetime()) -> binary().
iso8601({{Y,M,D},{H,I,S}}) ->
    iolist_to_binary(
      io_lib:format("~4..0b~2..0b~2..0bT~2..0b~2..0b~2..0bZ",
                    [Y, M, D, H, I, S])).

%% @doc Produce a datetime tuple from a ISO8601 string
-spec datetime(binary()|string()) -> {ok, calendar:datetime()} | error.
datetime(Binary) when is_binary(Binary) ->
    datetime(binary_to_list(Binary));
datetime(String) when is_list(String) ->
    case io_lib:fread("~4d~2d~2dT~2d~2d~2dZ", String) of
        {ok, [Y,M,D,H,I,S], _} ->
            {ok, {{Y,M,D},{H,I,S}}};
        %% TODO: match {more, _, _, RevList} to allow for shortened
        %% month-month/etc.
        _ ->
            error
    end.

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
            Usage = get_usage(User, Start, End, Riak),
            riakc_pb_socket:stop(Riak),
            {ok, Usage};
        {error, Reason} ->
            {error, Reason}
    end.

get_usage(User, Start, End, Riak) ->
    Slices = slices_filling(Start, End),
    UsageAdder = usage_adder(User, Riak),
    lists:foldl(UsageAdder, [], Slices).

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

-ifdef(TEST).
-ifdef(EQC).

iso8601_test() ->
    true = eqc:quickcheck(iso8601_roundtrip_prop()).

%% make sure that iso8601 roundtrips with datetime
iso8601_roundtrip_prop() ->
    %% iso8601 & datetime don't actually care if the date was valid,
    %% but writing a valid generator was fun
    ?FORALL(T, datetime_g(),
            is_binary(iso8601(T)) andalso
                {ok, T} == datetime(iso8601(T))).

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

datetime_test() ->
    true = eqc:quickcheck(datetime_invalid_prop()).

%% make sure that datetime correctly returns 'error' for invalid
%% iso8601 date strings
datetime_invalid_prop() ->
    ?FORALL(L, list(char()),
            case datetime(L) of
                {{_,_,_},{_,_,_}} ->
                    %% really, we never expect this to happen, given
                    %% that a random string is highly unlikely to be
                    %% valid iso8601, but just in case...
                    valid_iso8601(L);
                error ->
                    not valid_iso8601(L)
            end).

%% a string is considered valid iso8601 if it is of the form
%% ddddddddZddddddT, where d is a digit, Z is a 'Z' and T is a 'T'
valid_iso8601(L) ->
    length(L) == 4+2+2+1+2+2+2+1 andalso
        string:chr(L, $Z) == 4+2+2+1 andalso
        lists:all(fun is_digit/1, string:substr(L, 1, 8)) andalso
        string:chr(L, $T) == 16 andalso
        lists:all(fun is_digit/1, string:substr(L, 10, 15)).

is_digit(C) ->
    C >= $0 andalso C =< $9.

archive_period_test() ->
    true = eqc:quickcheck(archive_period_prop()).

%% make sure archive_period accepts valid periods, but bombs on
%% invalid ones
archive_period_prop() ->
    ?FORALL(I, oneof([valid_period_g(),
                      choose(-86500, 86500)]), % purposely outside day boundary
            begin
                application:set_env(riak_moss, access_archive_period, I),
                case archive_period() of
                    {ok, I} ->
                        valid_period(I);
                    {error, _Reason} ->
                        not valid_period(I)
                end
            end).

%% not exhaustive, but a good selection
valid_period_g() ->
    elements([1,10,100,
              2,4,8,16,32,
              3,9,27,
              6,18,54,
              12,24,48,96,
              60,600,3600,21600]). % 1min, 10min, 1hr, 6hr

%% a valid period is an integer 1-86400 that evenly divides 86400 (the
%% number of seconds in a day)
valid_period(I) ->
    is_integer(I) andalso
        I > 0 andalso I =< 86400
        andalso (86400 rem I) == 0.

slice_containing_test() ->
    true = eqc:quickcheck(slice_containing_prop()).

%% make sure that slice_containing returns a slice that is the length
%% of the archive period, and actually surrounds the given time
slice_containing_prop() ->
    ?FORALL({T, I}, {datetime_g(), valid_period_g()},
            begin
                application:set_env(riak_moss, access_archive_period, I),
                {S, E} = slice_containing(T),
                %% slice actually surrounds time
                S =< T andalso T =< E andalso
                    %% slice length is the configured period
                    I == datetime_diff(S, E) andalso
                    %% start of slice is N periods from start of day
                    0 == datetime_diff(S, {element(1, S),{0,0,0}}) rem I
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
                application:set_env(riak_moss, access_archive_period, I),
                {S1, E1} = slice_containing(T),
                {S2, E2} = next_slice({S1, E1}, I),
                S2 == E1 andalso I == datetime_diff(S2, E2)
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
                application:set_env(riak_moss, access_archive_period, I),
                T1 = calendar:gregorian_seconds_to_datetime(
                       I*M+R+calendar:datetime_to_gregorian_seconds(T0)),
                Slices = slices_filling(T0, T1),
                [Early, Late] = lists:sort([T0, T1]),
                {SF,EF} = hd(Slices),
                {SL,EL} = lists:last(Slices),
                ?WHENFAIL(
                   io:format("SF: ~p~nT0: ~p~nEF: ~p~n~n"
                             "SL: ~p~nT1: ~p~nEL: ~p~n~n"
                             "# slices: ~p~n",
                             [SF, T0, EF, SL, T1, EL, length(Slices)]),
                   conj([SF =< Early, Early =< EF,
                         SL =< Late, Late =< EL,
                         length(Slices) == datetime_diff(SF, EL) div I]))
            end).

%% Check that all elements in the list are boolean true, or 2-tuples
%% where the second element is true; return 'true' if this is the
%% case, or the input List if it is not.  This is a useful alternative
%% to many 'andalso' clauses at the end of an EQC test, as it's easier
%% to tell which clause failed.
conj(List) ->
    case lists:all(fun({_,B}) -> B == true; (B) -> B == true end, List) of
        true -> true;
        false -> List
    end.

make_object_test() ->
    true = eqc:quickcheck(make_object_prop()).

%% check that an archive object is in the right bucket, with a key
%% containing the end time and the username, with application/json as
%% the content type, and a value that is a JSON representation of the
%% sum of each access metric plus start and end times
make_object_prop() ->
    ?FORALL({UserKey, Accesses, Start, DRand},
            {user_key_g(), list(access_g()), datetime_g(), nat()},
            begin
                %% different periods should be irrelevant to this test
                application:set_env(riak_moss, access_archive_period, 10),

                %% Start and End are always within the same slice;
                %% make sure they are at test time as well
                {_, SliceEnd} = slice_containing(Start),
                StartSec = calendar:datetime_to_gregorian_seconds(Start),
                EndSec = calendar:datetime_to_gregorian_seconds(SliceEnd),
                End = calendar:gregorian_seconds_to_datetime(
                        (DRand rem (1+EndSec-StartSec))+StartSec),
                %% the '1+' above allows the choice of End==SliceEnd
                
                Obj = make_object(UserKey, Accesses, {Start, End}),
                
                Unique = lists:usort(
                           [ if is_atom(K)   -> atom_to_binary(K, latin1);
                                is_binary(K) -> K
                             end || {K, _V} <- lists:flatten(Accesses)]),
                {struct, MJ} = mochijson2:decode(
                                 riakc_obj:get_update_value(Obj)),

                ?WHENFAIL(
                io:format("keys: ~p~n", [MJ]),
                conj([{bucket, ?ACCESS_BUCKET == riakc_obj:bucket(Obj)},
                      {key_user, 0 /= string:str(
                                        binary_to_list(riakc_obj:key(Obj)),
                                        binary_to_list(UserKey))},
                      {key_time, 0 /= string:str(
                                        binary_to_list(riakc_obj:key(Obj)),
                                        binary_to_list(iso8601(SliceEnd)))},
                      {ctype, "application/json" ==
                           riakc_obj:md_ctype(
                             riakc_obj:get_update_metadata(Obj))},

                      {start_time, iso8601(Start) ==
                           proplists:get_value(<<"start_time">>, MJ)},
                      {end_time, iso8601(End) ==
                           proplists:get_value(<<"end_time">>, MJ)},
                      {keys, lists:all(fun({X,Y}) -> X == Y end,
                                       [{sum_access(K, Accesses),
                                         proplists:get_value(K, MJ)}
                                        || K <- Unique])}]))
            end).

%% create something vaguely user-key-ish; not actually a good
%% representation since user keys are 20-byte base64 strings, not any
%% random character, but this will hopefully find some odd corner
%% cases in case that key format changes
user_key_g() ->
    ?LET(L, ?SUCHTHAT(X, list(char()), X /= []), list_to_binary(L)).

%% create an access proplist
access_g() ->
    ?LET(L, list(access_field_g()), lists:ukeysort(1, L)).

%% create one access metric
access_field_g() ->
    {elements([bytes_in, bytes_out, <<"dummy1">>, dummy2]),
     oneof([int(), largeint()])}.

%% sum a given access metric K, given a list of accesses
sum_access(K, Accesses) ->
    lists:foldl(fun(Access, Sum) ->
                        Sum+proplists:get_value(
                              K, Access, proplists:get_value(
                                           binary_to_atom(K, latin1),
                                           Access, 0))
                end,
                0,
                Accesses).

-endif. % EQC
-endif. % TEST
