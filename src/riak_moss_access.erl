%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Data format and lookup utilities for access logs (I/O usage
%% stats).
-module(riak_moss_access).

-export([
         archive_period/0,
         log_flush_interval/0,
         max_flush_size/0,
         make_object/3,
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

-define(NODEKEY, <<"MossNode">>).

%% @doc Retrieve the number of seconds that should elapse between
%% archivings of access stats.  This setting is controlled by the
%% `access_archive_period' environment variable of the `riak_moss'
%% application.
-spec archive_period() -> {ok, integer()}|{error, term()}.
archive_period() ->
    case application:get_env(riak_moss, access_archive_period) of
        {ok, AP} when is_integer(AP), AP > 0 ->
            {ok, AP};
        _ ->
            {error, "riak_moss:access_archive_period was not an integer"}
    end.

%% @doc Retrieve the number of seconds that should elapse between
%% flushes of access stats.  This setting is controlled by the
%% `access_log_flush_interval' environment variable of the `riak_moss'
%% application.
-spec log_flush_interval() -> {ok, integer()}|{error, term()}.
log_flush_interval() ->
    case application:get_env(riak_moss, access_log_flush_factor) of
        {ok, AF} when is_integer(AF), AF > 0 ->
            case archive_period() of
                {ok, AP} ->
                    case AP rem AF of
                        0 ->
                            {ok, AP div AF};
                        _ ->
                            {error, "riak_moss:access_log_flush_interval"
                                    " does not evenly divide"
                                    " riak_moss:access_archive_period"}
                    end;
                APError ->
                    APError
            end;
        _ ->
            {error, "riak_moss:access_log_flush_interval was not an integer"}
    end.

%% @doc Retrieve the maximum number of records that should be added to
%% the log before the log is automatically archived.  This setting is
%% controlled by the `access_log_flush_size' environment variable of
%% the `riak_moss' application.
-spec max_flush_size() -> {ok, integer()}|{error, term()}.
max_flush_size() ->
    case application:get_env(riak_moss, access_log_flush_size) of
        {ok, AP} when is_integer(AP), AP > 0 ->
            {ok, AP};
        _ ->
            {error, "riak_moss:access_log_flush_size was not a positive integer"}
    end.

%% @doc Create a Riak object for storing a user/slice's access data.
%% The list of stats (`Accesses') must contain a list of proplists.
%% The keys of the proplist must be either atoms or binaries, to be
%% encoded as JSON keys.  The values of the proplists must be numbers,
%% as the values for each key will be summed in the stored object.
-spec make_object(iodata(),
                  [[{atom()|binary(), number()}]],
                  slice())
         -> riakc_obj:riakc_obj().
make_object(User, Accesses, {Start, End}) ->
    {ok, Period} = archive_period(),
    Aggregate = aggregate_accesses(Accesses),
    rts:new_sample(?ACCESS_BUCKET, User, Start, End, Period,
                   [{?NODEKEY, node()}|Aggregate]).

aggregate_accesses(Accesses) ->
    Merged = lists:foldl(fun merge_ops/2, [], Accesses),
    %% now mochijson-ify
    [ {OpName, {struct, Stats}} || {OpName, Stats} <- Merged ].

merge_ops({OpName, Stats}, Acc) ->
    case lists:keytake(OpName, 1, Acc) of
        {value, {OpName, Existing}, RemAcc} ->
            [{OpName, merge_stats(Stats, Existing)}|RemAcc];
        false ->
            [{OpName, Stats}|Acc]
    end.

%% `Stats' had better be an orddict
merge_stats(Stats, Acc) ->
    orddict:merge(fun(_K, V1, V2) -> V1+V2 end, Acc, Stats).

%% @doc Produce a usage compilation for the given `User' between
%% `Start' and `End' times, inclusive.  The result is an orddict in
%% which the keys are MOSS node names.  The value for each key is a
%% list of samples.  Each sample is an orddict full of metrics.
-spec get_usage(pid(),
                term(), %% TODO: riak_moss:user_key() type doesn't exist
                calendar:datetime(),
                calendar:datetime()) ->
         {Usage::orddict:orddict(), Errors::[{slice(), term()}]}.
get_usage(Riak, User, Start, End) ->
    {ok, Period} = archive_period(),
    {Usage, Errors} = rts:find_samples(Riak, ?ACCESS_BUCKET, User,
                                       Start, End, Period),
    {group_by_node(Usage), Errors}.

group_by_node(Samples) ->
    lists:foldl(fun(Sample, Acc) ->
                        {value, {?NODEKEY, Node}, Other} =
                            lists:keytake(?NODEKEY, 1, Sample),
                        orddict:append(Node, Other, Acc)
                end,
                orddict:new(),
                Samples).

-ifdef(TEST).
-ifdef(EQC).

archive_period_test() ->
    true = eqc:quickcheck(archive_period_prop()).

%% make sure archive_period accepts valid periods, but bombs on
%% invalid ones
archive_period_prop() ->
    ?FORALL(I, oneof([rts:valid_period_g(),
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

%% a valid period is an integer 1-86400 that evenly divides 86400 (the
%% number of seconds in a day)
valid_period(I) ->
    is_integer(I) andalso I > 0.

make_object_test() ->
    true = eqc:quickcheck(make_object_prop()).

%% check that an archive object is in the right bucket, with a key
%% containing the end time and the username, with application/json as
%% the content type, and a value that is a JSON representation of the
%% sum of each access metric plus start and end times
make_object_prop() ->
    ?FORALL(Accesses,
            list({op_g(), access_g()}),
            begin
                application:set_env(riak_moss, access_archive_period, 60000),

                %% trust rts:make_object_prop to check all of the
                %% bucket/key/time/etc. properties
                User = <<"AAABBBCCCDDDEEEFFF">>,
                T0 = {{2012,02,16},{10,44,00}},
                T1 = {{2012,02,16},{10,45,00}},

                Obj = make_object(User, Accesses, {T0, T1}),
                
                Unique = lists:usort(
                           [ if is_atom(K)   -> atom_to_binary(K, latin1);
                                is_binary(K) -> K
                             end || {K, _V} <- lists:flatten(Accesses)]),
                {struct, MJ} = mochijson2:decode(
                                 riakc_obj:get_update_value(Obj)),

                Paired = [{{struct, sum_access(K, Accesses)},
                           proplists:get_value(K, MJ)}
                          || K <- Unique],

                ?WHENFAIL(
                   io:format(user, "keys: ~p~nAccesses: ~p~nPaired: ~p~n",
                             [MJ, Accesses, Paired]),
                   [] == [ {X, Y} || {X, Y} <- Paired, X =/= Y ])
            end).

%% create something vaguely user-key-ish; not actually a good
%% representation since user keys are 20-byte base64 strings, not any
%% random character, but this will hopefully find some odd corner
%% cases in case that key format changes
user_key_g() ->
    ?LET(L, ?SUCHTHAT(X, list(char()), X /= []), list_to_binary(L)).

op_g() ->
    elements([<<"BucketRead">>, <<"BucketCreate">>,
              <<"KeyRead">>, <<"KeyReadACL">>]).

%% create an access proplist
access_g() ->
    ?LET(L, list(access_field_g()), lists:ukeysort(1, L)).

%% create one access metric
access_field_g() ->
    {elements([<<"Count">>, <<"SystemErrorCount">>,
               <<"BytesOut">>, <<"BytesOutIncomplete">>]),
     oneof([int(), largeint()])}.

%% sum a given access metric K, given a list of accesses
sum_access(K, Accesses) ->
    lists:foldl(fun({MK, Access}, Sum) when K == MK ->
                        orddict:merge(fun(_K, V1, V2) -> V1+V2 end,
                                      Access, Sum);
                   (_, Sum) -> Sum
                end,
                [],
                Accesses).

-endif. % EQC
-endif. % TEST
