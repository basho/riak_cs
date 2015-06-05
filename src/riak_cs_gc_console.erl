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

%% @doc These functions are used by the riak-cs-gc command line script.

-module(riak_cs_gc_console).

-export([human_time/1]).

-export([batch/1,
         status/1,
         pause/1,
         resume/1,
         cancel/1,
         'set-interval'/1,
         'set-leeway'/1,
         'oldest-entries'/1]).

-define(SAFELY(Code, Description),
        try
            Code
        catch
            Type:Reason ->
                io:format("~s failed:~n  ~p:~p~n~p~n",
                          [Description, Type, Reason, erlang:get_stacktrace()]),
                error
        end).

-define(SCRIPT_NAME, "riak-cs-admin gc").

-define(SECONDS_PER_DAY, 86400).
-define(DAYS_FROM_0_TO_1970, 719528).

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Kick off a gc round, unless one is already
%% in progress.
batch(Opts) ->
    ?SAFELY(start_batch(parse_batch_opts(Opts)), "Starting garbage collection batch").

%% @doc Find out what the gc daemon is up to.
status(_Opts) ->
    ?SAFELY(get_status(), "Checking garbage collection status").

cancel(_Opts) ->
    ?SAFELY(cancel_batch(), "Canceling the garbage collection batch").

pause(_) ->
    output("Warning: Subcommand 'pause' will be removed in future version."),
    _ = riak_cs_gc_manager:set_interval(infinity),
    cancel([]).

resume(_) ->
    output("Warning: Subcommand 'resume' will be removed in future version."),
    set_interval(riak_cs_gc:gc_interval()).

'set-interval'(Opts) ->
    ?SAFELY(set_interval(parse_interval_opts(Opts)), "Setting the garbage collection interval").

'set-leeway'(Opts) ->
    ?SAFELY(set_leeway(parse_leeway_opts(Opts)), "Setting the garbage collection leeway time").

'oldest-entries'(_) ->
    Bags = riak_cs_mb_helper:bags(),
    ?SAFELY(begin
                [begin
                     {First, Dates} = riak_cs_gc_key_list:find_oldest_entries(BagId),
                     io:format("~s: ", [BagId]),
                     case First of
                         undefined ->
                             io:format("No GC key found.~n");
                         _ ->
                             io:format("First key is '~s'.~n", [First])
                     end,
                     [output(Date)|| Date <- Dates]
                 end || {BagId, _, _} <- Bags]
            end,
            "Finding oldest entries in GC bucket").

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_batch({ok, Options}) ->
    handle_batch_start(riak_cs_gc_manager:start_batch(Options));
start_batch({error, _}) ->
    getopt:usage(batch_options(), "riak-cs-admin gc", standard_io),
    output("Invalid argument").

get_status() ->
    handle_status(riak_cs_gc_manager:pp_status()).

cancel_batch() ->
    handle_batch_cancellation(riak_cs_gc_manager:cancel_batch()).

set_interval(undefined) ->
    output("Error: No interval value specified"),
    error;
set_interval({'EXIT', _}) ->
    output("Error: Invalid interval specified."),
    error;
set_interval(Interval) ->
    case riak_cs_gc_manager:set_interval(Interval) of
        ok ->
            output("The garbage collection interval was updated."),
            ok;
        {error, _} ->
            output("Error: Invalid interval specified."),
            error
    end.

set_leeway(undefined) ->
    output("Error: No leeway time value specified"),
    error;
set_leeway({'EXIT', _}) ->
    output("Error: Invalid leeway time specified."),
    error;
set_leeway(Leeway) ->
    case riak_cs_gc:set_leeway_seconds(Leeway) of
        ok ->
            output("The garbage collection leeway time was updated."),
            ok;
        {error, _} ->
            output("Error: Invalid leeway time specified."),
            error
    end.

handle_batch_start(ok) ->
    output("Garbage collection batch started."),
    ok;
handle_batch_start({error, running}) ->
    output("The garbage collection daemon is already running."),
    error.

handle_status({ok, {State, Details}}) ->
    _ = print_state(State),
    _ = print_details(Details),
    ok.

handle_batch_cancellation(ok) ->
    output("The garbage collection batch was canceled.");
handle_batch_cancellation({error, idle}) ->
    output("No garbage collection batch was running."),
    error.

output(Output) ->
    io:format(Output ++ "~n").

-spec print_state(riak_cs_gc_manager:statename()) -> ok.
print_state(idle) ->
    output("There is no garbage collection in progress");
print_state(running) ->
    output("A garbage collection batch is in progress").

%% @doc Pretty-print the status returned from the gc daemon.
print_details(Details) ->
    [ begin
          {HumanName, HumanValue} = human_detail(K, V),
          io:format("  ~s: ~s~n", [HumanName, HumanValue])
      end
      || {K, V} <- Details ].

human_detail(interval, infinity) ->
    {"The current garbage collection interval is", "infinity (i.e. gc is disabled)"};
human_detail(interval, Interval) when is_integer(Interval) ->
    {"The current garbage collection interval is", integer_to_list(Interval)};
human_detail(interval, _) ->
    {"The current garbage collection interval is", "undefined"};
human_detail(leeway, Leeway) when is_integer(Leeway) ->
    {"The current garbage collection leeway time is", integer_to_list(Leeway)};
human_detail(leeway, _) ->
    {"The current garbage collection leeway time is", "undefined"};
human_detail(next, undefined) ->
    {"Next run scheduled for", "undefined"};
human_detail(next, Time) ->
    {"Next run scheduled for", human_time(Time)};
human_detail(last, undefined) ->
    {"Last run started at", "undefined"};
human_detail(last, Time) ->
    {"Last run started at", human_time(Time)};
human_detail(current, Time) ->
    {"Current run started at", human_time(Time)};
human_detail(elapsed, Elapsed) ->
    {"Elapsed time of current run", integer_to_list(Elapsed)};
human_detail(files_deleted, Count) ->
    {"Files deleted in current run", integer_to_list(Count)};
human_detail(files_skipped, Count) ->
    {"Files skipped in current run", integer_to_list(Count)};
human_detail(files_left, Count) ->
    {"Files left in current run", integer_to_list(Count)};
human_detail(Name, Value) ->
    %% anything not to bomb if something was added
    {io_lib:format("~p", [Name]), io_lib:format("~p", [Value])}.

-spec human_time(non_neg_integer()|undefined) -> binary().
human_time(undefined) -> "unknown/never";
human_time(Seconds) ->
    Seconds0 = Seconds + ?DAYS_FROM_0_TO_1970*?SECONDS_PER_DAY,
    rts:iso8601(calendar:gregorian_seconds_to_datetime(Seconds0)).

parse_batch_opts([Leeway]) ->
    try
        case list_to_integer(Leeway) of
            LeewayInt when LeewayInt >= 0 ->
                {ok, [{leeway, LeewayInt}]};
            _O ->
                {error, negative_leeway}
        end
    catch T:E ->
            {error, {T, E}}
    end;
parse_batch_opts(Args) ->
    case getopt:parse(batch_options(), Args) of
        {ok, {Options, _}} -> {ok, convert(Options)};
        {error, _} = E -> E
    end.

batch_options() ->
    [{leeway, $l, "leeway", integer, "Leeway seconds"},
     {start,  $s, "start",  string,
      "Start time (iso8601 format, like 20130320T094500Z)"},
     {'end',  $e, "end",    string,
      "End time (iso8601 format, like 20130420T094500Z)"},
     {'max-workers', $c, "max-workers", integer, "Number of concurrent workers"}].

convert(Options) ->
    lists:map(fun({leeway, Leeway}) when Leeway >= 0 ->
                      {leeway, Leeway};
                 ({start, Start}) ->
                      {start, iso8601_to_epoch(Start)};
                 ({'end', End}) ->
                      {'end', iso8601_to_epoch(End)};
                 ({'max-workers', Concurrency}) when Concurrency > 0 ->
                      {'max-workers', Concurrency};
                 (BadArg) ->
                      error({bad_arg, BadArg})
              end, Options).

-spec iso8601_to_epoch(string()) -> non_neg_integer().
iso8601_to_epoch(S) ->
    {ok, Datetime} = rts:datetime(S),
    GregorianSeconds = calendar:datetime_to_gregorian_seconds(Datetime),
    GregorianSeconds - 62167219200. %% Unix Epoch in Gregorian second

parse_interval_opts([]) ->
    undefined;
parse_interval_opts(["infinity"]) ->
    infinity;
parse_interval_opts([Interval | _]) ->
    catch list_to_integer(Interval).

parse_leeway_opts([]) ->
    undefined;
parse_leeway_opts([Leeway | _]) ->
    catch list_to_integer(Leeway).
