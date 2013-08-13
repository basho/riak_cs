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

-export([
         batch/1,
         status/1,
         pause/1,
         resume/1,
         cancel/1,
         'set-interval'/1
        ]).

-define(SAFELY(Code, Description),
        try
            Code
        catch
            Type:Reason ->
                io:format("~s failed:~n  ~p:~p~n",
                          [Description, Type, Reason]),
                error
        end).

-define(SCRIPT_NAME, "riak-cs-gc").

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Kick off a gc round, unless one is already
%% in progress.
batch(_Opts) ->
    ?SAFELY(start_batch(), "Starting garbage collection batch").

%% @doc Find out what the gc daemon is up to.
status(_Opts) ->
    ?SAFELY(get_status(), "Checking garbage collection status").

cancel(_Opts) ->
    ?SAFELY(cancel_batch(), "Canceling the garbage collection batch").

pause(_Opts) ->
    ?SAFELY(pause(), "Pausing the garbage collection daemon").

resume(_Opts) ->
    ?SAFELY(resume(), "Resuming the garbage collection daemon").

'set-interval'(Opts) ->
    ?SAFELY(set_interval(parse_interval_opts(Opts)), "Setting the garbage collection interval").

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_batch() ->
    handle_batch_start(riak_cs_gc_d:manual_batch([])).

get_status() ->
    handle_status(riak_cs_gc_d:status()).

cancel_batch() ->
    handle_batch_cancellation(riak_cs_gc_d:cancel_batch()).

pause() ->
    handle_pause(riak_cs_gc_d:pause()).

resume() ->
    handle_resumption(riak_cs_gc_d:resume()).

set_interval(undefined) ->
    output("Error: No interval value specified");
set_interval(infinity) ->
    output("The garbage collection interval was updated."),
    riak_cs_gc_d:set_interval(infinity);
set_interval(Interval) when is_integer(Interval) ->
    output("The garbage collection interval was updated."),
    riak_cs_gc_d:set_interval(Interval);
set_interval({'EXIT', _}) ->
    output("Error: Invalid interval specified.").


handle_batch_start(ok) ->
    output("Garbage collection batch started."),
    ok;
handle_batch_start({error, already_deleting}) ->
    output("Error: A garbage collection batch"
           " is already in progress."),
    error.

handle_status({ok, {State, Details}}) ->
    print_status(State, Details);
handle_status(_) ->
    ok.

handle_batch_cancellation(ok) ->
    output("The garbage collection batch was canceled.");
handle_batch_cancellation({error, no_batch}) ->
    output("No garbage collection batch was running.").

handle_pause(ok) ->
    output("The garbage collection daemon was paused.");
handle_pause({error, already_paused}) ->
    output("The garbage collection daemon was already paused.").

handle_resumption(ok) ->
    output("The garbage collection daemon was resumed.");
handle_resumption({error, not_paused}) ->
    output("The garbage collection daemon was not paused.").

output(Output) ->
    io:format(Output ++ "~n").

print_status(State, Details) ->
    print_state(State),
    print_details(Details).

print_state(idle) ->
    output("There is no garbage collection in progress");
print_state(fetching_next_fileset) ->
    output("A garbage collection batch is in progress");
print_state(initiating_file_delete) ->
    output("A garbage collection batch is in progress");
print_state(waiting_file_delete) ->
    output("A garbage collection batch is in progress");
print_state(paused) ->
    output("A garbage collection batch is currently paused").

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

human_time(undefined) -> "unknown/never";
human_time(Seconds) when is_integer(Seconds) ->
    human_time(calendar:gregorian_seconds_to_datetime(Seconds));
human_time(Datetime)  -> rts:iso8601(Datetime).

parse_interval_opts([]) ->
    undefined;
parse_interval_opts(["infinity"]) ->
    infinity;
parse_interval_opts([Interval | _]) ->
    catch list_to_integer(Interval).
