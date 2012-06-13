%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc These functions are used by the riak-cs-gc command line script.
-module(riak_cs_gc_console).

-export([
         batch/1,
         status/1,
         pause/1,
         resume/1,
         cancel/1
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
    ?SAFELY(pause_batch(), "Pausing the garbage collection batch").

resume(_Opts) ->
    ?SAFELY(resume_batch(), "Resuming the garbage collection batch").

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_batch() ->
    handle_batch_start(riak_cs_gc_d:manual_batch([])).

get_status() ->
    handle_status(riak_cs_gc_d:status()).

pause_batch() ->
    handle_batch_pause(riak_cs_gc_d:pause_batch()).

cancel_batch() ->
    handle_batch_cancellation(riak_cs_gc_d:cancel_batch()).

resume_batch() ->
    handle_batch_resumption(riak_cs_gc_d:resume_batch()).

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

handle_batch_pause(ok) ->
    output("The garbage collection batch was paused.");
handle_batch_pause({error, no_batch}) ->
    output("No garbage collection batch was running.").

handle_batch_cancellation(ok) ->
    output("The garbage collection batch was canceled.");
handle_batch_cancellation({error, no_batch}) ->
    output("No garbage collection batch was running.").

handle_batch_resumption(ok) ->
    output("The garbage collection batch was resumed.");
handle_batch_resumption({error, no_batch}) ->
    output("No garbage collection batch was running.").

output(Output) ->
    io:format(Output ++ "~n").

print_status(State, Details) ->
    print_state(State),
    print_details(Details).

print_state(idle) ->
    output("There is no garbage collection in progress");
print_state(fetching_next_filest) ->
    output("A garbage collection batch is in progress");
print_state(initiating_file_delete) ->
    output("A garbage collection batch is in progress");
print_state(waiting_filel_delete) ->
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

human_detail(interval, Interval) ->
    {"The current garbage collection interval is", integer_to_list(Interval)};
human_detail(next, Time) ->
    {"Next run scheduled for", human_time(Time)};
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
