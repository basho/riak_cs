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

%% @doc Kick off a gc round, unless one is already
%% in progress.
batch(_Opts) ->
    ?SAFELY(
       case riak_moss_gc_d:manual_batch([]) of
           ok ->
               io:format("Garbage collection batch started.~n"),
               ok;
           {error, already_deleting} ->
               io:format("Error: A garbage collection batch"
                         " is already in progress.~n"),
               error
       end,
       "Starting garbage collection batch").

%% @doc Find out what the gc daemon is up to.
status(_Opts) ->
    ?SAFELY(
       begin
           {ok, {State, Details}} = riak_moss_gc_d:status(),
           print_state(State),
           print_details(Details)
       end,
       "Checking garbage collection status").

print_state(idle) ->
    io:format("There is no garbage collection in progress~n");
print_state(calculating) ->
    io:format("A garbage collection batch is in progress~n");
print_state(paused) ->
    io:format("A garbage collection batch is currently paused~n").

cancel(_Opts) ->
    ?SAFELY(
       case riak_moss_gc_d:cancel_batch() of
           ok ->
               io:format("The garbage collection batch was canceled.~n");
           {error, no_batch} ->
               io:format("No garbage collection batch was running.~n")
       end,
       "Canceling the garbage collection batch").

pause(_Opts) ->
    ?SAFELY(
       case riak_moss_gc_d:pause_batch() of
           ok ->
               io:format("The garbage collection batch was paused.~n");
           {error, no_batch} ->
               io:format("No garbage collection batch was running.~n")
       end,
       "Pausing the garbage collection batch").

resume(_Opts) ->
    ?SAFELY(
       case riak_moss_gc_d:resume_batch() of
           ok ->
               io:format("The garbage collection batch was resumed.~n");
           {error, no_batch} ->
               io:format("No garbage collection batch was running.~n")
       end,
       "Resuming the garbage collection batch").

%% @doc Pretty-print the status returned from the gc daemon.
print_details(Details) ->
    [ begin
          {HumanName, HumanValue} = human_detail(K, V),
          io:format("  ~s: ~s~n", [HumanName, HumanValue])
      end
      || {K, V} <- Details ].

human_detail(interval, Interval) ->
    {"The current garbage collection interval is", Interval};
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
human_time(Datetime)  -> rts:iso8601(Datetime).
