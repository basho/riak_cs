%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

%% @doc These functions are used by the riak-cs-storage command line script.

-module(riak_cs_storage_console).

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

-define(SCRIPT_NAME, "riak-cs-admin storage").
-define(BATCH_OPTIONS, [{recalc, $r, "recalc", boolean,
                         "recalculate all users for this period"
                         " (default=false)"},
                        {detailed, undefined, "detailed", boolean,
                         "calculate detailed bucket usage summary"
                         " (default=false)"}]).

%% @doc Kick off a batch of storage calculation, unless one is already
%% in progress.
batch(Opts) ->
    ?SAFELY(
       case getopt:parse(?BATCH_OPTIONS, Opts) of
           {ok, {Parsed, _Extra}} ->
               case riak_cs_storage_d:start_batch(Parsed) of
                   ok ->
                       io:format("Batch storage calculation started.~n"),
                       ok;
                   {error, already_calculating} ->
                       io:format("Error: A batch storage calculation"
                                 " is already in progress.~n"),
                       error
               end;
           {error, {OptReason, OptMessage}} ->
               io:format("Error: ~p: ~p~n", [OptReason, OptMessage]),
               getopt:usage(?BATCH_OPTIONS, lists:flatten([?SCRIPT_NAME, " batch"])),
               error
       end,
       "Starting batch storage calculation").

%% @doc Find out what the storage daemon is up to.
status(_Opts) ->
    ?SAFELY(
       begin
           {ok, {State, Details}} = riak_cs_storage_d:status(),
           _ = print_state(State),
           _ = print_details(Details),
           ok
       end,
       "Checking storage calculation status").

print_state(idle) ->
    io:format("There is no storage calculation in progress~n");
print_state(calculating) ->
    io:format("A storage calculation is in progress~n");
print_state(paused) ->
    io:format("A storage calculation is current paused~n").

cancel(_Opts) ->
    ?SAFELY(
       case riak_cs_storage_d:cancel_batch() of
           ok ->
               io:format("The calculation was canceled.~n");
           {error, no_batch} ->
               io:format("No storage calculation was running.~n")
       end,
       "Canceling the storage calculation").

pause(_Opts) ->
    ?SAFELY(
       case riak_cs_storage_d:pause_batch() of
           ok ->
               io:format("The calculation was paused.~n");
           {error, no_batch} ->
               io:format("No storage calculation was running.~n")
       end,
       "Pausing the storage calculation").

resume(_Opts) ->
    ?SAFELY(
       case riak_cs_storage_d:resume_batch() of
           ok ->
               io:format("The calculation was resumed.~n");
           {error, no_batch} ->
               io:format("No calcluation was running.~n")
       end,
       "Resuming the storage calcluation").

%% @doc Pretty-print the status returned from the storage daemon.
print_details(Details) ->
    [ begin
          {HumanName, HumanValue} = human_detail(K, V),
          io:format("  ~s: ~s~n", [HumanName, HumanValue])
      end
      || {K, V} <- Details ].

human_detail(schedule, Schedule) ->
    Human = case Schedule of
                [] -> "none defined";
                _ ->
                    %% convert the list of tuples to a comma-separated
                    %% stringy thing
                    string:join([io_lib:format("~2..0b~2..0b", [H, M])
                                 || {H, M} <- Schedule],
                                ",")
            end,
    {"Schedule", Human};
human_detail(last, Time) ->
    {"Last run started at", human_time(Time)};
human_detail(next, Time) ->
    {"Next run scheduled for", human_time(Time)};
human_detail(current, Time) ->
    {"Current run started at", human_time(Time)};
human_detail(elapsed, Elapsed) ->
    {"Elapsed time of current run", integer_to_list(Elapsed)};
human_detail(users_done, Count) ->
    {"Users completed in current run", integer_to_list(Count)};
human_detail(users_skipped, Count) ->
    {"Users skipped in current run", integer_to_list(Count)};
human_detail(users_left, Count) ->
    {"Users left in current run", integer_to_list(Count)};
human_detail(Name, Value) ->
    %% anything not to bomb if something was added
    {io_lib:format("~p", [Name]), io_lib:format("~p", [Value])}.

human_time(undefined) -> "unknown/never";
human_time(Datetime)  -> rts:iso8601(Datetime).
