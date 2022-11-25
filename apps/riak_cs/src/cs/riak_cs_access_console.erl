%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

%% @doc These functions are used by the riak-cs-access command line script.
-module(riak_cs_access_console).

-export([
         flush/1
        ]).

-define(DEFAULT_FLUSH_RETRIES, 10).
-define(RETRY_TIMEOUT, 5000).

%% @doc Roll the current access log over to the archiver, and then
%% wait until the archiver has stored it in riak.
flush(Opts) ->
    try
        Retries = flush_retries(Opts),
        io:format("Adding current log to archive queue...~n"),
        case wait_for_logger(Retries) of
            ok ->
                io:format("Waiting for archiver to finish...~n"),
                wait_for_archiver(Retries);
            Error ->
                Error
        end
    catch
        Type:Reason ->
            io:format("Flushing access failed:~n  ~p:~p~n", [Type, Reason]),
            error
    end.

%% @doc Check the -w command line flag for a non-default number of
%% attempts to get the logger and archiver to flush.  Retries are used
%% instead of one long timeout in order to provide an opportunity to
%% print "still working" dots to the console.
flush_retries(Opts) ->
    case lists:dropwhile(fun(E) -> E /= "-w" end, Opts) of
        ["-w", RetriesStr|_] ->
            case catch list_to_integer(RetriesStr) of
                I when is_integer(I) -> I;
                _ -> ?DEFAULT_FLUSH_RETRIES
            end;
        _ ->
            ?DEFAULT_FLUSH_RETRIES
    end.

%% @doc Wait for the logger to confirm our flush message.
wait_for_logger(Retries) ->
    OldTrap = erlang:process_flag(trap_exit, true),
    Self = self(),
    Ref = erlang:make_ref(),
    Pid = erlang:spawn_link(
            fun() ->
                    Result = riak_cs_access_log_handler:flush(
                               Retries*?RETRY_TIMEOUT),
                    Self ! {Ref, Result}
            end),
    case wait_for_logger(Retries, Ref, Pid) of
        ok ->
            erlang:process_flag(trap_exit, OldTrap),
            ok;
        timeout ->
            io:format("Flushing current log timed out.~n"),
            error;
        Error ->
            io:format("Flushing current log failed:~n  ~p~n", [Error]),
            error
    end.

wait_for_logger(N, _, _) when N < 1 ->
    timeout;
wait_for_logger(N, Ref, Pid) ->
    receive
        {Ref, Result} ->
            Result;
        {'EXIT', Pid, Reason} ->
            Reason
    after ?RETRY_TIMEOUT ->
            io:format("."), %% waiting indicator
            wait_for_logger(N-1, Ref, Pid)
    end.

%% @doc Wait for the archiver to say it's idle.  The assumption is
%% that the archiver will have received the logger's roll before it
%% receives our status request, so it shouldn't say it's idle until it
%% has archived that roll.
wait_for_archiver(N) ->
    wait_for_archiver(riak_cs_access_archiver_manager:status(?RETRY_TIMEOUT), N).

wait_for_archiver(_, N) when N < 1 ->
    io:format("Flushing archiver timed out.~n"),
    error;
wait_for_archiver({ok, Props}, N) ->
    Backlog = lists:keyfind(backlog, 1, Props),
    Workers = lists:keyfind(workers, 1, Props),
    case {Backlog, Workers} of
        {{backlog, 0}, {workers, []}} ->
            io:format("All access logs were flushed.~n"),
            ok;
        {{backlog, Count}, {workers, WorkerPids}} ->
            _ = archivers_status(WorkerPids),
            io:format("~b more archives to flush~n", [Count]),

            %% give the archiver some time to do its thing
            timer:sleep(?RETRY_TIMEOUT),
            wait_for_archiver(N-1)
    end;
wait_for_archiver({error, {busy, QLength}}, N) ->
    io:format("Archive manager is busy,"
              " with ~b message~s in its inbox.~n",
              [QLength, if QLength == 1 -> ""; true -> "s" end]),
    %% busy response means the gen_server call timed out, so we
    %% don't need to re-delay this request
    wait_for_archiver(N-1);
wait_for_archiver({error, Reason}, _) ->
    io:format("Flushing archives failed:~n  ~p~n", [Reason]),
    error.

archivers_status(Pids)  ->
    [archiver_status(
       riak_cs_access_archiver:status(Pid, ?RETRY_TIMEOUT)) ||
        Pid <- Pids].

archiver_status({ok, idle, _}) ->
    ok;
archiver_status({ok, archiving, []}) ->
    ok;
archiver_status({ok, archiving, Props}) ->
    case lists:keyfind(slice, 1, Props) of
        {slice, {Start, End}} ->
            io:format("Currently archiving ~s-~s~n",
                      [rts:iso8601(Start), rts:iso8601(End)]);
        false ->
            ok
    end;
archiver_status({ok, busy, QLength}) ->
    io:format("Archiver is busy,"
              " with ~b message~s in its inbox.~n",
              [QLength, if QLength == 1 -> ""; true -> "s" end]),
    ok;
archiver_status({error, _}) ->
    ok.
