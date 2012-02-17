%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc These functions are used by the riak_moss command line script.
-module(riak_moss_usage_console).

-export([
         flush_access/1
        ]).

-define(DEFAULT_FLUSH_RETRIES, 10).
-define(RETRY_TIMEOUT, 5000).

%% @doc Roll the current access log over to the archiver, and then
%% wait until the archiver has stored it in riak.
flush_access(Opts) ->
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
                    Result = riak_moss_access_logger:flush(
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
    wait_for_archiver(N, N).

wait_for_archiver(N, _) when N < 1 ->
    io:format("~nFlushing archiver timed out.~n"),
    error;
wait_for_archiver(N, Max) ->
    case riak_moss_access_archiver:status(?RETRY_TIMEOUT) of
        {ok, 0} ->
            io:format("~nAll access logs were flushed.~n"),
            ok;
        {ok, Left} ->
            %% first correct for previous status requests, though this
            %% doesn't really work across consecutive flush runs
            RealLeft = Left-(Max-N),
            io:format("~nWaiting for ~b more archive~s to flush.~n",
                      [RealLeft, if RealLeft == 1 -> ""; true -> "s" end]),
            wait_for_archiver(N-1, Max);
        Error ->
            io:format("Flushing archives failed:~n  ~p~n", [Error]),
            error
    end.
