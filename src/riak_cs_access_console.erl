%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

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
                    Result = riak_cs_access_logger:flush(
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
wait_for_archiver(N) when N < 1 ->
    io:format("Flushing archiver timed out.~n"),
    error;
wait_for_archiver(N) ->
    case riak_cs_access_archiver:status(?RETRY_TIMEOUT) of
        {ok, idle, _Props} ->
            io:format("All access logs were flushed.~n"),
            ok;
        {ok, archiving, Props} ->
            case lists:keyfind(slice, 1, Props) of
                {slice, {Start, End}} ->
                    io:format("Currently archiving ~s-~s~n",
                              [rts:iso8601(Start), rts:iso8601(End)]);
                false ->
                    ok
            end,
            case lists:keyfind(backlog, 1, Props) of
                {backlog, Count} ->
                    io:format("~b more archives to flush~n", [Count]);
                false ->
                    ok
            end,
            %% give the archiver some time to do its thing
            timer:sleep(?RETRY_TIMEOUT),
            wait_for_archiver(N-1);
        {ok, busy, Props} ->
            case lists:keyfind(message_queue_len, 1, Props) of
                {message_queue_len, Length} ->
                    io:format("Archiver is busy,"
                              " with ~b message~s in its inbox.~n",
                              [Length, if Length == 1 -> ""; true -> "s" end]);
                false ->
                    ok
            end,
            %% busy response means the gen_fsm call timed out, so we
            %% don't need to re-delay this request
            wait_for_archiver(N-1);
        Error ->
            io:format("Flushing archives failed:~n  ~p~n", [Error]),
            error
    end.
