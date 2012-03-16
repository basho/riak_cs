%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc These functions are used by the riak-cs-storage command line script.
-module(riak_cs_storage_console).

-export([
         batch/1
        ]).

%% @doc Kick off a batch of storage calculation, unless one is already
%% in progress.
batch(_Opts) ->
    try
        case riak_moss_storage_d:start_batch() of
            ok ->
                io:format("Batch storage calculation started.~n"),
                ok;
            {error, already_calculating} ->
                io:format("Error: A batch storage calculation is already"
                          " in progress.~n"),
                error
        end
    catch
        Type:Reason ->
            io:format("Starting batch storage calculation failed:~n"
                      "  ~p:~p~n", [Type, Reason]),
            error
    end.
