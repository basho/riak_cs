%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_wm_error_handler).
-export([render_error/3]).

render_error(500, Req, Reason) ->
    {ok, ReqState} = Req:add_response_header("Content-Type", "text/html"),
    {Path,_} = Req:path(),
    error_logger:error_msg("webmachine error: path=~p~n~p~n", [Path, Reason]),
    STString = io_lib:format("~p", [Reason]),
    ErrorStart = "<html><head><title>500 Internal Server Error</title></head><body><h1>Internal Server Error</h1>The server encountered an error while processing this request:<br><pre>",
    ErrorEnd = "</pre><P><HR><ADDRESS>mochiweb+webmachine web server</ADDRESS></body></html>",
    ErrorIOList = [ErrorStart,STString,ErrorEnd],
    {erlang:iolist_to_binary(ErrorIOList), ReqState};
render_error(_Code, Req, _Reason) ->
    Req:response_body().
