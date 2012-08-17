%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_wm_error_handler).
-export([render_error/3]).

-include("riak_moss.hrl").

render_error(500, Req, Reason) ->
    dt_entry(<<"render_error">>),
    {ok, ReqState} = Req:add_response_header("Content-Type", "text/html"),
    {Path,_} = Req:path(),
    error_logger:error_msg("webmachine error: path=~p~n~p~n", [Path, Reason]),

    ErrorOne = <<"<html><head><title>500 Internal Server Error</title>">>,
    ErrorTwo = <<"</head><body><h1>Internal Server Error</h1>">>,
    ErrorThree = <<"The server encountered an error while processing ">>,
    ErrorFour = <<"this request</body></html>">>,
    IOList = [ErrorOne, ErrorTwo, ErrorThree, ErrorFour],
    {erlang:iolist_to_binary(IOList), ReqState};
render_error(_Code, Req, _Reason) ->
    dt_entry(<<"render_error">>),
    Req:response_body().

dt_entry(Name) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, Name, []).
    
