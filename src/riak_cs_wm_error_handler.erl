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

-module(riak_cs_wm_error_handler).
-export([render_error/3]).

-include("riak_cs.hrl").

render_error(500, Req, Reason) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"render_error">>),
    {ok, ReqState} = Req:add_response_header("Content-Type", "text/html"),
    {Path,_} = Req:path(),
    error_logger:error_msg("webmachine error: path=~p~n~p~n~p\n", [Path, Reason, erlang:get_stacktrace()]),

    ErrorOne = <<"<html><head><title>500 Internal Server Error</title>">>,
    ErrorTwo = <<"</head><body><h1>Internal Server Error</h1>">>,
    ErrorThree = <<"The server encountered an error while processing ">>,
    ErrorFour = <<"this request</body></html>">>,
    IOList = [ErrorOne, ErrorTwo, ErrorThree, ErrorFour],
    {erlang:iolist_to_binary(IOList), ReqState};
render_error(_Code, Req, _Reason) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"render_error">>),
    Req:response_body().
