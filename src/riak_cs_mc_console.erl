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

%% @doc These functions are used by the riak-cs-mc command line script.

-module(riak_cs_mc_console).

-export([status/1, input/1, refresh/1]).

-define(SAFELY(Code, Description),
        try
            Code
        catch
            Type:Reason ->
                io:format("~s failed:~n  ~p:~p~n",
                          [Description, Type, Reason]),
                error
        end).

-define(SCRIPT_NAME, "riak-cs-mc").

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Show multi-container weight information
status(_Opts) ->
    ?SAFELY(get_status(), "Checking multi-container status").

refresh(_Opts) ->
    ?SAFELY(handle_result(riak_cs_mc_server:refresh()),
            "Refresh multi-container weight").

input(Args) ->
    ?SAFELY(handle_result(riak_cs_mc_server:input(parse_input_args(Args))),
            "Updating the multi-container weight information").

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_status() ->
    handle_status(riak_cs_mc_server:status()).

handle_status({ok, Status}) ->
    %% TODO: More readable format
    io:format("~p~n", [Status]),
    ok;
handle_status({error, Reason}) ->
    io:format("Error: ~p~n", [Reason]).

parse_input_args([JsonString]) ->
    mochijson2:decode(JsonString).

handle_result(ok) ->
    ok;
handle_result({ok, Result}) ->
    io:format("~p~n", [Result]),
    ok;
handle_result({error, Reason}) ->
    io:format("Error: ~p~n", [Reason]),
    ok.
