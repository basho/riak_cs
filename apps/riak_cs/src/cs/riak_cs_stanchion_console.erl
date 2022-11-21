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

%% @doc These functions are used by the riak-cs-stanchion command line script.

-module(riak_cs_stanchion_console).

-export([
         show/1
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

-define(SCRIPT_NAME, "riak-cs-admin stanchion").
-include("riak_cs.hrl").

%%%===================================================================
%%% Public API
%%%===================================================================

show([]) ->
    ?SAFELY(begin
                {Host, Port, SSL} = riak_cs_utils:stanchion_data(),
                Scheme = case SSL of
                             true -> "https://";
                             false -> "http://"
                         end,
                io:format("Current Stanchion Address: ~s~s:~s~n",
                          [Scheme, Host, integer_to_list(Port)])
            end, "Retrieving Stanchion info").
