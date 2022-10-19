%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Callbacks for the stanchion application.

-module(stanchion_console).

-export([version/1,
	 status/1]).

%% ===================================================================
%% Public API
%% ===================================================================

version(_) ->
    {ok, Vsn} = application:get_env(stanchion, stanchion_version),
    io:format("version : ~p~n", [Vsn]),
    ok.

status(_) ->
    Stats = stanchion_stats:get_stats(),
    [io:format("~s : ~p~n", [Key,Value]) || {Key,Value} <- Stats],
    ok.
