%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

%% @doc Supervisor for the riak_moss application.

-module(riak_moss_sup).

-behaviour(supervisor).

%% Public API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc API for starting the supervisor.
-spec start_link() -> supervisor:startchild_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc supervisor callback.
-spec init([]) -> {ok, {{supervisor:strategy(),
                         integer(),
                         integer()},
                        [supervisor:child_spec()]}} | ignore.
init([]) ->
    case application:get_env(riak_moss, moss_ip) of
        {ok, Ip} ->
            ok;
        undefined ->
            Ip = "0.0.0.0"
    end,
    case application:get_env(riak_moss, moss_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 80
    end,
    case application:get_env(riak_moss, dispatch) of
        {ok, Dispatch} ->
            ok;
        undefined ->
            Dispatch =
                [{[], riak_moss_resource, [{mode, service}]},
                 {[bucket], riak_moss_resource, [{mode, bucket}]},
                 {[bucket, key], riak_moss_resource, [{mode, key}]}]
    end,
    Server =
        {riak_moss_riakc,
         {riak_moss_riakc, start_link, []},
         permanent, 5000, worker, dynamic},
    WebConfig = [
                 {dispatch, Dispatch},
                 {ip, Ip},
                 {port, Port},
                 {log_dir, "log"}],
    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [WebConfig]},
           permanent, 5000, worker, dynamic},

    Processes = [Server,Web],
    {ok, { {one_for_one, 10, 10}, Processes} }.
