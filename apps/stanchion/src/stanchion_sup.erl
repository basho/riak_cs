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

%% @doc Supervisor for the `stanchion' application.

-module(stanchion_sup).

-behaviour(supervisor).

%% Public API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc API for starting the supervisor.
-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc supervisor callback.
-spec init([]) -> {ok, {supervisor:sup_flags(),
                        [supervisor:child_spec()]}} | ignore.
init([]) ->
    {Ip, Port} = case application:get_env(stanchion, host) of
        {ok, {_, _} = HostPort} -> HostPort;
        undefined -> {"0.0.0.0", 80}
    end,

    stanchion_stats:init(),
    %% Hide any bags from user-facing parts.
    case application:get_env(stanchion, supercluster_members) of
        undefined -> ok;
        {ok, Bags} -> application:set_env(stanchion, bags, Bags)
    end,

    %% Create child specifications
    WebConfig1 = [
                  {dispatch, stanchion_web:dispatch_table()},
                  {ip, Ip},
                  {port, Port},
                  {nodelay, true},
                  {log_dir, "log"},
                  %% {rewrite_module, stanchion_wm_rewrite},
                  {error_handler, stanchion_wm_error_handler}
                 ],
    WebConfig =
        case application:get_env(stanchion, ssl) of
            {ok, SSLOpts} ->
                WebConfig1 ++ [{ssl, true},
                               {ssl_opts, SSLOpts}];
            undefined ->
                WebConfig1
        end,
    Web = #{id => webmachine_mochiweb,
            start => {webmachine_mochiweb, start, [WebConfig]},
            restart => permanent,
            shutdown => 5000,
            modules => dynamic},
    ServerSup = #{id => stanchion_server_sup,
                  start => {stanchion_server_sup, start_link, []},
                  restart => permanent,
                  shutdown => 5000,
                  modules => dynamic},
    Processes = [ServerSup, Web],
    {ok, { #{strategy => one_for_one,
             intensity => 10,
             period => 10}, Processes} }.
