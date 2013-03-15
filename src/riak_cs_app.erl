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

%% @doc Callbacks for the riak_cs application.

-module(riak_cs_app).

-behaviour(application).

%% application API
-export([start/2,
         stop/1]).


-type start_type() :: normal | {takeover, node()} | {failover, node()}.
-type start_args() :: term().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc application start callback for riak_cs.
-spec start(start_type(), start_args()) -> {ok, pid()} |
                                           {error, term()}.
start(_Type, _StartArgs) ->
    case is_config_valid() of
        true ->
            riak_cs_sup:start_link();
        false ->
            _ = lager:error("You must update you Riak CS app.config. Please see the release notes for more information on updating you configuration."),
            {error, bad_config}
    end.

%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

is_config_valid() ->
    get_env_response_to_bool(application:get_env(riak_cs, connection_pools)).

get_env_response_to_bool({ok, _}) ->
    true;
get_env_response_to_bool(_) ->
    false.
