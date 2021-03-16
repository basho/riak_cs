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

%% @doc Supervisor for `riak_cs_put_fsm'

-module(riak_cs_put_fsm_sup).

-behaviour(supervisor).

-include("riak_cs.hrl").

%% API
-export([start_put_fsm/2]).
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).


%% ===================================================================
%% API functions
%% ===================================================================

%% @doc API for starting the supervisor.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a `riak_cs_put_fsm' child process.
-spec start_put_fsm(node(),
                    [{binary(), binary(), non_neg_integer(), binary(),
                      term(), pos_integer(), acl(), timeout(), pid(), pid()}])->
                           {ok, pid()} | {error, term()}.
start_put_fsm(Node, ArgList) ->
    supervisor:start_child({?MODULE, Node}, ArgList).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @doc Initialize this supervisor. This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_cs_put_fsm' processes.
-spec init([]) -> {ok, {{supervisor:strategy(),
                         pos_integer(),
                         pos_integer()},
                        [supervisor:child_spec()]}}.
init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = temporary,
    Shutdown = 2000,
    Type = worker,

    PutFsmSpec = {undefined,
                  {riak_cs_put_fsm, start_link, []},
                  Restart, Shutdown, Type, [riak_cs_put_fsm]},

    {ok, {SupFlags, [PutFsmSpec]}}.
