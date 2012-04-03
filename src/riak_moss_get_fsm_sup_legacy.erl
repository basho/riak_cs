%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for `riak_moss_get_fsm'

-module(riak_moss_get_fsm_sup_legacy).

-behaviour(supervisor).

%% API
-export([start_get_fsm/2]).
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

%% @doc Start a `riak_moss_get_fsm' child process.
-spec start_get_fsm(node(), supervisor:child_spec()) ->
                           supervisor:startchild_ret().
start_get_fsm(Node, Args) ->
    supervisor:start_child({?MODULE, Node}, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @doc Initialize this supervisor. This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_moss_get_fsm' processes.
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
                  {riak_moss_get_fsm, start_link, []},
                  Restart, Shutdown, Type, [riak_moss_get_fsm_legacy]},

    {ok, {SupFlags, [PutFsmSpec]}}.
