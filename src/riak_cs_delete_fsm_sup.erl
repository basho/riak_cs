%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for `riak_cs_delete_fsm'

-module(riak_cs_delete_fsm_sup).

-behaviour(supervisor).

%% API
-export([start_delete_fsm/2]).
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

%% @doc Start a `riak_cs_delete_fsm' child process.
-spec start_delete_fsm(node(), supervisor:child_spec()) ->
                              supervisor:startchild_ret().
start_delete_fsm(Node, Args) ->
    supervisor:start_child({?MODULE, Node}, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @doc Initialize this supervisor. This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_cs_delete_fsm' processes.
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

    DeleteFsmSpec = {undefined,
                     {riak_cs_delete_fsm, start_link, []},
                     Restart, Shutdown, Type, [riak_cs_delete_fsm]},

    {ok, {SupFlags, [DeleteFsmSpec]}}.
