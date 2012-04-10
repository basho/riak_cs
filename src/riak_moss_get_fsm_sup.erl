%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for `riak_moss_get_fsm'

-module(riak_moss_get_fsm_sup).

-behaviour(supervisor).

%% API
-export([start_get_fsm/5]).
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
-spec start_get_fsm(node(), binary(), binary(), pid(), pid()) ->
                           {ok, pid()} | {error, term()}.  %% SLF: R14B04's supervisor:startchild_ret() is broken?
start_get_fsm(Node, Bucket, Key, Caller, RiakPid) ->
    supervisor:start_child({?MODULE, Node}, [Bucket, Key, Caller, RiakPid]).

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
                  Restart, Shutdown, Type, [riak_moss_get_fsm]},

    {ok, {SupFlags, [PutFsmSpec]}}.
