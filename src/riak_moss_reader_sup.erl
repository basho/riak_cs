%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for `riak_moss_reader'

-module(riak_moss_reader_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_reader/2,
         terminate_reader/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc API for starting the supervisor.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a `riak_moss_reader' child process.
-spec start_reader(node(), supervisor:child_spec()) ->
                          supervisor:startchild_ret().
start_reader(Node, CallerPid) ->
    supervisor:start_child({?MODULE, Node}, [[CallerPid]]).

%% @doc Stop a reader process immediately
-spec terminate_reader(node(), supervisor:child_spec()) -> {ok | {error, term()}}.
terminate_reader(Node, ReaderPid) ->
    supervisor:terminate_child({?MODULE, Node}, ReaderPid).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @doc Initialize this supervisor. This is a `simple_one_for_one',
%%      whose child spec is for starting `riak_moss_reader' processes.
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

    ReaderSpec = {undefined,
                  {riak_moss_reader, start_link, []},
                  Restart, Shutdown, Type, [riak_moss_reader]},

    {ok, {SupFlags, [ReaderSpec]}}.
