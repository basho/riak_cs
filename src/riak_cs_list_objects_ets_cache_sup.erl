%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc
-module(riak_cs_list_objects_ets_cache_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    ETSCache = {riak_cs_list_objects_ets_cache,
                {riak_cs_list_objects_ets_cache, start_link, []},
                permanent, 5000, worker,
                [riak_cs_list_objects_ets_cache]},

    {ok, {SupFlags, [ETSCache]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
