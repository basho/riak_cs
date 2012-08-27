%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

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
    riak_cs_sup:start_link().

%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.
