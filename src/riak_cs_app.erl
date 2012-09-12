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
    case is_config_valid() of
        true ->
            riak_cs_sup:start_link();
        false ->
            lager:error("You must update you Riak CS app.config. Please see the release notes for more information on updating you configuration."),
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
