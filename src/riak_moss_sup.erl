%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for the riak_moss application.

-module(riak_moss_sup).

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
-spec init([]) -> {ok, {{supervisor:strategy(),
                         integer(),
                         integer()},
                        [supervisor:child_spec()]}}.
init([]) ->
    case application:get_env(riak_moss, moss_ip) of
        {ok, Ip} ->
            ok;
        undefined ->
            Ip = "0.0.0.0"
    end,
    case application:get_env(riak_moss, moss_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 80
    end,
    case application:get_env(riak_moss, dispatch) of
        {ok, Dispatch} ->
            ok;
        undefined ->
            Dispatch =
                [{[], riak_moss_wm_service, []},
                 {[bucket], riak_moss_wm_bucket, []},
                 {[bucket, key], riak_moss_wm_key, []}]
    end,
    Server =
        {riak_moss_riakc,
         {riak_moss_riakc, start_link, []},
         permanent, 5000, worker, dynamic},
    WebConfig = [
                 {dispatch, Dispatch},
                 {ip, Ip},
                 {port, Port},
                 {log_dir, "log"}],
    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [WebConfig]},
           permanent, 5000, worker, dynamic},

    Processes = [Server,Web],
    {ok, { {one_for_one, 10, 10}, Processes} }.
