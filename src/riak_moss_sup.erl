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

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc API for starting the supervisor.
-spec start_link() -> supervisor:startchild_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc supervisor callback.
-spec init([]) -> {ok, {{supervisor:strategy(),
                         integer(),
                         integer()},
                        [supervisor:child_spec()]}} | ignore.
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

    %% Create child specifications
    Server =
        {riak_moss_riakc,
         {riak_moss_riakc, start_link, []},
         permanent, 5000, worker, dynamic},
    WebConfig1 = [
                 {dispatch, riak_moss_web:dispatch_table()},
                 {ip, Ip},
                 {port, Port},
                 {nodelay, true},
                 {log_dir, "log"}],
    case application:get_env(riak_moss, ssl) of

        {ok, SSLOpts} ->
            WebConfig = WebConfig1 ++ [{ssl, true},
                                       {ssl_opts, SSLOpts}];
        undefined ->
            WebConfig = WebConfig1
    end,
    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [WebConfig]},
           permanent, 5000, worker, dynamic},
    Processes = [Server,Web],
    {ok, { {one_for_one, 10, 10}, Processes} }.
