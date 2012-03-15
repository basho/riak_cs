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

    %% Create child specifications
    WebConfig1 = [
                 {dispatch, riak_moss_web:dispatch_table()},
                 {ip, Ip},
                 {port, Port},
                 {nodelay, true},
                 {log_dir, "log"},
                 {rewrite_module, riak_moss_wm_rewrite},
                 {error_handler, riak_moss_wm_error_handler}],
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
    PutFsmSup = {riak_moss_put_fsm_sup,
                 {riak_moss_put_fsm_sup, start_link, []},
                 permanent, 5000, worker, dynamic},
    GetFsmSup = {riak_moss_get_fsm_sup,
             {riak_moss_get_fsm_sup, start_link, []},
             permanent, 5000, worker, dynamic},
    DeleteFsmSup = {riak_moss_delete_fsm_sup,
                 {riak_moss_delete_fsm_sup, start_link, []},
                 permanent, 5000, worker, dynamic},
    DeleterSup = {riak_moss_deleter_sup,
                 {riak_moss_deleter_sup, start_link, []},
                 permanent, 5000, worker, dynamic},
    Processes = [DeleterSup,
                 DeleteFsmSup,
                 GetFsmSup,
                 PutFsmSup,
                 Web],
    {ok, { {one_for_one, 10, 10}, Processes} }.
