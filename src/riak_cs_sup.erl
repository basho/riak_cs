%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Supervisor for the riak_cs application.

-module(riak_cs_sup).

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
                         non_neg_integer(),
                         non_neg_integer()},
                        [supervisor:child_spec()]}}.
init([]) ->
    catch dtrace:init(),                   % NIF load trigger (R14B04)
    catch dyntrace:p(),                    % NIF load trigger (R15B01+)
    case application:get_env(riak_cs, cs_ip) of
        {ok, Ip} ->
            ok;
        undefined ->
            Ip = "0.0.0.0"
    end,
    case application:get_env(riak_cs, cs_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 80
    end,
    case application:get_env(riak_cs, http_logdir) of
        {ok, WebLogDir} ->
            ok = filelib:ensure_dir(WebLogDir);
        undefined ->
            WebLogDir = "log"
    end,

    %% Create child specifications
    WebConfig1 = [
                 {dispatch, riak_cs_web:dispatch_table()},
                 {ip, Ip},
                 {port, Port},
                 {nodelay, true},
                 {log_dir, WebLogDir},
                 {rewrite_module, riak_cs_s3_rewrite},
                 {error_handler, riak_cs_wm_error_handler},
                 {resource_module_option, submodule}],
    case application:get_env(riak_cs, ssl) of
        {ok, SSLOpts} ->
            WebConfig = WebConfig1 ++ [{ssl, true},
                                       {ssl_opts, SSLOpts}];
        undefined ->
            WebConfig = WebConfig1
    end,
    Stats = {riak_cs_stats, {riak_cs_stats, start_link, []},
             permanent, 5000, worker, dynamic},
    Web = {webmachine_mochiweb,
           {webmachine_mochiweb, start, [WebConfig]},
           permanent, 5000, worker, dynamic},
    ListObjectsETSCacheSup = {riak_cs_list_objects_ets_cache_sup,
                              {riak_cs_list_objects_ets_cache_sup, start_link, []},
                              permanent, 5000, supervisor, dynamic},
    PutFsmSup = {riak_cs_put_fsm_sup,
                 {riak_cs_put_fsm_sup, start_link, []},
                 permanent, 5000, worker, dynamic},
    GetFsmSup = {riak_cs_get_fsm_sup,
             {riak_cs_get_fsm_sup, start_link, []},
             permanent, 5000, worker, dynamic},
    DeleteFsmSup = {riak_cs_delete_fsm_sup,
                 {riak_cs_delete_fsm_sup, start_link, []},
                 permanent, 5000, worker, dynamic},
    Archiver = {riak_cs_access_archiver_sup,
                {riak_cs_access_archiver_sup, start_link, []},
                permanent, 5000, worker, dynamic},
    Storage = {riak_cs_storage_d,
               {riak_cs_storage_d, start_link, []},
               permanent, 5000, worker, [riak_cs_storage_d]},
    GC = {riak_cs_gc_d,
          {riak_cs_gc_d, start_link, []},
          permanent, 5000, worker, [riak_cs_gc_d]},

    {ok, PoolList} = application:get_env(riak_cs, connection_pools),
    WorkerStop = fun(Worker) -> riak_cs_riakc_pool_worker:stop(Worker) end,
    PoolSpecs = [{Name,
                  {poolboy, start_link, [[{name, {local, Name}},
                                          {worker_module, riak_cs_riakc_pool_worker},
                                          {size, Workers},
                                          {max_overflow, Overflow},
                                          {stop_fun, WorkerStop}]]},
                  permanent, 5000, worker, [poolboy]}
                 || {Name, {Workers, Overflow}} <- PoolList],
    Processes = PoolSpecs ++
      [Archiver,
       Storage,
       GC,
       Stats,
       ListObjectsETSCacheSup,
       DeleteFsmSup,
       GetFsmSup,
       PutFsmSup,
       Web],
    {ok, { {one_for_one, 10, 10}, Processes} }.
