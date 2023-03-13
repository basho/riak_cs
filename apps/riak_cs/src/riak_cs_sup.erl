%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Supervisor for the riak_cs application.

-module(riak_cs_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include("riak_cs.hrl").

-define(OPTIONS, [connection_pools,
                  listener,
                  admin_listener,
                  ssl,
                  admin_ssl,
                  {stanchion_hosting_mode, auto}]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    catch dyntrace:p(),                    % NIF load trigger (R15B01+)

    riak_cs_stats:init(),

    RewriteMod = application:get_env(riak_cs, rewrite_module, ?AWS_API_MOD),
    ok = application:set_env(webmachine_mochiweb, rewrite_modules, [{object_web, RewriteMod}]),

    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


-spec init([]) -> {ok, {supervisor:sup_flags(),
                        [supervisor:child_spec()]}}.
init([]) ->
    Options = [get_option_val(Option) || Option <- ?OPTIONS],
    init2(Options).

init2(Options) ->
    Mode = proplists:get_value(stanchion_hosting_mode, Options),
    RCSChildren =
        case Mode of
            M when M == auto;
                   M == riak_cs_only;
                   M == riak_cs_with_stanchion ->
                pool_specs(Options) ++ rcs_process_specs() ++ web_specs(Options);
            _ ->
                []
        end,
    {ok, {#{strategy => one_for_one,
            intensity => 10,
            period => 10}, RCSChildren
          }}.

rcs_process_specs() ->
    [ #{id => riak_cs_access_archiver_manager,
        start => {riak_cs_access_archiver_manager, start_link, []}},

      #{id => riak_cs_storage_d,
        start => {riak_cs_storage_d, start_link, []}},

      #{id => riak_cs_gc_manager,
        start => {riak_cs_gc_manager, start_link, []}},

      #{id => riak_cs_delete_fsm_sup,
        start => {riak_cs_delete_fsm_sup, start_link, []},
        type => supervisor,
        modules => dynamic},

      #{id => riak_cs_get_fsm_sup,
        start => {riak_cs_get_fsm_sup, start_link, []},
        modules => dynamic},

      #{id => riak_cs_put_fsm_sup,
        start => {riak_cs_put_fsm_sup, start_link, []},
        type => supervisor,
        modules => dynamic},

      #{id => riak_cs_diags,
        start => {riak_cs_diags, start_link, []},
        modules => dynamic},

      #{id => riak_cs_quota_sup,
        start => {riak_cs_quota_sup, start_link, []},
        type => supervisor,
        modules => dynamic},

      #{id => stanchion_sup,
        start => {stanchion_sup, start_link, []},
        type => supervisor,
        modules => dynamic}
    ]
        ++ riak_cs_mb_helper:process_specs().

get_option_val({Option, Default}) ->
    handle_get_env_result(Option, get_env(Option), Default);
get_option_val(Option) ->
    get_option_val({Option, undefined}).

get_env(Key) ->
    application:get_env(riak_cs, Key).

handle_get_env_result(Option, {ok, Value}, _) ->
    {Option, Value};
handle_get_env_result(Option, undefined, Default) ->
    {Option, Default}.

web_specs(Options) ->
    WebConfigs =
        case single_web_interface(proplists:get_value(admin_listener, Options)) of
            true ->
                [{riak_cs_object_web, add_admin_dispatch_table(object_web_config(Options))}];
            false ->
                [{riak_cs_admin_web, admin_web_config(Options)},
                 {riak_cs_object_web, object_web_config(Options)}]
        end,
    [web_spec(Name, Config) || {Name, Config} <- WebConfigs].

pool_specs(Options) ->
    rc_pool_specs(Options) ++
        pbc_pool_specs(Options).

rc_pool_specs(Options) ->
    WorkerStop = fun(Worker) -> riak_cs_riak_client:stop(Worker) end,
    MasterPools = proplists:get_value(connection_pools, Options),
    [#{id => Name,
      start => {poolboy, start_link, [[{name, {local, Name}},
                                       {worker_module, riak_cs_riak_client},
                                       {size, Workers},
                                       {max_overflow, Overflow},
                                       {stop_fun, WorkerStop}],
                                      []]}}
     || {Name, {Workers, Overflow}} <- MasterPools].

pbc_pool_specs(Options) ->
    WorkerStop = fun(Worker) -> riak_cs_riakc_pool_worker:stop(Worker) end,
    %% Use sums of fixed/overflow for pbc pool
    MasterPools = proplists:get_value(connection_pools, Options),
    {FixedSum, OverflowSum} = lists:foldl(fun({_, {Fixed, Overflow}}, {FAcc, OAcc}) ->
                                                  {Fixed + FAcc, Overflow + OAcc}
                                          end,
                                          {0, 0}, MasterPools),
    riak_cs_config:set_multibag_appenv(),
    Bags = riak_cs_mb_helper:bags(),
    [pbc_pool_spec(BagId, FixedSum, OverflowSum, Address, Port, WorkerStop)
     || {BagId, Address, Port} <- Bags].

pbc_pool_spec(BagId, Fixed, Overflow, Address, Port, WorkerStop) ->
    Name = riak_cs_riak_client:pbc_pool_name(BagId),
    #{id => Name,
      start => {poolboy, start_link, [[{name, {local, Name}},
                                       {worker_module, riak_cs_riakc_pool_worker},
                                       {size, Fixed},
                                       {max_overflow, Overflow},
                                       {stop_fun, WorkerStop}],
                                      [{address, Address},
                                       {port, Port}]]}}.

web_spec(Name, Config) ->
    #{id => Name,
      start => {webmachine_mochiweb, start, [Config]},
      modules => dynamic}.

object_web_config(Options) ->
    {IP, Port} = proplists:get_value(listener, Options),
    [{dispatch, riak_cs_web:object_api_dispatch_table()},
     {name, object_web},
     {dispatch_group, object_web},
     {ip, IP},
     {port, Port},
     {nodelay, true},
     {error_handler, riak_cs_wm_error_handler},
     {resource_module_option, submodule}] ++
        maybe_add_ssl_opts(proplists:get_value(ssl, Options)).

admin_web_config(Options) ->
    {IP, Port} = proplists:get_value(admin_listener,
                                     Options, {"127.0.0.1", 8000}),
    [{dispatch, riak_cs_web:admin_api_dispatch_table()},
     {name, admin_web},
     {dispatch_group, admin_web},
     {ip, IP}, {port, Port},
     {nodelay, true},
     {error_handler, riak_cs_wm_error_handler}] ++
        maybe_add_ssl_opts(proplists:get_value(admin_ssl, Options)).

single_web_interface(undefined) ->
    true;
single_web_interface(_) ->
    false.

maybe_add_ssl_opts(undefined) ->
    [];
maybe_add_ssl_opts(SSLOpts) ->
    [{ssl, true}, {ssl_opts, SSLOpts}].

add_admin_dispatch_table(Config) ->
    UpdDispatchTable = proplists:get_value(dispatch, Config) ++
        riak_cs_web:admin_api_dispatch_table(),
    [{dispatch, UpdDispatchTable} | proplists:delete(dispatch, Config)].

