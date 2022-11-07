%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

%% Public API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

-include("riak_cs.hrl").

-define(OPTIONS, [connection_pools,
                  listener,
                  admin_listener,
                  ssl,
                  admin_ssl,
                  {operation_mode, auto},
                  {rewrite_module, ?S3_API_MOD}]).

-define(REASONABLY_SMALL_TIMEOUT, 1000).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.
-type proplist() :: proplists:proplist().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc API for starting the supervisor.
-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc supervisor callback.
-spec init([]) -> {ok, {supervisor:sup_flags(),
                        [supervisor:child_spec()]}}.
init([]) ->
    catch dyntrace:p(),                    % NIF load trigger (R15B01+)
    Options = [get_option_val(Option) || Option <- ?OPTIONS],
    init2(Options).

init2(Options) ->
    Mode = proplists:get_value(operation_mode, Options),
    ThisHostAddr = this_host_addr(),
    {ok, Pbc} = riak_connection(),
    ok = ensure_service_bucket_props(Pbc),
    RCSChildren =
        case Mode of
            M when M == auto;
                   M == riak_cs_only;
                   M == riak_cs_with_stanchion ->
                riak_cs_stats:init(),
                pool_specs(Options) ++ rcs_process_specs() ++ web_specs(Options);
            _ ->
                []
        end,
    StanchionChildren =
        case do_we_get_to_run_stanchion(Mode, ThisHostAddr, Pbc) of
            {use_saved, HostPort} ->
                ok = apply_stanchion_details(HostPort),
                [];
            use_ours ->
                {ok, {_IP, Port}} = application:get_env(riak_cs, stanchion_listener),
                ok = save_stanchion_data(Pbc, {ThisHostAddr, Port}),
                stanchion_stats:init(),
                stanchion_process_specs()
        end,
    ok = riakc_pb_socket:stop(Pbc),
    {ok, {#{strategy => one_for_one,
            intensity => 10,
            period => 10}, RCSChildren ++ StanchionChildren
          }}.

ensure_service_bucket_props(Pbc) ->
    riakc_pb_socket:set_bucket(Pbc, ?SERVICE_BUCKET, [{allow_mult, false}]).

rcs_process_specs() ->
    [ #{id => riak_cs_access_archiver_manager,
        start => {riak_cs_access_archiver_manager, start_link, []}},

      #{id => riak_cs_storage_d,
        start => {riak_cs_storage_d, start_link, []}},

      #{id => riak_cs_gc_manager,
        start => {riak_cs_gc_manager, start_link, []}},

      #{id => riak_cs_delete_fsm_sup,
        start => {riak_cs_delete_fsm_sup, start_link, []},
        modules => dynamic},

      #{id => riak_cs_list_objects_ets_cache_sup,
        start => {riak_cs_list_objects_ets_cache_sup, start_link, []},
        type => supervisor,
        modules => dynamic},

      #{id => riak_cs_get_fsm_sup,
        start => {riak_cs_get_fsm_sup, start_link, []},
        modules => dynamic},

      #{id => riak_cs_put_fsm_sup,
        start => {riak_cs_put_fsm_sup, start_link, []},
        modules => dynamic},

      #{id => riak_cs_diags,
        start => {riak_cs_diags, start_link, []},
        modules => dynamic},

      #{id => riak_cs_quota_sup,
        start => {riak_cs_quota_sup, start_link, []},
        modules => dynamic}
    ]
        ++ riak_cs_mb_helper:process_specs().

stanchion_process_specs() ->
    {ok, {Ip, Port}} = application:get_env(riak_cs, stanchion_listener),

    %% Hide any bags from user-facing parts.
    case application:get_env(riak_cs, supercluster_members) of
        undefined -> ok;
        {ok, Bags} -> application:set_env(riak_cs, bags, Bags)
    end,

    WebConfig1 = [{dispatch, stanchion_web:dispatch_table()},
                  {ip, Ip},
                  {port, Port},
                  {nodelay, true},
                  {log_dir, "log"},
                  %% {rewrite_module, stanchion_wm_rewrite},
                  {error_handler, stanchion_wm_error_handler}
                 ],
    WebConfig =
        case application:get_env(riak_cs, stanchion_ssl) of
            {ok, true} ->
                {ok, CF} = application:get_env(riak_cs, stanchion_ssl_certfile),
                {ok, KF} = application:get_env(riak_cs, stanchion_ssl_keyfile),
                WebConfig1 ++ [{ssl, true},
                               {ssl_opts, [{certfile, CF}, {keyfile, KF}]}];
            {ok, false} ->
                WebConfig1
        end,
    Web = #{id => stanchion_webmachine,
            start => {webmachine_mochiweb, start, [WebConfig]},
            restart => permanent,
            shutdown => 5000,
            modules => dynamic},
    ServerSup = #{id => stanchion_server_sup,
                  start => {stanchion_server_sup, start_link, []},
                  restart => permanent,
                  shutdown => 5000,
                  modules => dynamic},
    [ServerSup, Web].



do_we_get_to_run_stanchion(Mode, ThisHostAddr, Pbc) ->
    {ok, {ConfiguredIP, ConfiguredPort}} = application:get_env(riak_cs, stanchion_listener),
    case read_stanchion_data(Pbc) of

        {ok, {{Host, Port}, Node}} when Mode == auto,
                                        Host == ThisHostAddr,
                                        Node == node() ->
            logger:info("read stanchion details previously saved by this host;"
                        " will start stanchion again at ~s:~b", [Host, Port]),
            use_ours;

        {ok, {{Host, Port}, Node}} when Mode == auto ->
            case check_stanchion_reachable(Host, Port) of
                ok ->
                    logger:info("read stanchion details, and will use ~s:~b (on node ~s)",
                                [Host, Port, Node]),
                    {use_saved, {Host, Port}};
                Error ->
                    logger:warning("stanchion at ~s:~b not reachable (~p); will set it up here", [Host, Port, Error]),
                    use_ours
            end;

        {ok, {{SavedHost, SavedPort}, Node}} when Mode == riak_cs_with_stanchion;
                                                  Mode == stanchion_only ->
            case ThisHostAddr of
                ConfiguredIP when ConfiguredPort == SavedPort,
                                  Node == node() ->
                    %% we read what we must have saved before
                    {use_saved, {SavedHost, SavedPort}};
                _ ->
                    logger:error("this node is configured to run stanchion but"
                                 " stanchion has already been started at ~s:~b",
                                 [SavedHost, SavedPort]),
                    conflicting_stanchion_configuration
            end;

        _ ->
            logger:info("no previously saved stanchion details; going to start stanchion on this node"),
            use_ours
    end.

apply_stanchion_details({Host, Port}) ->
    application:set_env(riak_cs, stanchion_host, Host),
    application:set_env(riak_cs, stanchion_port, Port),
    ok.

read_stanchion_data(Pbc) ->
    case riak_cs_pbc:get_sans_stats(Pbc, ?SERVICE_BUCKET, ?STANCHION_DETAILS_KEY,
                                    [{notfound_ok, false}],
                                    ?REASONABLY_SMALL_TIMEOUT) of
        {ok, Obj} ->
            case riakc_obj:value_count(Obj) of
                1 ->
                    StanchionDetails = binary_to_term(riakc_obj:get_value(Obj)),
                    {ok, StanchionDetails};
                0 ->
                    {error, notfound};
                N ->
                    Values = [binary_to_term(Value) ||
                                 Value <- riakc_obj:get_values(Obj),
                                 Value /= <<>>  % tombstone
                             ],
                    logger:warning("Read stanchion details riak object has ~b siblings."
                                   " Please select a riak_cs node, reconfigure it with operation_mode = riak_cs_with_stanchion (or stanchion_only),"
                                   " configure rest with operation_mode = riak_cs_only, and restart all nodes", [N]),
                    {ok, hd(Values)}
            end;
        _ ->
            {error, notfound}
    end.

save_stanchion_data(Pbc, HostPort) ->
    ok = riak_cs_pbc:put_sans_stats(
           Pbc, riakc_obj:new(?SERVICE_BUCKET, ?STANCHION_DETAILS_KEY,
                              term_to_binary({HostPort, node()})),
           ?REASONABLY_SMALL_TIMEOUT),
    logger:info("saved stanchion details: ~p", [{HostPort, node()}]),
    ok.

this_host_addr() ->
    {ok, Ifs} = inet:getifaddrs(),
    case lists:filtermap(
           fun({_If, PL}) ->
                   case proplists:get_value(addr, PL) of
                       Defined when Defined /= undefined,
                                    Defined /= {127,0,0,1},
                                    Defined /= {0,0,0,0} ->
                           {A1, A2, A3, A4} = Defined,
                           {true, {_If, io_lib:format("~b.~b.~b.~b", [A1, A2, A3, A4])}};
                       _ ->
                           false
                   end
           end, Ifs) of
        [{If, IP}] ->
            logger:info("This host address is ~p on iface ~s", [IP, If]),
            IP;
        [{If, IP}|_] ->
            logger:warning("This host has multiple network interfaces configured."
                           " Selecting ~p on ~s", [IP, If]),
            IP
    end.

check_stanchion_reachable(Host, Port) ->
    {ok, UseSSL} = application:get_env(riak_cs, stanchion_ssl),
    velvet:ping(Host, Port, UseSSL).



get_option_val({Option, Default}) ->
    handle_get_env_result(Option, get_env(Option), Default);
get_option_val(Option) ->
    get_option_val({Option, undefined}).

get_env(Key) ->
    application:get_env(riak_cs, Key).

-spec handle_get_env_result(atom(), {ok, term()} | 'undefined', term()) -> {atom(), term()}.
handle_get_env_result(Option, {ok, Value}, _) ->
    {Option, Value};
handle_get_env_result(Option, undefined, Default) ->
    {Option, Default}.

-spec web_specs(proplist()) -> [supervisor:child_spec()].
web_specs(Options) ->
    WebConfigs =
        case single_web_interface(proplists:get_value(admin_listener, Options)) of
            true ->
                [{object_web, add_admin_dispatch_table(object_web_config(Options))}];
            false ->
                [{admin_web, admin_web_config(Options)},
                 {object_web, object_web_config(Options)}]
        end,
    [web_spec(Name, Config) || {Name, Config} <- WebConfigs].

-spec pool_specs(proplist()) -> [supervisor:child_spec()].
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

-spec pbc_pool_spec(bag_id(), non_neg_integer(), non_neg_integer(),
                string(), non_neg_integer(), function()) ->
                       supervisor:child_spec().
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

-spec web_spec(atom(), proplist()) -> supervisor:child_spec().
web_spec(Name, Config) ->
    #{id => Name,
      start => {webmachine_mochiweb, start, [Config]},
      modules => dynamic}.

-spec object_web_config(proplist()) -> proplist().
object_web_config(Options) ->
    {IP, Port} = proplists:get_value(listener, Options),
    [{dispatch, riak_cs_web:object_api_dispatch_table()},
     {name, object_web},
     {dispatch_group, object_web},
     {ip, IP},
     {port, Port},
     {nodelay, true},
     {rewrite_module, proplists:get_value(rewrite_module, Options)},
     {error_handler, riak_cs_wm_error_handler},
     {resource_module_option, submodule}] ++
        maybe_add_ssl_opts(proplists:get_value(ssl, Options)).

-spec admin_web_config(proplist()) -> proplist().
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

-spec single_web_interface('undefined' | term()) -> boolean().
single_web_interface(undefined) ->
    true;
single_web_interface(_) ->
    false.

-spec maybe_add_ssl_opts('undefined' | proplist()) -> proplist().
maybe_add_ssl_opts(undefined) ->
    [];
maybe_add_ssl_opts(SSLOpts) ->
    [{ssl, true}, {ssl_opts, SSLOpts}].

-spec add_admin_dispatch_table(proplist()) -> proplist().
add_admin_dispatch_table(Config) ->
    UpdDispatchTable = proplists:get_value(dispatch, Config) ++
        riak_cs_web:admin_api_dispatch_table(),
    [{dispatch, UpdDispatchTable} | proplists:delete(dispatch, Config)].


riak_connection() ->
    {Host, Port} = riak_cs_config:riak_host_port(),
    Timeout = case application:get_env(riak_cs, riakc_connect_timeout) of
                  {ok, ConfigValue} ->
                      ConfigValue;
                  undefined ->
                      10000
              end,
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).
