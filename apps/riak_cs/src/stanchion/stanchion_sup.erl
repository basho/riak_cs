%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2022 TI Tokyo    All Rights Reserved.
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

-module(stanchion_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).
-export([negotiate_stanchion/0]).

-include("riak_cs.hrl").

-define(REASONABLY_SMALL_TIMEOUT, 1000).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    {ok, Mode} = application:get_env(riak_cs, operation_mode),
    {ok, Pbc} = riak_connection(),
    ThisHostAddr = this_host_addr(),
    Children =
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
            period => 10}, Children
         }}.


negotiate_stanchion() ->
    Mode = auto,
    {ok, Pbc} = riak_connection(),
    ThisHostAddr = this_host_addr(),
    case do_we_get_to_run_stanchion(Mode, ThisHostAddr, Pbc) of
        use_ours ->
            {ok, {_IP, Port}} = application:get_env(riak_cs, stanchion_listener),
            _ = [supervisor:start_child(?MODULE, F)
                 || F <- stanchion_process_specs()],
            ok = save_stanchion_data(Pbc, {ThisHostAddr, Port});
        {use_saved, _} ->
            _ = [supervisor:delete_child(?MODULE, Id)
                 || {Id, _, _, _} <- supervisor:which_children(?MODULE)]
    end,
    ok = riakc_pb_socket:stop(Pbc),
    ok.



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
    Web =
        #{id => stanchion_webmachine,
          start => {webmachine_mochiweb, start, [WebConfig]},
          restart => permanent,
          shutdown => 5000,
          modules => dynamic},
    ServerSup =
        #{id => stanchion_server_sup,
          start => {stanchion_server_sup, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => supervisor,
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

check_stanchion_reachable(Host, Port) ->
    {ok, UseSSL} = application:get_env(riak_cs, stanchion_ssl),
    velvet:ping(Host, Port, UseSSL).


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
            logger:info("this host address is ~s on iface ~s", [IP, If]),
            IP;
        [{If, IP}|_] ->
            logger:warning("This host has multiple network interfaces configured."
                           " Selecting ~p on ~s", [IP, If]),
            IP
    end.
