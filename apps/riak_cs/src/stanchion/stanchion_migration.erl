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

%% @doc Functions to locate and migrate stanchion.

-module(stanchion_migration).

-export([do_we_get_to_run_stanchion/3,
         locate_stanchion/0,
         apply_stanchion_details/1,
         read_stanchion_data/1,
         save_stanchion_data/2]).

-include("riak_cs.hrl").

-define(REASONABLY_SMALL_TIMEOUT, 1000).


-spec locate_stanchion() -> ok.
locate_stanchion() ->
    Mode = auto,
    {ok, Pbc} = riak_cs_utils:riak_connection(),
    ThisHostAddr = riak_cs_utils:this_host_addr(),
    case do_we_get_to_run_stanchion(Mode, ThisHostAddr, Pbc) of
        use_ours ->
            {ok, {_IP, Port}} = application:get_env(riak_cs, stanchion_listener),
            _ = [supervisor:start_child(?MODULE, F)
                 || F <- stanchion_sup:stanchion_process_specs()],
            ok = save_stanchion_data(Pbc, {ThisHostAddr, Port});
        {use_saved, _} ->
            _ = [supervisor:delete_child(?MODULE, Id)
                 || {Id, _, _, _} <- supervisor:which_children(?MODULE)]
    end,
    ok = riakc_pb_socket:stop(Pbc),
    ok.

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
