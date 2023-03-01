%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2022, 2023 TI Tokyo    All Rights Reserved.
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

-export([validate_stanchion/0,
         adopt_stanchion/0,
         do_we_get_to_run_stanchion/2,
         apply_stanchion_details/1,
         save_stanchion_data/1
        ]).

-include("riak_cs.hrl").

-define(REASONABLY_SMALL_TIMEOUT, 3000).


-spec validate_stanchion() -> ok.
validate_stanchion() ->
    {ConfiguredIP, ConfiguredPort, _Ssl} = riak_cs_config:stanchion(),
    logger:debug("validate_stanchion: ~p", [{ConfiguredIP, ConfiguredPort}]),
    case read_stanchion_data() of
        {ok, {{Host, Port}, Node}}
          when Host == ConfiguredIP,
               Port == ConfiguredPort,
               Node == node() ->
            logger:debug("validate_stanchion: matching data read"),
            ok;
        {ok, {{Host, Port}, Node}} ->
            logger:info("stanchion details updated: ~s:~p on ~s", [Host, Port, Node]),
            case lists:member(ConfiguredIP, riak_cs_utils:this_host_addresses()) of
                true when node() == Node ->
                    stop_stanchion_here(),
                    ok;
                _ ->
                    ok
            end,
            apply_stanchion_details({Host, Port});
        {error, notfound} ->
            logger:info("no previously saved stanchion details; adopting stanchion here"),
            adopt_stanchion()
    end.


-spec adopt_stanchion() -> ok | {error, stanchion_not_relocatable}.
adopt_stanchion() ->
    case riak_cs_config:stanchion_hosting_mode() of
        auto ->
            Addr = riak_cs_utils:select_addr_for_stanchion(),
            {ok, {_IP, Port}} = application:get_env(riak_cs, stanchion_listener),
            start_stanchion_here(),
            ok = save_stanchion_data({Addr, Port}),
            apply_stanchion_details({Addr, Port}),
            ok;
        M ->
            logger:error("Riak CS stanchion_hosting_mode is ~s. Cannot adopt stanchion.", [M]),
            {error, stanchion_not_relocatable}
    end.


start_stanchion_here() ->
    case supervisor:which_children(stanchion_sup) of
        [] ->
            _ = [supervisor:start_child(stanchion_sup, F) || F <- stanchion_sup:stanchion_process_specs()];
        _ ->
            already_running
    end.

stop_stanchion_here() ->
    case supervisor:which_children(stanchion_sup) of
        [] ->
            already_stopped;
        FF ->
            logger:notice("Stopping stanchion on this node"),
            [begin
                 ok = supervisor:terminate_child(stanchion_sup, Id),
                 ok = supervisor:delete_child(stanchion_sup, Id)
             end || {Id, _, _, _} <- FF]
    end.


do_we_get_to_run_stanchion(Mode, ThisHostAddr) ->
    {ConfiguredIP, ConfiguredPort, _Ssl} = riak_cs_config:stanchion(),
    case read_stanchion_data() of
        {ok, {{Host, Port}, Node}} when Mode == auto ->
            logger:info("going to use stanchion started at ~s:~b (~s)", [Host, Port, Node]),
            if Host == ThisHostAddr andalso
               Port == ConfiguredPort andalso
               Node == node() ->
                    logger:info("read stanchion details previously saved by us;"
                                " will start stanchion again at ~s:~b", [Host, Port]),
                    use_ours;
               el/=se ->
                    {use_saved, {Host, Port}}
            end;

        {ok, {{Host, Port}, Node}} when Mode == riak_cs_only ->
            logger:info("going to use stanchion started at ~s:~b (~s)", [Host, Port, Node]),
            {use_saved, {Host, Port}};

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
    riak_cs_config:set_stanchion(Host, Port).

read_stanchion_data() ->
    Pbc = stanchion_utils:get_pbc(),
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
                                   " Please select a riak_cs node, reconfigure it with stanchion_hosting_mode = riak_cs_with_stanchion (or stanchion_only),"
                                   " configure rest with stanchion_hosting_mode = riak_cs_only, and restart all nodes", [N]),
                    {ok, hd(Values)}
            end;
        _ ->
            {error, notfound}
    end.

save_stanchion_data(HostPort) ->
    Pbc = stanchion_utils:get_pbc(),
    ok = riak_cs_pbc:put_sans_stats(
           Pbc, riakc_obj:new(?SERVICE_BUCKET, ?STANCHION_DETAILS_KEY,
                              term_to_binary({HostPort, node()})),
           ?REASONABLY_SMALL_TIMEOUT),
    logger:info("saved stanchion details: ~p", [{HostPort, node()}]),
    ok.
