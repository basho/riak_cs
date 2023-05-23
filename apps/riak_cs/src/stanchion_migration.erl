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
         apply_stanchion_details/1,
         save_stanchion_data/1
        ]).

-include("riak_cs.hrl").

-define(REASONABLY_SMALL_TIMEOUT, 3000).


-spec validate_stanchion() -> ok.
validate_stanchion() ->
    {ConfiguredIP, ConfiguredPort, _Ssl} = riak_cs_config:stanchion(),
    case read_stanchion_data() of
        {ok, {{Host, Port}, Node}}
          when Host == ConfiguredIP,
               Port == ConfiguredPort,
               Node == node() ->
            ok;
        {ok, {{Host, Port}, Node}} ->
            logger:info("stanchion details updated: ~s:~p on ~s", [Host, Port, Node]),
            case lists:member(ConfiguredIP, this_host_addresses()) of
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

this_host_addresses() ->
    {ok, Ifs} = inet:getifaddrs(),
    lists:filtermap(
      fun({_If, PL}) ->
              case proplists:get_value(addr, PL) of
                  AA when AA /= undefined,
                          AA /= {0,0,0,0},
                          size(AA) == 4 ->
                      {A1, A2, A3, A4} = AA,
                      {true, lists:flatten(io_lib:format("~b.~b.~b.~b", [A1, A2, A3, A4]))};
                  _ ->
                      false
              end
      end, Ifs).

select_addr_for_stanchion() ->
    {Subnet_, Mask_} = riak_cs_config:stanchion_subnet_and_netmask(),
    {ok, Subnet} = inet:parse_address(Subnet_),
    {ok, Mask} = inet:parse_address(Mask_),
    case netutils:get_local_ip_from_subnet({Subnet, Mask}) of
        {ok, {A1, A2, A3, A4}} ->
            lists:flatten(io_lib:format("~b.~b.~b.~b", [A1, A2, A3, A4]));
        undefined ->
            logger:warning("No network interfaces with assigned addresses matching ~s:"
                           " falling back to 127.0.0.1", [Mask_]),
            "127.0.0.1"
    end.

adopt_stanchion() ->
    case riak_cs_config:stanchion_hosting_mode() of
        auto ->
            Addr = select_addr_for_stanchion(),
            {ok, Port} = application:get_env(riak_cs, stanchion_port),
            ok = save_stanchion_data({Addr, Port}),
            ok = apply_stanchion_details({Addr, Port}),
            start_stanchion_here(),
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
