%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Entry point for poolboy to pool Riak connections

-module(riak_cs_riakc_pool_worker).

-export([riak_host_port/0,
         start_link/1,
         stop/1]).

-spec riak_host_port() -> {string(), pos_integer()}.
riak_host_port() ->
    case application:get_env(riak_cs, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_cs, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    {Host, Port}.

-spec start_link(term()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    Address = proplists:get_value(address, Args, "127.0.0.1"),
    Port = proplists:get_value(port, Args, 8087),
    Timeout = case application:get_env(riak_cs, riakc_connect_timeout) of
        {ok, ConfigValue} ->
            ConfigValue;
        undefined ->
            10000
    end,
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Address, Port, StartOptions).

stop(undefined) ->
    ok;
stop(Worker) ->
    riakc_pb_socket:stop(Worker).
