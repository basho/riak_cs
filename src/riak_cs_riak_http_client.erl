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

%% @doc Entry point for a non-pooled HTTP connection to Riak

-module(riak_cs_riak_http_client).

-export([riak_http_host_port/0,
         get_rhc/0
]).

-spec riak_http_host_port() -> {string(), pos_integer()}.
riak_http_host_port() ->
    case application:get_env(riak_cs, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_cs, riak_http_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8098
    end,
    {Host, Port}.

get_rhc() ->
    {Host, Port} = riak_http_host_port(),
    Prefix = "riak",   % Required for create(), but should not be used, since this is old-style
    Opts = [],
    rhc:create(Host, Port, Prefix, Opts).
