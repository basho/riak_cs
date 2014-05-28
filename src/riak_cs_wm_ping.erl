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

-module(riak_cs_wm_ping).

-export([init/1,
         service_available/2,
         allowed_methods/2,
         to_html/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ping_context, {pool_pid=true :: boolean(),
                       riak_client :: undefined | riak_client()}).

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(_Config) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"init">>),
    {ok, #ping_context{}}.

-spec service_available(#wm_reqdata{}, #ping_context{}) -> {boolean(), #wm_reqdata{}, #ping_context{}}.
service_available(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"service_available">>),
    {Available, UpdCtx} = riak_ping(get_connection_pid(), Ctx),
    {Available, RD, UpdCtx}.

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"allowed_methods">>),
    {['GET', 'HEAD'], RD, Ctx}.

to_html(ReqData, Ctx) ->
    {"OK", ReqData, Ctx}.

finish_request(RD, Ctx=#ping_context{riak_client=undefined}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#ping_context{riak_client=RcPid,
                                     pool_pid=PoolPid}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [1], []),
    case PoolPid of
        true ->
            riak_cs_riak_client:checkin(RcPid);
        false ->
            riak_cs_riak_client:stop(RcPid)
    end,
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finish_request">>, [1], []),
    {true, RD, Ctx#ping_context{riak_client=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

%% @doc Return the configured ping timeout. Default is 5 seconds.  The
%% timeout is used in call to `poolboy:checkout' and if that fails in
%% the call to `riakc_pb_socket:ping' so the effective cumulative
%% timeout could be up to 2 * `ping_timeout()'.
-spec ping_timeout() -> pos_integer().
ping_timeout() ->
    case application:get_env(riak_cs, ping_timeout) of
        undefined ->
            ?DEFAULT_PING_TIMEOUT;
        {ok, Timeout} ->
            Timeout
    end.

-spec get_connection_pid() -> {riak_client(), boolean()}.
get_connection_pid() ->
    case pool_checkout() of
        full ->
            non_pool_connection();
        RcPid ->
            {RcPid, true}
    end.

-spec pool_checkout() -> full | riak_client().
pool_checkout() ->
    case riak_cs_riak_client:checkout(request_pool) of
        {ok, RcPid} ->
            RcPid;
        {error, _Reason} ->
            full
    end.

-spec non_pool_connection() -> {undefined | riak_client(), false}.
non_pool_connection() ->
    case riak_cs_riak_client:start_link([]) of
        {ok, RcPid} ->
            {RcPid, false};
        {error, _} ->
            {undefined, false}
    end.

-spec riak_ping({riak_client(), boolean()}, #ping_context{}) -> {boolean(), #ping_context{}}.
riak_ping({RcPid, PoolPid}, Ctx) ->
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    Available = case catch riakc_pb_socket:ping(MasterPbc, ping_timeout()) of
                    pong ->
                        true;
                    _ ->
                        false
                end,
    {Available, Ctx#ping_context{riak_client=RcPid, pool_pid=PoolPid}}.
