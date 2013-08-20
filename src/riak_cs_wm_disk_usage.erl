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

-module(riak_cs_wm_disk_usage).

-export([init/1,
         service_available/2,
         allowed_methods/2,
         to_html/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(test_context, {pool_pid=true :: boolean(),
                       riakc_pid :: 'undefined' | pid()}).

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(_Config) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"init">>),
    {ok, #test_context{}}.

service_available(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"service_available">>),
    Available = true,
    UpdCtx = Ctx,
    % case poolboy:checkout(request_pool, true, test_timeout()) of
    %     full ->
    %         case riak_cs_riakc_pool_worker:start_link([]) of
    %             {ok, Pid} ->
    %                 UpdCtx = Ctx#test_context{riakc_pid=Pid,
    %                                           pool_pid=false};
    %             {error, _} ->
    %                 Pid = undefined,
    %                 UpdCtx = Ctx
    %         end;
    %     Pid ->
    %         UpdCtx = Ctx#test_context{riakc_pid=Pid}
    % end,
    % case (catch riakc_pb_socket:test(Pid, test_timeout())) of
    %     pong ->
    %         Available = true;
    %     _ ->
    %         Available = false
    % end,
    {Available, RD, UpdCtx}.

allowed_methods(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"allowed_methods">>),
    {['GET', 'HEAD'], RD, Ctx}.

to_html(ReqData, Ctx) ->
    {Json, RD2, C2} = produce_body(ReqData, Ctx),
    {Json, RD2, C2}.
    % {"TEST OK", ReqData, Ctx}.

produce_body(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"produce_body">>),
    Body = mochijson2:encode(get_disk_stats()),
    ETag = riak_cs_utils:etag_from_binary(riak_cs_utils:md5(Body)),
    RD2 = wrq:set_resp_header("ETag", ETag, RD),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"produce_body">>),
    {Body, RD2, Ctx}.

finish_request(RD, Ctx=#test_context{riakc_pid=undefined}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#test_context{}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [1], []),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finish_request">>, [1], []),
    {true, RD, Ctx#test_context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

get_disk_stats() ->
    DiskData = disksup:get_disk_data(),
    [Root|_] = DiskData,
    {Id, KByte, Capacity} = Root,
    {struct, [{id, Id}, {kb, KByte}, {percent_full, Capacity}]}.