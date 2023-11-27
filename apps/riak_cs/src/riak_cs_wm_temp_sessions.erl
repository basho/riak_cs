%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_wm_temp_sessions).

-export([init/1,
         service_available/2,
         forbidden/2,
         options/2,
         content_types_provided/2,
         allowed_methods/2,
         finish_request/2,
         produce_xml/2
        ]).

-include("riak_cs_web.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

init(Config) ->
    AuthBypass = not proplists:get_value(admin_auth_enabled, Config),
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    {ok, #rcs_web_context{auth_bypass = AuthBypass,
                          api = Api,
                          response_module = RespModule}}.

-spec options(#wm_reqdata{}, #rcs_web_context{}) -> {[{string(), string()}], #wm_reqdata{}, #rcs_web_context{}}.
options(RD, Ctx) ->
    {riak_cs_wm_utils:cors_headers(), RD, Ctx}.

-spec service_available(#wm_reqdata{}, #rcs_web_context{}) -> {true, #wm_reqdata{}, #rcs_web_context{}}.
service_available(RD, Ctx) ->
    riak_cs_wm_utils:service_available(
      wrq:set_resp_headers(riak_cs_wm_utils:cors_headers(), RD), Ctx).

-spec allowed_methods(#wm_reqdata{}, #rcs_web_context{}) -> {[atom()], #wm_reqdata{}, #rcs_web_context{}}.
allowed_methods(RD, Ctx) ->
    {['GET', 'OPTIONS'], RD, Ctx}.


-spec forbidden(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #rcs_web_context{}}.
forbidden(RD, Ctx) ->
    case wrq:method(RD) of
        'OPTIONS' ->
            {false, RD, Ctx};
        _ ->
            forbidden2(RD, Ctx)
    end.
forbidden2(RD, Ctx = #rcs_web_context{auth_bypass = AuthBypass}) ->
    riak_cs_wm_utils:find_and_auth_admin(RD, Ctx, AuthBypass).

content_types_provided(RD, Ctx) ->
    {[{?XML_TYPE, produce_xml}], RD, Ctx}.

-spec finish_request(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, term()}, term(), term()}.
finish_request(RD, Ctx=#rcs_web_context{riak_client = undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx=#rcs_web_context{riak_client=RcPid}) ->
    riak_cs_riak_client:checkin(RcPid),
    {true, RD, Ctx#rcs_web_context{riak_client = undefined}}.


produce_xml(RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                       response_module = ResponseMod,
                                       request_id = RequestId}) ->
    Form = mochiweb_util:parse_qs(wrq:req_body(RD)),
    MaxItems = proplists:get_value("MaxItems", Form),
    Marker = proplists:get_value("Marker", Form),
    case riak_cs_temp_sessions:list(
           RcPid, #list_temp_sessions_request{request_id = RequestId,
                                              max_items = MaxItems,
                                              marker = Marker}) of
        {ok, #{temp_sessions := TempSessions,
               marker := NewMarker,
               is_truncated := IsTruncated}} ->
            Doc = riak_cs_xml:to_xml(
                    #list_temp_sessions_response{temp_sessions = TempSessions,
                                                 request_id = RequestId,
                                                 marker = NewMarker,
                                                 is_truncated = IsTruncated}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end.
