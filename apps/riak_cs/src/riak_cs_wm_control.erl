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

-module(riak_cs_wm_control).

%% webmachine resource exports
-export([init/1,
         allowed_methods/2,
         content_types_provided/2,
         service_available/2
        ]).
-ignore_xref([init/1,
              encodings_provided/2,
              content_types_provided/2,
              service_available/2
             ]).

-include("riak_cs.hrl").

init(Props) ->
    AuthBypass = not proplists:get_value(admin_auth_enabled, Props),
    {ok, #rcs_web_context{auth_bypass = AuthBypass}}.

-spec allowed_methods(#wm_reqdata{}, #rcs_web_context{}) -> {[atom()], #wm_reqdata{}, #rcs_web_context{}}.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, #rcs_web_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_provided(RD, Context) ->
    [{"text/html", produce_html}].

service_available(RD, Ctx) ->
    {riak_cs_config:control_ui_enabled(), RD, Ctx}.

produce_html(RD, Ctx) ->
    Body = 
    {Body, RD, Ctx}.
