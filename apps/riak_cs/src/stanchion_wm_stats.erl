%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

-module(stanchion_wm_stats).

-export([init/1,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         content_types_provided/2,
         produce_body/2
        ]).

-ignore_xref([init/1,
              service_available/2,
              allowed_methods/2,
              is_authorized/2,
              content_types_provided/2,
              produce_body/2
             ]).

-include("stanchion.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #stanchion_context{auth_bypass=AuthBypass}}.

-spec service_available(#wm_reqdata{}, #stanchion_context{}) ->
          {true, #wm_reqdata{}, #stanchion_context{}}.
service_available(RD, Ctx) ->
    stanchion_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(#wm_reqdata{}, #stanchion_context{}) ->
          {[atom()], #wm_reqdata{}, #stanchion_context{}}.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.

%% @doc Check that the request is from the admin user
is_authorized(RD, Ctx) ->
    #stanchion_context{auth_bypass = AuthBypass} = Ctx,
    AuthHeader = wrq:get_req_header("authorization", RD),
    case stanchion_wm_utils:parse_auth_header(AuthHeader, AuthBypass) of
        {ok, AuthMod, Args} ->
            case AuthMod:authenticate(RD, Args) of
                ok ->
                    %% Authentication succeeded
                    {true, RD, Ctx};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    stanchion_response:api_error(access_denied, RD, Ctx)
            end
    end.

-spec content_types_provided(#wm_reqdata{}, #stanchion_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #stanchion_context{}}.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_body}],
     RD, Ctx}.

-spec produce_body(#wm_reqdata{}, #stanchion_context{}) ->
          {binary(), #wm_reqdata{}, #stanchion_context{}}.
produce_body(RD, Ctx) ->
    Stats = stanchion_stats:get_stats(),
    JSON = jsx:encode(Stats),
    {JSON, RD, Ctx}.
