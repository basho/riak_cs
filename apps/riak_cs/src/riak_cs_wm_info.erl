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

-module(riak_cs_wm_info).

-export([init/1,
         allowed_methods/2,
         service_available/2,
         options/2,
         forbidden/2,
         content_types_provided/2,
         to_json/2
        ]).

-ignore_xref([init/1,
              allowed_methods/2,
              service_available/2,
              options/2,
              forbidden/2,
              content_types_provided/2,
              to_json/2
             ]).

-include("riak_cs.hrl").

init(_Config) ->
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    {ok, #rcs_web_context{api = Api,
                          response_module = RespModule,
                          auth_bypass = riak_cs_config:auth_bypass()}}.

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
    {[{?JSON_TYPE, to_json}], RD, Ctx}.

-spec to_json(#wm_reqdata{}, #rcs_web_context{}) -> {binary(), #wm_reqdata{}, #rcs_web_context{}}.
to_json(RD, Ctx) ->
    {jsx:encode(gather_info()), RD, Ctx}.

gather_info() ->
    {MS, _} = erlang:statistics(wall_clock),
    St = MS div 1000,
    S = St rem 60,
    Mt = St div 60,
    M = Mt rem 60,
    Ht = Mt div 60,
    H = Ht rem 24,
    Dt = Ht div 24,
    D = Dt,
    Str = case {D, H, M} of
              {A, _, _} when A > 0 -> io_lib:format("~b day~s, ~b hour~s, ~b minute~s, ~b sec", [D, s(D), H, s(H), M, s(M), S]);
              {_, A, _} when A > 0 -> io_lib:format("~b hour~s, ~b minute~s, ~b sec", [H, s(H), M, s(M), S]);
              {_, _, A} when A > 0 -> io_lib:format("~b minute~s, ~b sec", [M, s(M), S]);
              _ -> io_lib:format("~b sec", [S])
          end,
    #{version => list_to_binary(?RCS_VERSION_STRING),
      uptime => iolist_to_binary(Str)
     }.

s(1) -> "";
s(_) -> "s".
