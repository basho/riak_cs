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

-module(stanchion_wm_buckets).

-export([init/1,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         content_types_accepted/2,
         post_is_create/2,
         create_path/2,
         accept_body/2
        ]).

-ignore_xref([init/1,
              service_available/2,
              allowed_methods/2,
              is_authorized/2,
              content_types_accepted/2,
              post_is_create/2,
              create_path/2,
              accept_body/2
             ]).

-include("stanchion.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").


init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #stanchion_context{auth_bypass=AuthBypass}}.

-spec service_available(#wm_reqdata{}, #stanchion_context{}) -> {true, #wm_reqdata{}, #stanchion_context{}}.
service_available(RD, Ctx) ->
    stanchion_wm_utils:service_available(RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.

%% @doc Check that the request is from the admin user
-spec is_authorized(#wm_reqdata{}, #stanchion_context{}) -> {boolean(), #wm_reqdata{}, #stanchion_context{}}.
is_authorized(RD, Ctx=#stanchion_context{auth_bypass=AuthBypass}) ->
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

-spec post_is_create(#wm_reqdata{}, #stanchion_context{}) -> {true, #wm_reqdata{}, #stanchion_context{}}.
post_is_create(_RD, _Ctx) ->
    {true, _RD, _Ctx}.

%% @doc Set the path for the new bucket resource and set
%% the Location header to generate a 201 Created response.
-spec create_path(#wm_reqdata{}, #stanchion_context{}) -> {string(), #wm_reqdata{}, #stanchion_context{}}.
create_path(RD, Ctx) ->
    {wrq:disp_path(RD), RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #stanchion_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #stanchion_context{}}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", accept_body}], RD, Ctx}.

%% @doc Create a bucket from a POST
-spec accept_body(#wm_reqdata{}, #stanchion_context{}) ->
          {true | {halt, pos_integer()},
           #wm_reqdata{}, #stanchion_context{}}.
accept_body(RD, Ctx) ->
    Body = wrq:req_body(RD),
    case stanchion_server:create_bucket(
           jsx:decode(Body, [{labels, atom}])) of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            stanchion_response:api_error(Reason, RD, Ctx)
    end.
