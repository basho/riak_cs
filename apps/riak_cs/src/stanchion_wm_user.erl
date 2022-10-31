%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(stanchion_wm_user).

-export([init/1,
         service_available/2,
         allowed_methods/2,
         is_authorized/2,
         create_path/2,
         content_types_accepted/2,
         accept_body/2]).

-include("stanchion.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    stanchion_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['PUT'], RD, Ctx}.

%% @doc Check that the request is from the admin user
is_authorized(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
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

%% @doc Set the path for the new user resource and set
%% the Location header to generate a 201 Created response.
%% -spec create_path(term(), term()) -> {string(), term(), term()}.
create_path(RD, Ctx) ->
    {wrq:disp_path(RD), RD, Ctx}.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", accept_body}], RD, Ctx}.

%% @doc Create a user from a POST
-spec accept_body(term(), term()) ->
                         {true | {halt, pos_integer()},
                          term(),
                          term()}.
accept_body(RD, Ctx) ->
    Body = wrq:req_body(RD),
    KeyId = wrq:path_info(key_id, RD),
    %% @TODO Handle json decoding exceptions
    ParsedBody = mochijson2:decode(Body),
    FieldList = stanchion_wm_utils:json_to_proplist(ParsedBody),
    case stanchion_server:update_user(KeyId, FieldList) of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            stanchion_response:api_error(Reason, RD, Ctx)
    end.
