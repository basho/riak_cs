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

-module(stanchion_wm_versioning).

-export([init/1,
         service_available/2,
         is_authorized/2,
         malformed_request/2,
         allowed_methods/2,
         content_types_accepted/2,
         accept_body/2]).

-include("stanchion.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    % eval every atoms to make binary_to_existing_atom/2 work.
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    stanchion_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @doc Check that the request is from the admin user
is_authorized(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case stanchion_wm_utils:parse_auth_header(AuthHeader, AuthBypass) of
        {ok, AuthMod, Args} ->
            case AuthMod:authenticate(RD, Args) of
                ok ->
                    {true, RD, Ctx};
                {error, _Reason} ->
                    stanchion_response:api_error(access_denied, RD, Ctx)
            end
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['PUT'], RD, Ctx}.

-spec content_types_accepted(term(), term()) ->
                                    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    {[{"application/json", accept_body}], RD, Ctx}.

%% @doc Process the request body on `PUT'.
accept_body(RD, Ctx) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    Body = wrq:req_body(RD),
    %% @TODO Handle json decoding exceptions
    ParsedBody = mochijson2:decode(Body),
    FieldList = stanchion_wm_utils:json_to_proplist(ParsedBody),
    case stanchion_server:set_bucket_versioning(Bucket,
                                                FieldList) of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            stanchion_response:api_error(Reason, RD, Ctx)
    end.
