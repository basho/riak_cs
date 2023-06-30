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

-module(stanchion_wm_bucket).

-export([init/1,
         service_available/2,
         is_authorized/2,
         content_types_provided/2,
         malformed_request/2,
         to_xml/2,
         allowed_methods/2,
         content_types_accepted/2,
         %% accept_body/2,
         delete_resource/2
        ]).
-ignore_xref([init/1,
              service_available/2,
              is_authorized/2,
              content_types_provided/2,
              malformed_request/2,
              to_xml/2,
              allowed_methods/2,
              content_types_accepted/2,
              %% accept_body/2,
              delete_resource/2
             ]).

-include("stanchion.hrl").
-include_lib("webmachine/include/webmachine.hrl").


init(Config) ->
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #stanchion_context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    stanchion_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @doc Check that the request is from the admin user
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

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['GET', 'PUT', 'DELETE'], RD, Ctx}.

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    %% @TODO Add JSON support
    {[{"application/xml", to_xml}], RD, Ctx}.

content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        CType ->
            {[{CType, accept_body}], RD, Ctx}
    end.


-spec to_xml(term(), term()) ->
    {{'halt', _}, #wm_reqdata{}, term()}.
to_xml(RD, Ctx) ->
    Bucket = wrq:path_info(bucket, RD),
    stanchion_response:list_buckets_response(Bucket, RD, Ctx).

-spec delete_resource(term(), term()) -> {'true' | {'halt', term()}, #wm_reqdata{}, term()}.
delete_resource(ReqData, Ctx) ->
    Bucket = list_to_binary(wrq:path_info(bucket, ReqData)),
    RequesterId = list_to_binary(wrq:get_qs_value("requester", "", ReqData)),
    case stanchion_server:delete_bucket(Bucket, RequesterId) of
        ok ->
            {true, ReqData, Ctx};
        {error, Reason} ->
            stanchion_response:api_error(Reason, ReqData, Ctx)
    end.
