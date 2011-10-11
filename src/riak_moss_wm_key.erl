%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(riak_moss_wm_key).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         produce_body/2,
         allowed_methods/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(_Ctx) ->
    {ok, _Ctx}.

service_available(RD, Ctx) ->
    riak_moss_wm_utils:service_available(RD, Ctx).

malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

forbidden(RD, Ctx) ->
    %% TODO:
    %% Actually check to see if this
    %% is a real account.
    {false, RD, Ctx}.

%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    %% TODO: add PUT, POST, DELETE
    {['HEAD', 'GET'], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.

    %% For now just return plaintext
    {[{"text/plain", produce_body}], RD, Ctx}.

produce_body(RD, Ctx) ->
    %% TODO:
    %% This is really just a placeholder
    %% return value.
    Return_body = <<>>,
    {Return_body, RD, Ctx}.

%% TODO:
%% Add content_types_accepted when we add
%% in PUT and POST requests.
