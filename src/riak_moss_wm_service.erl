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

-module(riak_moss_wm_service).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         malformed_request/2,
         produce_body/2,
         allowed_methods/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec init(term()) -> {ok, term()}.
init(_Ctx) ->
    {ok, _Ctx}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    %% TODO:
    %% At some point in the future
    %% this needs to check if we have
    %% an alive Riak server. Although
    %% maybe it makes more sense to be
    %% optimistic and wait untl we actually
    %% check the ACL?


    %% For now we just always
    %% return true
    {true, RD, Ctx}.

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.


%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
-spec forbidden(term(), term()) -> {false, term(), term()}.
forbidden(RD, Ctx) ->
    %% TODO:
    %% Actually check to see if this
    %% is a real account.
    {false, RD, Ctx}.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    %% TODO:
    %% The docs seem to suggest GET is the only
    %% allowed method. It's worth checking to see
    %% if HEAD is supported too.
    {['GET'], RD, Ctx}.

-spec content_types_provided(term(), term()) ->
    {[{atom(), module()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    %% TODO:
    %% This needs to be xml soon, but for now
    %% let's do json.
    {[{"application/json", produce_body}], RD, Ctx}.


%% TODO:
%% This spec will need to be updated
%% when we change this to allow streaming
%% bodies.
-spec produce_body(term(), term()) ->
    {iolist(), term(), term()}.
produce_body(RD, Ctx) ->
    %% TODO:
    %% Here is where we need to actually
    %% extract the user from the auth
    %% and retrieve their list of
    %% buckets. For now we just return
    %% an empty list.
    Return_body = [],
    {mochijson2:encode(Return_body), RD, Ctx}.
