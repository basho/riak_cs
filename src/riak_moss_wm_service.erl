%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec malformed_request(term(), term()) -> {false, term(), term()}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case riak_moss_wm_utils:parse_auth_header(AuthHeader, AuthBypass) of
        {ok, AuthMod, Args} ->
            case AuthMod:authenticate(RD, Args) of
                {ok, User} ->
                    %% Authentication succeeded
                    {false, RD, Ctx#context{user=User}};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    {true, RD, Ctx}
            end
    end.

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
