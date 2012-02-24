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
         to_xml/2,
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
    {AuthMod, KeyId, Signature} =
        riak_moss_wm_utils:parse_auth_header(AuthHeader, AuthBypass),
    case riak_moss_utils:get_user(KeyId) of
        {ok, {User, _}} ->
            case AuthMod:authenticate(RD, User?MOSS_USER.key_secret, Signature) of
                ok ->
                    %% Authentication succeeded
                    {false, RD, Ctx#context{user=User}};
                {error, _Reason} ->
                    %% Authentication failed, deny access
                    riak_moss_s3_response:api_error(access_denied, RD, Ctx)
            end;
        {error, no_such_user} ->
            %% Anonymous access not allowed, deny access
            riak_moss_s3_response:api_error(access_denied, RD, Ctx);
        {error, Reason} ->
            lager:error("Retrieval of user record for ~p failed. Reason: ~p", [KeyId, Reason]),
            riak_moss_s3_response:api_error(user_record_unavailable, RD, Ctx)
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
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx) ->
    %% TODO:
    %% This needs to be xml soon, but for now
    %% let's do json.
    {[{"application/xml", to_xml}], RD, Ctx}.


%% TODO:
%% This spec will need to be updated
%% when we change this to allow streaming
%% bodies.
-spec to_xml(term(), term()) ->
    {iolist(), term(), term()}.
to_xml(RD, Ctx=#context{user=User}) ->
    riak_moss_s3_response:list_all_my_buckets_response(User, RD, Ctx).
