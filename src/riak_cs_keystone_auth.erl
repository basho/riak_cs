%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_keystone_auth).

-behavior(riak_cs_auth).

-compile(export_all).

-export([identify/2, authenticate/4]).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include("oos_api.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(QS_KEYID, "AWSAccessKeyId").
-define(QS_SIGNATURE, "Signature").

%% ===================================================================
%% Public API
%% ===================================================================

-spec identify(#wm_reqdata{}, #context{}) -> failed | {string() | undefined , string()}.
identify(RD, #context{api=s3}) ->
    validate_token(s3, RD);
identify(RD, #context{api=oos}) ->
    validate_token(oos, wrq:get_req_header("x-auth-token", RD)).

-spec authenticate(rcs_user(), {string(), term()}, #wm_reqdata{}, #context{}) ->
                          ok | {error, invalid_authentication}.
authenticate(_User, {_, TokenItems}, _RD, _Ctx) ->
    %% @TODO Expand authentication check for non-operators who may
    %% have access
    %% @TODO Can we rely on role names along or must the service id
    %% also be checked?

    %% Verify that the set of user roles contains a valid
    %% operator role
    TokenRoles = riak_cs_json:value_or_default(
                   riak_cs_json:get(TokenItems, [<<"access">>, <<"user">>, <<"roles">>]),
                   undefined),
    IsDisjoint = ordsets:is_disjoint(token_names(TokenRoles), riak_cs_config:os_operator_roles()),
    case not IsDisjoint of
        true ->
            ok;
        false ->
            {error, invalid_authentication}
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

token_names(undefined) ->
    ordsets:new();
token_names(Roles) ->
    ordsets:from_list(
      [proplists:get_value(<<"name">>, Role, []) || {struct, Role} <- Roles]).

-spec validate_token(s3 | oos, undefined | string()) -> failed | {term(), term()}.
validate_token(_, undefined) ->
    failed;
validate_token(Api, AuthToken) ->
    %% @TODO Check token cache
    %% @TODO Ensure token is not in revoked tokens list

    %% Request token info and determine tenant
    %% Riak CS `key_id' is concatentation of the OS tenant_id and
    %% user_id with a colon delimiter.
    handle_token_info_response(
      request_keystone_token_info(Api, AuthToken)).

-spec request_keystone_token_info(s3 | oos, string() | {term(), term()}) -> term().
request_keystone_token_info(oos, AuthToken) ->
    RequestURI = riak_cs_config:os_tokens_url() ++ AuthToken,
    RequestHeaders = [{"X-Auth-Token", riak_cs_config:os_admin_token()}],
    httpc:request(get, {RequestURI, RequestHeaders}, [], []);
request_keystone_token_info(s3, RD) ->
    {KeyId, Signature}  = case wrq:get_req_header("authorization", RD) of
                              undefined ->
                                  {wrq:get_qs_value(?QS_KEYID, RD), wrq:get_qs_value(?QS_SIGNATURE, RD)};
                              AuthHeader ->
                                  parse_auth_header(AuthHeader)
                          end,
    RequestURI = riak_cs_config:os_s3_tokens_url(),
    STS = base64url:encode_to_string(calculate_sts(RD)),
    RequestBody = riak_cs_json:to_json(?KEYSTONE_S3_AUTH_REQ{
                                           access=list_to_binary(KeyId),
                                           signature=list_to_binary(Signature),
                                           token=list_to_binary(STS)}),
    RequestHeaders = [{"X-Auth-Token", riak_cs_config:os_admin_token()}],
    httpc:request(post, {RequestURI, RequestHeaders, "application/json", RequestBody}, [], []).

handle_token_info_response({ok, {{_HTTPVer, _Status, _StatusLine}, _, TokenInfo}})
  when _Status >= 200, _Status =< 299 ->
    TokenResult = riak_cs_json:from_json(TokenInfo),
    UserId = case riak_cs_json:value_or_default(
               riak_cs_json:get(TokenResult, [<<"access">>, <<"user">>, <<"id">>]),
               undefined) of
                 undefined ->
                     undefined;
                 UserIdBin ->
                     binary_to_list(UserIdBin)
             end,
    {Name, Email, TenantId} = riak_cs_oos_utils:extract_user_data(
                                riak_cs_oos_utils:get_os_user_data(UserId)),
    {TenantId, {{Name, Email, UserId}, TokenResult}};
handle_token_info_response({ok, {{_HTTPVer, _Status, _StatusLine}, _, _}}) ->
    failed;
handle_token_info_response({error, Reason}) ->
    _ = lager:warning("Error occurred requesting token from keystone. Reason: ~p",
                  [Reason]),
    failed.

parse_auth_header("AWS " ++ Key) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {KeyId, KeyData};
        _ -> {undefined, undefined}
    end;
parse_auth_header(_) ->
    {undefined, undefined}.

-spec calculate_sts(term()) -> binary().
calculate_sts(RD) ->
    Headers = riak_cs_wm_utils:normalize_headers(RD),
    AmazonHeaders = riak_cs_wm_utils:extract_amazon_headers(Headers),
    OriginalResource = riak_cs_s3_rewrite:original_resource(RD),
    Resource = case OriginalResource of
        undefined -> []; %% TODO: get noisy here?
        {Path,QS} -> [Path, canonicalize_qs(lists:sort(QS))]
    end,
    Expires = wrq:get_qs_value("Expires", RD),
    case Expires of
        undefined ->
            case proplists:is_defined("x-amz-date", Headers) of
                true ->
                    Date = "\n";
                false ->
                    Date = [wrq:get_req_header("date", RD), "\n"]
            end;
        _ ->
            Date = Expires ++ "\n"
    end,
    case wrq:get_req_header("content-md5", RD) of
        undefined ->
            CMD5 = [];
        CMD5 ->
            ok
    end,
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            ContentType = [];
        ContentType ->
            ok
    end,
    list_to_binary([atom_to_list(wrq:method(RD)), "\n",
                    CMD5,
                    "\n",
                    ContentType,
                    "\n",
                    Date,
                    AmazonHeaders,
                    Resource]).

canonicalize_qs(QS) ->
    canonicalize_qs(QS, []).

canonicalize_qs([], []) ->
    [];
canonicalize_qs([], Acc) ->
    lists:flatten(["?", Acc]);
canonicalize_qs([{K, []}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            canonicalize_qs(T, [K|Acc]);
        false ->
            canonicalize_qs(T)
    end;
canonicalize_qs([{K, V}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            canonicalize_qs(T, [[K, "=", V]|Acc]);
        false ->
            canonicalize_qs(T)
    end.

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

tenant_id_test() ->
    Token = "{\"access\":{\"token\":{\"expires\":\"2012-02-05T00:00:00\","
            "\"id\":\"887665443383838\", \"tenant\":{\"id\":\"1\", \"name\""
            ":\"customer-x\"}}, \"user\":{\"name\":\"joeuser\", \"tenantName\""
            ":\"customer-x\", \"id\":\"1\", \"roles\":[{\"serviceId\":\"1\","
            "\"id\":\"3\", \"name\":\"Member\"}], \"tenantId\":\"1\"}}}",
    InvalidToken = "{\"access\":{\"token\":{\"expires\":\"2012-02-05T00:00:00\","
        "\"id\":\"887665443383838\", \"tenant\":{\"id\":\"1\", \"name\""
        ":\"customer-x\"}}, \"user\":{\"name\":\"joeuser\", \"tenantName\""
        ":\"customer-x\", \"id\":\"1\", \"roles\":[{\"serviceId\":\"1\","
        "\"id\":\"3\", \"name\":\"Member\"}]}}}",
    ?assertEqual({ok, <<"1">>},
                 riak_cs_json:get(riak_cs_json:from_json(Token),
                                  [<<"access">>, <<"user">>, <<"tenantId">>])),
    ?assertEqual({error, not_found},
                 riak_cs_json:get(riak_cs_json:from_json(InvalidToken),
                                  [<<"access">>, <<"user">>, <<"tenantId">>])).

-endif.
