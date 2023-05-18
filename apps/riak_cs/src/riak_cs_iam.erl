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

-module(riak_cs_iam).

-export([create_role/1,
         delete_role/1,
         get_role/2,
         fix_permissions_boundary/1,
         create_saml_provider/1,
         delete_saml_provider/1,
         get_saml_provider/2,
         exprec_role/1,
         exprec_saml_provider/1
        ]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("kernel/include/logger.hrl").


-spec create_role(maps:map()) -> {ok, role()} | {error, already_exists | term()}.
create_role(Specs) ->
    Encoded = jsx:encode(Specs),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:create_role(
               "application/json",
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec delete_role(string()) -> ok | {error, term()}.
delete_role(RoleId) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:delete_role(RoleId, [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec get_role(binary(), pid()) -> {ok, ?IAM_ROLE{}} | {error, term()}.
get_role(RoleName, RcPid) ->
    case riak_cs_riak_client:get_role(RcPid, RoleName) of
        {ok, Obj} ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.

from_riakc_obj(Obj) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            case riakc_obj:get_value(Obj) of
                ?DELETED_MARKER ->
                    {error, not_found};
                V ->
                    {ok, binary_to_term(V)}
            end;
        0 ->
            error(no_value);
        N ->
            logger:warning("object with key ~p has ~b siblings", [riakc_obj:key(Obj), N]),
            Values = [V || V <- riakc_obj:get_values(Obj),
                           V /= <<>>,   %% tombstone
                           V /= ?DELETED_MARKER],
            case length(Values) of
                0 ->
                    {error, not_found};
                _ ->
                    {ok, binary_to_term(hd(Values))}
            end
    end.


-spec exprec_role(maps:map()) -> ?IAM_ROLE{}.
exprec_role(Map) ->
    Role0 = ?IAM_ROLE{permissions_boundary = PB0,
                      role_last_used = LU0,
                      tags = TT0} = exprec:frommap_role_v1(Map),
    TT = [exprec:frommap_tag(T) || T <- TT0],
    LU = case LU0 of
             Undefined1 when Undefined1 =:= null;
                             Undefined1 =:= undefined ->
                 undefined;
             _ ->
                 exprec:frommap_role_last_used(LU0)
         end,
    PB = case PB0 of
             Undefined2 when Undefined2 =:= null;
                             Undefined2 =:= undefined ->
                 undefined;
             _ ->
                 exprec:frommap_permissions_boundary(PB0)
         end,
    Role0?IAM_ROLE{permissions_boundary = PB,
                   role_last_used = LU,
                   tags = TT}.

-spec exprec_saml_provider(maps:map()) -> ?IAM_SAML_PROVIDER{}.
exprec_saml_provider(Map) ->
    P0 = ?IAM_SAML_PROVIDER{tags = TT0} = exprec:frommap_saml_provider_v1(Map),
    TT = [exprec:frommap_tag(T) || T <- TT0],
    P0?IAM_SAML_PROVIDER{tags = TT}.

%% CreateRole takes a string for PermissionsBoundary parameter, which
%% needs to become part of a structure (and handled and exported thus), so:
-spec fix_permissions_boundary(maps:map()) -> maps:map().
fix_permissions_boundary(#{permissions_boundary := A} = Map) when not is_map(A) ->
    maps:update(permissions_boundary, #{permissions_boundary_arn => A}, Map);
fix_permissions_boundary(Map) ->
    Map.


-spec create_saml_provider(maps:map()) -> {ok, {Arn::string(), [tag()]}} | {error, term()}.
create_saml_provider(Specs) ->
    Encoded = jsx:encode(
                enrich_with_valid_until(Specs)),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:create_saml_provider(
               "application/json",
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).

enrich_with_valid_until(#{saml_metadata_document := _D} = Specs) ->
    ?LOG_WARNING("STUB need to extract ValidUntil here from SAMLMetadataDocument", []),
    TS = calendar:system_time_to_local_time(os:system_time(second) + 3600*24, second),
    maps:put(valid_until, rts:iso8601(TS), Specs).

-spec delete_saml_provider(string()) -> ok | {error, term()}.
delete_saml_provider(Arn) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:delete_saml_provider(
               binary_to_list(base64:encode(Arn)), [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec get_saml_provider(string(), pid()) -> {ok, ?IAM_SAML_PROVIDER{}} | {error, term()}.
get_saml_provider(Arn, RcPid) ->
    BinKey = list_to_binary(Arn),
    case riak_cs_riak_client:get_saml_provider(RcPid, BinKey) of
        {ok, Obj} ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.


handle_response({ok, Returnable}) ->
    {ok, Returnable};
handle_response(ok) ->
    ok;
handle_response({error, {error_status, _, _, ErrorDoc}}) ->
    riak_cs_s3_response:error_response(ErrorDoc);
handle_response({error, _} = Error) ->
    Error.



-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
