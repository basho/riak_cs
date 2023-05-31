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
         find_role/2,

         create_saml_provider/1,
         delete_saml_provider/1,
         get_saml_provider/2,
         find_saml_provider/2,
         saml_provider_entity_id/1,

         create_policy/1,
         delete_policy/1,
         get_policy/2,
         find_policy/2,
         merge_policies/1,

         fix_permissions_boundary/1,
         exprec_role/1,
         exprec_policy/1,
         exprec_saml_provider/1
        ]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("xmerl/include/xmerl.hrl").
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

-spec delete_role(binary()) -> ok | {error, term()}.
delete_role(Arn) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:delete_role(Arn, [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec get_role(binary(), pid()) -> {ok, ?IAM_ROLE{}} | {error, term()}.
get_role(Arn, RcPid) ->
    case riak_cs_riak_client:get_role(RcPid, Arn) of
        {ok, Obj} ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.

-spec find_role(maps:map() | Name::binary(), pid()) -> {ok, role()} | {error, notfound | term()}.
find_role(Name, RcPid) when is_binary(Name) ->
    find_role(#{name => Name}, RcPid);
find_role(#{name := Name}, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_ROLE_BUCKET, ?ROLE_NAME_INDEX, Name),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_role(Key, RcPid);
        {error, Reason} ->
            logger:error("Failed to find role by name ~s: ~p", [Name, Reason]),
            {error, Reason}
    end;
find_role(#{path := Path}, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_ROLE_BUCKET, ?ROLE_PATH_INDEX, Path),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_role(Key, RcPid);
        {error, Reason} ->
            logger:error("Failed to find role by path ~s: ~p", [Path, Reason]),
            {error, Reason}
    end.


-spec create_policy(maps:map()) -> {ok, policy()} | {error, already_exists | term()}.
create_policy(Specs) ->
    Encoded = jsx:encode(Specs),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:create_policy(
               "application/json",
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec delete_policy(binary()) -> ok | {error, term()}.
delete_policy(Arn) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:delete_policy(Arn, [{auth_creds, AdminCreds}]),
    handle_response(Result).

-spec get_policy(binary(), pid()) -> {ok, ?IAM_POLICY{}} | {error, term()}.
get_policy(Arn, RcPid) ->
    case riak_cs_riak_client:get_policy(RcPid, Arn) of
        {ok, Obj} ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.

-spec find_policy(maps:map() | Name::binary(), pid()) -> {ok, policy()} | {error, notfound | term()}.
find_policy(Name, RcPid) when is_binary(Name) ->
    get_policy(#{name => Name}, RcPid);
find_policy(#{name := Name}, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_POLICY_BUCKET, ?POLICY_NAME_INDEX, Name),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_policy(Key, RcPid);
        {error, Reason} ->
            logger:error("Failed to find managed policy by name ~s: ~p", [Name, Reason]),
            {error, Reason}
    end.


-spec merge_policies([policy()]) -> {ok, policy()} | {error, term()}.
merge_policies(PP) ->
    merge_policies(PP, []).
merge_policies([], Q) ->
    {ok, Q};
merge_policies([P1|PP], Q) ->
    merge_policies(PP, merge_these_two(P1, Q)).
merge_these_two(P1, P2) ->
    ?LOG_DEBUG("STUB ~p ~p", [P1, P2]),
    
    P1.



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


-spec delete_saml_provider(binary()) -> ok | {error, term()}.
delete_saml_provider(Arn) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:delete_saml_provider(
               Arn, [{auth_creds, AdminCreds}]),
    handle_response(Result).


-spec get_saml_provider(binary(), pid()) -> {ok, ?IAM_SAML_PROVIDER{}} | {error, term()}.
get_saml_provider(Arn, RcPid) ->
    case riak_cs_riak_client:get_saml_provider(RcPid, Arn) of
        {ok, Obj} ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.

-spec find_saml_provider(maps:map() | Arn::binary(), pid()) -> {ok, saml_provider()} | {error, notfound | term()}.
find_saml_provider(Name, RcPid) when is_binary(Name) ->
    find_saml_provider(#{name => Name}, RcPid);
find_saml_provider(#{name := Name}, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_POLICY_BUCKET, ?SAMLPROVIDER_NAME_INDEX, Name),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_saml_provider(Key, RcPid);
        {error, Reason} ->
            logger:error("Failed to find SAML Provider by name ~s: ~p", [Name, Reason]),
            {error, Reason}
    end;
find_saml_provider(#{entity_id := EntityID}, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_POLICY_BUCKET, ?SAMLPROVIDER_ENTITYID_INDEX, EntityID),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_saml_provider(Key, RcPid);
        {error, Reason} ->
            logger:error("Failed to find SAML Provider by EntityID ~s: ~p", [EntityID, Reason]),
            {error, Reason}
    end.

saml_provider_entity_id(?IAM_SAML_PROVIDER{saml_metadata_document = D}) ->
    {#xmlElement{content = RootContent}, _} = xmerl_scan:string(binary_to_list(D)),
    ?LOG_DEBUG("STUB ~p", [RootContent]),
    
    <<"fafa.org">>.




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

-spec exprec_policy(maps:map()) -> ?IAM_POLICY{}.
exprec_policy(Map) ->
    Policy0 = ?IAM_ROLE{tags = TT0} = exprec:frommap_policy_v1(Map),
    TT = [exprec:frommap_tag(T) || T <- TT0],
    Policy0?IAM_ROLE{tags = TT}.

-spec exprec_saml_provider(maps:map()) -> ?IAM_SAML_PROVIDER{}.
exprec_saml_provider(Map) ->
    P0 = ?IAM_SAML_PROVIDER{tags = TT0} = exprec:frommap_saml_provider_v1(Map),
    TT = [exprec:frommap_tag(T) || T <- TT0],
    P0?IAM_SAML_PROVIDER{tags = TT}.


handle_response({ok, Returnable}) ->
    {ok, Returnable};
handle_response(ok) ->
    ok;
handle_response({error, {error_status, _, _, ErrorDoc}}) ->
    riak_cs_aws_response:error_response(ErrorDoc);
handle_response({error, _} = Error) ->
    Error.



-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
