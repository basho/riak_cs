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

-export([get_user/2,
         find_user/2,
         update_user/1,

         create_role/1,
         update_role/1,
         delete_role/1,
         get_role/2,
         find_role/2,

         create_saml_provider/1,
         delete_saml_provider/1,
         get_saml_provider/2,
         find_saml_provider/2,
         parse_saml_provider_idp_metadata/1,

         create_policy/1,
         update_policy/1,
         delete_policy/1,
         get_policy/2,
         find_policy/2,
         attach_role_policy/3,
         detach_role_policy/3,
         attach_user_policy/3,
         detach_user_policy/3,

         fix_permissions_boundary/1,
         exprec_user/1,
         exprec_bucket/1,
         exprec_role/1,
         exprec_policy/1,
         exprec_saml_provider/1,
         unarm/1
        ]).

-include("moss.hrl").
-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").


-spec get_user(flat_arn(), pid()) -> {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, notfound}.
get_user(Arn, RcPid) ->
    case riak_cs_riak_client:get_user(RcPid, Arn) of
        {ok, {Obj, KDB}} ->
            {ok, {riak_cs_user:from_riakc_obj(Obj, KDB), Obj}};
        ER ->
            ER
    end.

-spec find_user(maps:map(), pid()) -> {ok, rcs_user()} | {error, notfound}.
find_user(#{name := A}, RcPid) ->
    find_user(?USER_NAME_INDEX, A, RcPid);
find_user(#{canonical_id := A}, RcPid) ->
    find_user(?USER_ID_INDEX, A, RcPid);
find_user(#{key_id := A}, RcPid) ->
    find_user(?USER_KEYID_INDEX, A, RcPid);
find_user(#{email := A}, RcPid) ->
    find_user(?USER_EMAIL_INDEX, A, RcPid).

find_user(Index, A, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    Res = riakc_pb_socket:get_index_eq(Pbc, ?USER_BUCKET, Index, A),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Arn|_]}} ->
            get_user(Arn, RcPid);
        {error, Reason} ->
            logger:error("Failed to find user by index ~s with key ~s: ~p", [Index, A, Reason]),
            {error, Reason}
    end.

-spec update_user(rcs_user()) -> ok | {error, term()}.
update_user(U = ?IAM_USER{key_id = KeyId}) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:update_user("application/json",
                                KeyId,
                                riak_cs_json:to_json(U),
                                [{auth_creds, AdminCreds}]),
    handle_response(Result).


-spec create_role(maps:map()) -> {ok, role()} | {error, already_exists | term()}.
create_role(Specs) ->
    Encoded = riak_cs_json:to_json(exprec:frommap_role_v1(Specs)),
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
create_policy(Specs = #{policy_document := D}) ->
    case riak_cs_aws_policy:policy_from_json(D) of
        {ok, _} ->
            Encoded = riak_cs_json:to_json(exprec_policy(Specs)),
            {ok, AdminCreds} = riak_cs_config:admin_creds(),
            Result = velvet:create_policy(
                       "application/json",
                       Encoded,
                       [{auth_creds, AdminCreds}]),
            handle_response(Result);
        ER ->
            ER
    end.

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
    find_policy(#{name => Name}, RcPid);
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

-spec attach_role_policy(binary(), binary(), pid()) ->
          ok | {error, error_reason()}.
attach_role_policy(PolicyArn, RoleName, RcPid) ->
    case find_role(#{name => RoleName}, RcPid) of
        {ok, Role = ?IAM_ROLE{attached_policies = PP}} ->
            case lists:member(PolicyArn, PP) of
                true ->
                    ok;
                false ->
                    case get_policy(PolicyArn, RcPid) of
                        {ok, Policy = ?IAM_POLICY{is_attachable = true,
                                                  attachment_count = AC}} ->
                            case update_role(Role?IAM_ROLE{attached_policies = lists:usort([PolicyArn | PP])}) of
                                ok ->
                                    update_policy(Policy?IAM_POLICY{attachment_count = AC + 1});
                                ER1 ->
                                    ER1
                            end;
                        {ok, ?IAM_POLICY{}} ->
                            {error, policy_not_attachable};
                        {error, notfound} ->
                            {error, no_such_policy}
                    end
            end;
        {error, notfound} ->
            {error, no_such_role}
    end.

-spec detach_role_policy(binary(), binary(), pid()) ->
          ok | {error, unmodifiable_entity}.
detach_role_policy(PolicyArn, RoleName, RcPid) ->
    case find_role(#{name => RoleName}, RcPid) of
        {ok, Role = ?IAM_ROLE{attached_policies = PP}} ->
            case lists:member(PolicyArn, PP) of
                false ->
                    ok;
                true ->
                    case get_policy(PolicyArn, RcPid) of
                        {ok, Policy = ?IAM_POLICY{attachment_count = AC}} ->
                            case update_role(Role?IAM_ROLE{attached_policies = lists:delete(PolicyArn, PP)}) of
                                ok ->
                                    update_policy(Policy?IAM_POLICY{attachment_count = AC - 1});
                                ER1 ->
                                    ER1
                            end;
                        {error, notfound} ->
                            {error, no_such_policy}
                    end
            end;
        {error, notfound} ->
            {error, no_such_role}
    end.

-spec attach_user_policy(binary(), binary(), pid()) ->
          ok | {error, error_reason()}.
attach_user_policy(PolicyArn, UserName, RcPid) ->
    case find_user(#{name => UserName}, RcPid) of
        {ok, {User = ?RCS_USER{attached_policies = PP}, _}} ->
            case lists:member(PolicyArn, PP) of
                true ->
                    ok;
                false ->
                    case get_policy(PolicyArn, RcPid) of
                        {ok, Policy = ?IAM_POLICY{is_attachable = true,
                                                  attachment_count = AC}} ->
                            case update_user(User?RCS_USER{attached_policies = lists:usort([PolicyArn | PP])}) of
                                ok ->
                                    update_policy(Policy?IAM_POLICY{attachment_count = AC + 1});
                                ER1 ->
                                    ER1
                            end;
                        {ok, ?IAM_POLICY{}} ->
                            {error, policy_not_attachable};
                        {error, notfound} ->
                            {error, no_such_policy}
                    end
            end;
        {error, notfound} ->
            {error, no_such_user}
    end.

-spec detach_user_policy(binary(), binary(), pid()) ->
          ok | {error, unmodifiable_entity}.
detach_user_policy(PolicyArn, UserName, RcPid) ->
    case find_user(#{name => UserName}, RcPid) of
        {ok, User = ?RCS_USER{attached_policies = PP}} ->
            case lists:member(PolicyArn, PP) of
                false ->
                    ok;
                true ->
                    case get_policy(PolicyArn, RcPid) of
                        {ok, Policy = ?IAM_POLICY{attachment_count = AC}} ->
                            case update_user(User?RCS_USER{attached_policies = lists:delete(PolicyArn, PP)}) of
                                ok ->
                                    update_policy(Policy?IAM_POLICY{attachment_count = AC - 1});
                                ER1 ->
                                    ER1
                            end;
                        {error, notfound} ->
                            {error, no_such_policy}
                    end
            end;
        {error, notfound} ->
            {error, no_such_user}
    end.

update_role(R = ?IAM_ROLE{arn = Arn}) ->
    Encoded = riak_cs_json:to_json(R),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:update_role(
               "application/json",
               Arn,
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).

update_policy(A = ?IAM_POLICY{arn = Arn}) ->
    Encoded = riak_cs_json:to_json(A),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:update_policy(
               "application/json",
               Arn,
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).


%% CreateRole takes a string for PermissionsBoundary parameter, which
%% needs to become part of a structure (and handled and exported thus), so:
-spec fix_permissions_boundary(maps:map()) -> maps:map().
fix_permissions_boundary(#{permissions_boundary := A} = Map) when not is_map(A) ->
    maps:update(permissions_boundary, #{permissions_boundary_arn => A}, Map);
fix_permissions_boundary(Map) ->
    Map.


-spec create_saml_provider(maps:map()) -> {ok, {Arn::string(), [tag()]}} | {error, term()}.
create_saml_provider(Specs) ->
    Encoded = riak_cs_json:to_json(exprec_saml_provider(Specs)),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Result = velvet:create_saml_provider(
               "application/json",
               Encoded,
               [{auth_creds, AdminCreds}]),
    handle_response(Result).


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
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_SAMLPROVIDER_BUCKET, ?SAMLPROVIDER_NAME_INDEX, Name),
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
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_SAMLPROVIDER_BUCKET, ?SAMLPROVIDER_ENTITYID_INDEX, EntityID),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_saml_provider(Key, RcPid);
        {error, Reason} ->
            logger:error("Failed to find SAML Provider by EntityID ~s: ~p", [EntityID, Reason]),
            {error, Reason}
    end.

-spec parse_saml_provider_idp_metadata(saml_provider()) ->
          {ok, saml_provider()} | {error, invalid_metadata_document}.
parse_saml_provider_idp_metadata(?IAM_SAML_PROVIDER{saml_metadata_document = D} = P) ->
    {Xml, _} = xmerl_scan:string(binary_to_list(D)),
    Ns = [{namespace, [{"md", "urn:oasis:names:tc:SAML:2.0:metadata"}]}],
    [#xmlAttribute{value = EntityID}] = xmerl_xpath:string("/md:EntityDescriptor/@entityID", Xml, Ns),
    [#xmlAttribute{value = ValidUntil}] = xmerl_xpath:string("/md:EntityDescriptor/@validUntil", Xml, Ns),
    [#xmlAttribute{value = ConsumeUri}] = xmerl_xpath:string("/md:EntityDescriptor/md:IDPSSODescriptor/md:SingleSignOnService/@Location", Xml, Ns),
    case extract_certs(
           xmerl_xpath:string("/md:EntityDescriptor/md:IDPSSODescriptor/md:KeyDescriptor", Xml, Ns), []) of
        {ok, Certs} ->
            {ok, P?IAM_SAML_PROVIDER{entity_id = list_to_binary(EntityID),
                                     valid_until = calendar:rfc3339_to_system_time(ValidUntil, [{unit, millisecond}]),
                                     consume_uri = ConsumeUri,
                                     certificates = Certs}};
        {error, Reason} ->
            logger:warning("Problem parsing certificate in IdP metadata: ~p", [Reason]),
            {error, invalid_metadata_document}
    end.

extract_certs([], Q) ->
    {ok, Q};
extract_certs([#xmlElement{content = RootContent,
                           attributes = RootAttributes} | Rest], Q) ->
    [#xmlElement{content = KeyInfoContent}|_] = riak_cs_xml:find_elements('ds:KeyInfo', RootContent),
    [#xmlElement{content = X509DataContent}|_] = riak_cs_xml:find_elements('ds:X509Data', KeyInfoContent),
    [#xmlElement{content = X509CertificateContent}|_] = riak_cs_xml:find_elements('ds:X509Certificate', X509DataContent),
    [#xmlText{value = CertDataS}] = [T || T = #xmlText{} <- X509CertificateContent],
    [#xmlAttribute{value = TypeS}|_] = [A || A = #xmlAttribute{name = use} <- RootAttributes],
    Type = list_to_atom(TypeS),
    FP = esaml_util:convert_fingerprints(
           [crypto:hash(sha, base64:decode(CertDataS))]),
    case riak_cs_utils:parse_x509_cert(list_to_binary(CertDataS)) of
        {ok, Certs} ->
            extract_certs(Rest, [{Type, Certs, FP} | Q]);
        ER ->
            ER
    end.


from_riakc_obj(Obj) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            case riakc_obj:get_value(Obj) of
                ?DELETED_MARKER ->
                    {error, notfound};
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
                    {error, notfound};
                _ ->
                    {ok, binary_to_term(hd(Values))}
            end
    end.


-spec exprec_user(maps:map()) -> ?IAM_USER{}.
exprec_user(Map) ->
    U0 = ?IAM_USER{status = S,
                   attached_policies = AP0,
                   password_last_used = PLU0,
                   permissions_boundary = PB0,
                   tags = TT0,
                   buckets = BB} = exprec:frommap_rcs_user_v3(Map),
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
    PB = case PB0 of
             Undefined when Undefined =:= null;
                            Undefined =:= undefined ->
                 undefined;
             _ ->
                 exprec:frommap_permissions_boundary(PB0)
         end,
    U0?IAM_USER{status = status_from_binary(S),
                attached_policies = [A || AP0 /= <<>>, A <- AP0],
                password_last_used = maybe_int(PLU0),
                permissions_boundary = PB,
                tags = TT,
                buckets = [exprec_bucket(B) || BB /= <<>>, B <- BB]}.
status_from_binary(<<"enabled">>) -> enabled;
status_from_binary(<<"disabled">>) -> disabled.
maybe_int(none) -> undefined;
maybe_int(undefined) -> undefined;
maybe_int(A) -> A.


-spec exprec_bucket(maps:map()) -> ?RCS_BUCKET{}.
exprec_bucket(Map) ->
    B0 = ?RCS_BUCKET{last_action = LA0,
                     acl = A0} = exprec:frommap_moss_bucket_v1(Map),
    B0?RCS_BUCKET{last_action = last_action_from_binary(LA0),
                  acl = maybe_exprec_acl(A0)}.
last_action_from_binary(<<"undefined">>) -> undefined;
last_action_from_binary(<<"created">>) -> created;
last_action_from_binary(<<"deleted">>) -> deleted.
maybe_exprec_acl(undefined) -> undefined;
maybe_exprec_acl(A) -> exprec:frommap_acl_v3(A).


-spec exprec_role(maps:map()) -> ?IAM_ROLE{}.
exprec_role(Map) ->
    Role0 = ?IAM_ROLE{permissions_boundary = PB0,
                      role_last_used = LU0,
                      tags = TT0} = exprec:frommap_role_v1(Map),
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
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
    Policy0 = ?IAM_POLICY{tags = TT0} = exprec:frommap_policy_v1(Map),
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
    Policy0?IAM_POLICY{tags = TT}.

-spec exprec_saml_provider(maps:map()) -> ?IAM_SAML_PROVIDER{}.
exprec_saml_provider(Map) ->
    P0 = ?IAM_SAML_PROVIDER{tags = TT0} = exprec:frommap_saml_provider_v1(Map),
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
    P0?IAM_SAML_PROVIDER{tags = TT}.

-spec unarm(A) -> A when A :: rcs_user() | role() | policy() | saml_provider().
unarm(A = ?IAM_USER{}) ->
    A;
unarm(A = ?IAM_POLICY{policy_document = D}) ->
    A?IAM_POLICY{policy_document = base64:decode(D)};
unarm(A = ?IAM_ROLE{assume_role_policy_document = D})
  when is_binary(D), size(D) > 0 ->
    A?IAM_ROLE{assume_role_policy_document = base64:decode(D)};
unarm(A = ?IAM_SAML_PROVIDER{saml_metadata_document = D}) ->
    A?IAM_SAML_PROVIDER{saml_metadata_document = base64:decode(D)}.




handle_response({ok, Returnable}) ->
    {ok, Returnable};
handle_response(ok) ->
    ok;
handle_response({error, {error_status, _, _, ErrorDoc}}) ->
    riak_cs_aws_response:velvet_response(ErrorDoc);
handle_response({error, _} = Error) ->
    Error.



-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
