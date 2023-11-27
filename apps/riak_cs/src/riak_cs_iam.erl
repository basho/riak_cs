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

-export([create_user/1,
         delete_user/1,
         get_user/2,
         find_user/2,
         update_user/1,
         list_attached_user_policies/3,
         list_users/2,

         create_policy/1,
         update_policy/1,
         delete_policy/2,
         get_policy/2,
         find_policy/2,
         list_policies/2,
         attach_role_policy/3,
         detach_role_policy/3,
         attach_user_policy/3,
         detach_user_policy/3,
         express_policies/2,

         create_role/1,
         update_role/1,
         delete_role/1,
         get_role/2,
         list_roles/2,
         find_role/2,

         create_saml_provider/1,
         delete_saml_provider/1,
         get_saml_provider/2,
         list_saml_providers/2,
         find_saml_provider/2,
         parse_saml_provider_idp_metadata/1,

         fix_permissions_boundary/1,
         exprec_user/1,
         exprec_bucket/1,
         exprec_role/1,
         exprec_iam_policy/1,
         exprec_saml_provider/1,
         unarm/1
        ]).

-include("moss.hrl").
-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").


%% ------------ users

-spec create_user(maps:map()) -> {ok, rcs_user()} | {error, already_exists | term()}.
create_user(Specs = #{user_name := Name}) ->
    Email = iolist_to_binary([Name, $@, riak_cs_config:iam_create_user_default_email_host()]),
    riak_cs_user:create_user(Name, Email, Specs).

-spec delete_user(rcs_user()) -> ok | {error, term()}.
delete_user(?IAM_USER{attached_policies = PP}) when PP /= [] ->
    {error, user_has_attached_policies};
delete_user(?IAM_USER{buckets = BB}) when BB /= [] ->
    {error, user_has_buckets};
delete_user(?IAM_USER{arn = Arn}) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:delete_user(base64:encode(Arn), [{auth_creds, AdminCreds}]).

-spec get_user(flat_arn(), pid()) -> {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, notfound}.
get_user(Arn, Pbc) ->
    case riak_cs_pbc:get(Pbc, ?USER_BUCKET, Arn, get_cs_user) of
        {ok, Obj} ->
            {ok, {riak_cs_user:from_riakc_obj(Obj, _KeepDeletedBuckets = false), Obj}};
        {weak_ok, Obj} ->
            {ok, {riak_cs_user:from_riakc_obj(Obj, true), Obj}};
        ER ->
            ER
    end.

-spec find_user(maps:map(), pid()) -> {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, notfound | term()}.
find_user(#{name := A}, Pbc) ->
    find_user(?USER_NAME_INDEX, A, Pbc);
find_user(#{canonical_id := A}, Pbc) ->
    find_user(?USER_ID_INDEX, A, Pbc);
find_user(#{key_id := A}, Pbc) ->
    case find_user(?USER_KEYID_INDEX, A, Pbc) of
        {ok, _} = Found ->
            Found;
        _ ->
            ?LOG_DEBUG("Trying to read 3.1 user by KeyId ~s", [A]),
            get_user(A, Pbc)
    end;
find_user(#{email := A}, Pbc) ->
    find_user(?USER_EMAIL_INDEX, A, Pbc).

find_user(Index, A, Pbc) ->
    Res = riakc_pb_socket:get_index_eq(Pbc, ?USER_BUCKET, Index, A),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Arn|_]}} ->
            get_user(Arn, Pbc);
        {error, Reason} ->
            logger:notice("Riak client connection error while finding user ~s in ~s: ~p", [A, Index, Reason]),
            {error, Reason}
    end.

-spec update_user(rcs_user()) -> {ok, rcs_user()} | {error, reportable_error_reason()}.
update_user(U = ?IAM_USER{key_id = KeyId}) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:update_user("application/json",
                       KeyId,
                       riak_cs_json:to_json(U),
                       [{auth_creds, AdminCreds}]).

-spec list_attached_user_policies(binary(), binary(), pid()) ->
          {ok, [{flat_arn(), PolicyName::binary()}]} | {error, term()}.
list_attached_user_policies(UserName, PathPrefix, Pbc) ->
    case find_user(#{name => UserName}, Pbc) of
        {ok, {?IAM_USER{attached_policies = AA}, _}} ->
            AANN = [begin
                        try
                            {ok, ?IAM_POLICY{path = Path,
                                             policy_name = N}} = get_policy(A, Pbc),
                            case 0 < binary:longest_common_prefix([Path, PathPrefix]) of
                                true ->
                                    {A, N};
                                false ->
                                    []
                            end
                        catch
                            error:badmatch ->
                                logger:error("Policy ~s not found while it is still attached to user ~s",
                                             [A, UserName]),
                                []
                        end
                    end || A <- AA],
            {ok, lists:flatten(AANN)};
        ER ->
            ER
    end.

-spec list_users(riak_client(), #list_users_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_users(RcPid, #list_users_request{path_prefix = PathPrefix,
                                      max_items = MaxItems,
                                      marker = Marker}) ->
    Arg = #{path_prefix => PathPrefix,
            max_items => MaxItems,
            marker => Marker},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_USER_BUCKET, riak_cs_riak_mapred:query(users, Arg)) of
        {ok, Batches} ->
            {ok, #{users => extract_objects(Batches, []),
                   marker => undefined,
                   is_truncated => false}};
        {error, _} = ER ->
            ER
    end.



%% ------------ policies

-spec create_policy(maps:map()) -> {ok, iam_policy()} | {error, reportable_error_reason()}.
create_policy(Specs = #{policy_document := D}) ->
    case riak_cs_aws_policy:policy_from_json(D) of  %% this is to validate PolicyDocument
        {ok, _} ->
            Encoded = riak_cs_json:to_json(exprec_iam_policy(Specs)),
            {ok, AdminCreds} = riak_cs_config:admin_creds(),
            velvet:create_policy("application/json",
                                 Encoded,
                                 [{auth_creds, AdminCreds}]);
        {error, _} = ER ->
            ER
    end.

-spec delete_policy(binary(), pid()) -> ok | {error, reportable_error_reason()}.
delete_policy(Arn, Pbc) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    case get_policy(Arn, Pbc) of
        {ok, ?IAM_POLICY{attachment_count = AC}} when AC > 0 ->
            {error, policy_in_use};
        {ok, _} ->
            velvet:delete_policy(Arn, [{auth_creds, AdminCreds}]);
        {error, notfound} ->
            {error, no_such_policy}
    end.

-spec get_policy(binary(), pid()) -> {ok, ?IAM_POLICY{}} | {error, term()}.
get_policy(Arn, Pbc) ->
    case riak_cs_pbc:get(Pbc, ?IAM_POLICY_BUCKET, Arn, get_cs_policy) of
        {OK, Obj} when OK =:= ok;
                       OK =:= weak_ok ->
            from_riakc_obj(Obj);
        ER ->
            ER
    end.

-spec find_policy(maps:map() | Name::binary(), pid()) -> {ok, policy()} | {error, notfound | term()}.
find_policy(Name, Pbc) when is_binary(Name) ->
    find_policy(#{name => Name}, Pbc);
find_policy(#{name := Name}, Pbc) ->
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_POLICY_BUCKET, ?POLICY_NAME_INDEX, Name),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_policy(Key, Pbc);
        {error, Reason} ->
            {error, Reason}
    end.

-spec list_policies(riak_client(), #list_policies_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_policies(RcPid, #list_policies_request{path_prefix = PathPrefix,
                                            only_attached = OnlyAttached,
                                            policy_usage_filter = PolicyUsageFilter,
                                            scope = Scope,
                                            max_items = MaxItems,
                                            marker = Marker}) ->
    Arg = #{path_prefix => PathPrefix,
            only_attached => OnlyAttached,
            policy_usage_filter => PolicyUsageFilter,
            scope => Scope,
            max_items => MaxItems,
            marker => Marker},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_POLICY_BUCKET, riak_cs_riak_mapred:query(policies, Arg)) of
        {ok, Batches} ->
            {ok, #{policies => extract_objects(Batches, []),
                   marker => undefined,
                   is_truncated => false}};
        {error, _} = ER ->
            ER
    end.

-spec attach_role_policy(binary(), binary(), pid()) ->
          ok | {error, reportable_error_reason()}.
attach_role_policy(PolicyArn, RoleName, Pbc) ->
    case find_role(#{name => RoleName}, Pbc) of
        {ok, Role = ?IAM_ROLE{attached_policies = PP}} ->
            case lists:member(PolicyArn, PP) of
                true ->
                    ok;
                false ->
                    case get_policy(PolicyArn, Pbc) of
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
detach_role_policy(PolicyArn, RoleName, Pbc) ->
    case find_role(#{name => RoleName}, Pbc) of
        {ok, Role = ?IAM_ROLE{attached_policies = PP}} ->
            case lists:member(PolicyArn, PP) of
                false ->
                    ok;
                true ->
                    case get_policy(PolicyArn, Pbc) of
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
          ok | {error, reportable_error_reason()}.
attach_user_policy(PolicyArn, UserName, Pbc) ->
    case find_user(#{name => UserName}, Pbc) of
        {ok, {User = ?RCS_USER{attached_policies = PP}, _}} ->
            case lists:member(PolicyArn, PP) of
                true ->
                    ok;
                false ->
                    case get_policy(PolicyArn, Pbc) of
                        {ok, Policy = ?IAM_POLICY{is_attachable = true,
                                                  attachment_count = AC}} ->
                            case update_user(User?RCS_USER{attached_policies = lists:usort([PolicyArn | PP])}) of
                                {ok, _} ->
                                    update_policy(Policy?IAM_POLICY{attachment_count = AC + 1});
                                {error, _} = ER ->
                                    ER
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
detach_user_policy(PolicyArn, UserName, Pbc) ->
    case find_user(#{name => UserName}, Pbc) of
        {ok, {User = ?RCS_USER{attached_policies = PP}, _}} ->
            case lists:member(PolicyArn, PP) of
                false ->
                    ok;
                true ->
                    case get_policy(PolicyArn, Pbc) of
                        {ok, Policy = ?IAM_POLICY{attachment_count = AC}} ->
                            case update_user(User?RCS_USER{attached_policies = lists:delete(PolicyArn, PP)}) of
                                {ok, _} ->
                                    update_policy(Policy?IAM_POLICY{attachment_count = AC - 1});
                                {error, _} = ER ->
                                    ER
                            end;
                        {error, notfound} ->
                            {error, no_such_policy}
                    end
            end;
        {error, notfound} ->
            {error, no_such_user}
    end.

-spec update_role(role()) -> ok | {error, reportable_error_reason()}.
update_role(R = ?IAM_ROLE{arn = Arn}) ->
    Encoded = riak_cs_json:to_json(R),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:update_role("application/json",
                       Arn,
                       Encoded,
                       [{auth_creds, AdminCreds}]).

-spec update_policy(iam_policy()) -> ok | {error, reportable_error_reason()}.
update_policy(A = ?IAM_POLICY{arn = Arn}) ->
    Encoded = riak_cs_json:to_json(A),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:update_policy("application/json",
                         Arn,
                         Encoded,
                         [{auth_creds, AdminCreds}]).


-spec express_policies([flat_arn()], pid()) -> [policy()].
express_policies(AA, Pbc) ->
    lists:flatten(
      [begin
           case get_policy(Arn, Pbc) of
               {ok, ?IAM_POLICY{policy_document = D}} ->
                   case riak_cs_aws_policy:policy_from_json(D) of
                       {ok, P} ->
                           P;
                       {error, _ReasonAlreadyReported} ->
                           logger:notice("Managed policy ~s has invalid policy document", [Arn]),
                           []
                   end;
               {error, _} ->
                   logger:notice("Managed policy ~s is not available", [Arn]),
                   []
           end
       end || Arn <- AA]
     ).

%% CreateRole takes a string for PermissionsBoundary parameter, which
%% needs to become part of a structure (and handled and exported thus), so:
-spec fix_permissions_boundary(maps:map()) -> maps:map().
fix_permissions_boundary(#{permissions_boundary := A} = Map) when A /= null,
                                                                  A /= undefined ->
    maps:update(permissions_boundary, #{permissions_boundary_arn => A}, Map);
fix_permissions_boundary(Map) ->
    Map.


%% ------------ roles

-spec create_role(maps:map()) -> {ok, role()} | {error, reportable_error_reason()}.
create_role(Specs) ->
    Encoded = riak_cs_json:to_json(exprec_role(Specs)),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:create_role("application/json",
                       Encoded,
                       [{auth_creds, AdminCreds}]).

-spec delete_role(binary()) -> ok | {error, reportable_error_reason()}.
delete_role(Arn) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:delete_role(Arn, [{auth_creds, AdminCreds}]).

-spec get_role(binary(), pid()) -> {ok, role()} | {error, term()}.
get_role(Arn, Pbc) ->
    case riak_cs_pbc:get(Pbc, ?IAM_ROLE_BUCKET, Arn, get_cs_role) of
        {OK, Obj} when OK =:= ok;
                       OK =:= weak_ok ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.

-spec find_role(maps:map() | Name::binary(), pid()) -> {ok, role()} | {error, notfound | term()}.
find_role(Name, Pbc) when is_binary(Name) ->
    find_role(#{name => Name}, Pbc);
find_role(#{name := A}, Pbc) ->
    find_role(?ROLE_NAME_INDEX, A, Pbc);
find_role(#{path := A}, Pbc) ->
    find_role(?ROLE_PATH_INDEX, A, Pbc);
find_role(#{id := A}, Pbc) ->
    find_role(?ROLE_ID_INDEX, A, Pbc).
find_role(Index, A, Pbc) ->
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_ROLE_BUCKET, Index, A),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_role(Key, Pbc);
        {error, Reason} ->
            logger:notice("Riak client connection error while finding role ~s in ~s: ~p", [A, Index, Reason]),
            {error, Reason}
    end.

-spec list_roles(riak_client(), #list_roles_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_roles(RcPid, #list_roles_request{path_prefix = PathPrefix,
                                      max_items = MaxItems,
                                      marker = Marker}) ->
    Arg = #{path_prefix => PathPrefix,
            max_items => MaxItems,
            marker => Marker},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_ROLE_BUCKET, riak_cs_riak_mapred:query(roles, Arg)) of
        {ok, Batches} ->
            {ok, #{roles => extract_objects(Batches, []),
                   marker => undefined,
                   is_truncated => false}};
        {error, _} = ER ->
            ER
    end.


%% ------------ SAML providers

-spec create_saml_provider(maps:map()) -> {ok, {Arn::binary(), [tag()]}} | {error, reportable_error_reason()}.
create_saml_provider(Specs) ->
    Encoded = riak_cs_json:to_json(exprec_saml_provider(Specs)),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:create_saml_provider("application/json",
                                Encoded,
                                [{auth_creds, AdminCreds}]).


-spec delete_saml_provider(binary()) -> ok | {error, reportable_error_reason()}.
delete_saml_provider(Arn) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    velvet:delete_saml_provider(Arn, [{auth_creds, AdminCreds}]).


-spec get_saml_provider(binary(), pid()) -> {ok, saml_provider()} | {error, term()}.
get_saml_provider(Arn, Pbc) ->
    case riak_cs_pbc:get(Pbc, ?IAM_SAMLPROVIDER_BUCKET, Arn, get_cs_saml_provider) of
        {OK, Obj} when OK =:= ok;
                       OK =:= weak_ok ->
            from_riakc_obj(Obj);
        Error ->
            Error
    end.

-spec list_saml_providers(riak_client(), #list_saml_providers_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_saml_providers(RcPid, #list_saml_providers_request{}) ->
    Arg = #{},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_SAMLPROVIDER_BUCKET, riak_cs_riak_mapred:query(saml_providers, Arg)) of
        {ok, Batches} ->
            {ok, #{saml_providers => extract_objects(Batches, [])}};
        {error, _} = ER ->
            ER
    end.

-spec find_saml_provider(maps:map() | Arn::binary(), pid()) ->
          {ok, saml_provider()} | {error, notfound | term()}.
find_saml_provider(Name, Pbc) when is_binary(Name) ->
    find_saml_provider(#{name => Name}, Pbc);
find_saml_provider(#{name := Name}, Pbc) ->
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_SAMLPROVIDER_BUCKET, ?SAMLPROVIDER_NAME_INDEX, Name),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_saml_provider(Key, Pbc);
        {error, Reason} ->
            {error, Reason}
    end;
find_saml_provider(#{entity_id := EntityID}, Pbc) ->
    Res = riakc_pb_socket:get_index_eq(Pbc, ?IAM_SAMLPROVIDER_BUCKET, ?SAMLPROVIDER_ENTITYID_INDEX, EntityID),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys = [Key|_]}} ->
            get_saml_provider(Key, Pbc);
        {error, Reason} ->
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
                                     consume_uri = list_to_binary(ConsumeUri),
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
maybe_int(null) -> undefined;
maybe_int(undefined) -> undefined;
maybe_int(A) -> A.


-spec exprec_bucket(maps:map()) -> ?RCS_BUCKET{}.
exprec_bucket(Map) ->
    B0 = ?RCS_BUCKET{last_action = LA0,
                     acl = A0} = exprec:frommap_moss_bucket_v2(Map),
    B0?RCS_BUCKET{last_action = last_action_from_binary(LA0),
                  acl = maybe_exprec_acl(A0)}.
last_action_from_binary(<<"undefined">>) -> undefined;
last_action_from_binary(<<"created">>) -> created;
last_action_from_binary(<<"deleted">>) -> deleted.
maybe_exprec_acl(null) -> undefined;
maybe_exprec_acl(undefined) -> undefined;
maybe_exprec_acl(A) -> exprec:frommap_acl_v3(A).


-spec exprec_role(maps:map()) -> ?IAM_ROLE{}.
exprec_role(Map) ->
    Role0 = ?IAM_ROLE{permissions_boundary = PB0,
                      role_last_used = LU0,
                      attached_policies = AP0,
                      tags = TT0} = exprec:frommap_role_v1(Map),
    PB = case PB0 of
             Undefined when Undefined =:= null;
                            Undefined =:= undefined ->
                 undefined;
             _ ->
                 exprec:frommap_permissions_boundary(PB0)
         end,
    LU = case LU0 of
             Undefined1 when Undefined1 =:= null;
                             Undefined1 =:= undefined ->
                 undefined;
             _ ->
                 exprec:frommap_role_last_used(LU0)
         end,
    AP = case AP0 of
             Undefined2 when Undefined2 =:= null;
                             Undefined2 =:= undefined;
                             Undefined2 =:= <<>> ->
                 [];
             Defined ->
                 Defined
         end,
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
    Role0?IAM_ROLE{permissions_boundary = PB,
                   role_last_used = LU,
                   attached_policies = AP,
                   tags = TT}.

-spec exprec_iam_policy(maps:map()) -> ?IAM_POLICY{}.
exprec_iam_policy(Map) ->
    Policy0 = ?IAM_POLICY{tags = TT0} = exprec:frommap_iam_policy(Map),
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
    Policy0?IAM_POLICY{tags = TT}.

-spec exprec_saml_provider(maps:map()) -> ?IAM_SAML_PROVIDER{}.
exprec_saml_provider(Map) ->
    P0 = ?IAM_SAML_PROVIDER{tags = TT0} = exprec:frommap_saml_provider_v1(Map),
    TT = [exprec:frommap_tag(T) || is_list(TT0), T <- TT0],
    P0?IAM_SAML_PROVIDER{tags = TT}.

-spec unarm(A) -> A when A :: rcs_user() | role() | iam_policy() | saml_provider().
unarm(A = ?IAM_USER{}) ->
    A;
unarm(A = ?IAM_POLICY{policy_document = D}) ->
    A?IAM_POLICY{policy_document = base64:decode(D)};
unarm(A = ?IAM_ROLE{assume_role_policy_document = D})
  when is_binary(D), size(D) > 0 ->
    A?IAM_ROLE{assume_role_policy_document = base64:decode(D)};
unarm(A = ?IAM_SAML_PROVIDER{saml_metadata_document = D}) ->
    A?IAM_SAML_PROVIDER{saml_metadata_document = base64:decode(D)}.


extract_objects([], Q) ->
    Q;
extract_objects([{_N, RR}|Rest], Q) ->
    extract_objects(Rest, Q ++ RR).
