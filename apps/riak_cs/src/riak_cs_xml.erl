%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc A collection functions for going to or from XML to an erlang
%% record type.

-module(riak_cs_xml).

-include("riak_cs.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API
-export([scan/1,
         to_xml/1,
         find_elements/2
        ]).

-define(XML_SCHEMA_INSTANCE, "http://www.w3.org/2001/XMLSchema-instance").


%% ===================================================================
%% Public API
%% ===================================================================


-spec find_elements(atom(), [#xmlElement{}]) -> [#xmlElement{}].
find_elements(Name, EE) ->
    [E || E = #xmlElement{name = N} <- EE, N == Name].


%% @doc parse XML and produce xmlElement (other comments and else are bad)
%% in R15B03 (and maybe later version), xmerl_scan:string/2 may return any
%% xml nodes, such as defined as xmlNode() above. It is unsafe because
%% `String' is the Body sent from client, which can be anything.
-spec scan(string()) -> {ok, #xmlElement{}} | {error, malformed_xml}.
scan(String) ->
    case catch xmerl_scan:string(String) of
        {'EXIT', _E} ->  {error, malformed_xml};
        { #xmlElement{} = ParsedData, _Rest} -> {ok, ParsedData};
        _E -> {error, malformed_xml}
    end.

-spec to_xml(term()) -> binary().
to_xml(undefined) ->
    [];
to_xml(SimpleForm) when is_list(SimpleForm) ->
    simple_form_to_xml(SimpleForm);
to_xml(?ACL{}=Acl) ->
    acl_to_xml(Acl);
to_xml(?LBRESP{}=ListBucketsResp) ->
    list_buckets_response_to_xml(ListBucketsResp);
to_xml(Resp) when is_record(Resp, list_objects_response);
                  is_record(Resp, list_object_versions_response) ->
    SimpleForm = list_objects_response_to_simple_form(Resp),
    to_xml(SimpleForm);
to_xml(?RCS_USER{} = User) ->
    user_record_to_xml(User);
to_xml({users, Users}) ->
    user_records_to_xml(Users);

to_xml(?IAM_ROLE{} = Role) ->
    role_record_to_xml(Role);
to_xml({roles, RR}) ->
    role_records_to_xml(RR);
to_xml(?IAM_SAML_PROVIDER{} = P) ->
    saml_provider_record_to_xml(P);
to_xml({saml_providers, PP}) ->
    saml_provider_records_to_xml(PP);

to_xml(#create_user_response{} = R) ->
    create_user_response_to_xml(R);
to_xml(#get_user_response{} = R) ->
    get_user_response_to_xml(R);
to_xml(#delete_user_response{} = R) ->
    delete_user_response_to_xml(R);
to_xml(#list_users_response{} = R) ->
    list_users_response_to_xml(R);

to_xml(#create_role_response{} = R) ->
    create_role_response_to_xml(R);
to_xml(#get_role_response{} = R) ->
    get_role_response_to_xml(R);
to_xml(#delete_role_response{} = R) ->
    delete_role_response_to_xml(R);
to_xml(#list_roles_response{} = R) ->
    list_roles_response_to_xml(R);

to_xml(#create_policy_response{} = R) ->
    create_policy_response_to_xml(R);
to_xml(#get_policy_response{} = R) ->
    get_policy_response_to_xml(R);
to_xml(#delete_policy_response{} = R) ->
    delete_policy_response_to_xml(R);
to_xml(#list_policies_response{} = R) ->
    list_policies_response_to_xml(R);

to_xml(#create_saml_provider_response{} = R) ->
    create_saml_provider_response_to_xml(R);
to_xml(#get_saml_provider_response{} = R) ->
    get_saml_provider_response_to_xml(R);
to_xml(#delete_saml_provider_response{} = R) ->
    delete_saml_provider_response_to_xml(R);
to_xml(#list_saml_providers_response{} = R) ->
    list_saml_providers_response_to_xml(R);

to_xml(#attach_role_policy_response{} = R) ->
    attach_role_policy_response_to_xml(R);
to_xml(#detach_role_policy_response{} = R) ->
    detach_role_policy_response_to_xml(R);
to_xml(#attach_user_policy_response{} = R) ->
    attach_user_policy_response_to_xml(R);
to_xml(#detach_user_policy_response{} = R) ->
    detach_user_policy_response_to_xml(R);
to_xml(#list_attached_user_policies_response{} = R) ->
    list_attached_user_policies_response_to_xml(R);
to_xml(#list_attached_role_policies_response{} = R) ->
    list_attached_role_policies_response_to_xml(R);

to_xml(#assume_role_with_saml_response{} = R) ->
    assume_role_with_saml_response_to_xml(R);

to_xml(#list_temp_sessions_response{} = R) ->
    list_temp_sessions_response_to_xml(R).





%% ===================================================================
%% Internal functions
%% ===================================================================

export_xml(XmlDoc) ->
    export_xml(XmlDoc, [{prolog, ?XML_PROLOG}]).
export_xml(XmlDoc, Opts) ->
    list_to_binary(xmerl:export_simple(XmlDoc, xmerl_xml, Opts)).

%% @doc Convert simple form into XML.
simple_form_to_xml(Elements) ->
    XmlDoc = format_elements(Elements),
    export_xml(XmlDoc).

format_elements(Elements) ->
    [format_element(E) || E <- Elements].

format_element({Tag, Elements}) ->
    {Tag, format_elements(Elements)};
format_element({Tag, Attrs, Elements}) ->
    {Tag, Attrs, format_elements(Elements)};
format_element(Value) ->
    format_value(Value).

%% @doc Convert an internal representation of an ACL into XML.
-spec acl_to_xml(acl()) -> binary().
acl_to_xml(?ACL{owner = Owner, grants = Grants}) ->
    Content = [make_internal_node('Owner', owner_content(Owner)),
               make_internal_node('AccessControlList', make_grants(Grants))],
    XmlDoc = [make_internal_node('AccessControlPolicy', Content)],
    export_xml(XmlDoc).

owner_content(#{display_name := Name,
                canonical_id := Id}) ->
    [make_external_node('ID', Id),
     make_external_node('DisplayName', Name)].

list_objects_response_to_simple_form(?LORESP{contents = Contents,
                                             common_prefixes = CommonPrefixes,
                                             name = Name,
                                             prefix = Prefix,
                                             marker = Marker,
                                             next_marker = NextMarker,
                                             max_keys = MaxKeys,
                                             delimiter = Delimiter,
                                             is_truncated = IsTruncated}) ->
    KeyContents = [{'Contents', key_content_to_simple_form(objects, Content)} ||
                      Content <- Contents],
    CCPP = [{'CommonPrefixes', [{'Prefix', [CommonPrefix]}]} ||
                         CommonPrefix <- CommonPrefixes],
    Body = [{'Name',       [Name]},
            {'Prefix',     [Prefix]},
            {'Marker',     [Marker]}] ++
        %% use a list-comprehension trick to only include
        %% the `NextMarker' element if it's not `undefined'
        [{'NextMarker',  [M]} ||
            M <- [NextMarker],
            M =/= undefined,
            IsTruncated] ++
        [{'MaxKeys',     [MaxKeys]},
         {'Delimiter',   [Delimiter]},
         {'IsTruncated', [IsTruncated]}] ++
        KeyContents ++ CCPP,
    [{'ListBucketResult', [{'xmlns', ?S3_XMLNS}], Body}];

list_objects_response_to_simple_form(?LOVRESP{contents = Contents,
                                              common_prefixes = CommonPrefixes,
                                              name = Name,
                                              prefix = Prefix,
                                              key_marker = KeyMarker,
                                              version_id_marker = VersionIdMarker,
                                              next_key_marker = NextKeyMarker,
                                              next_version_id_marker = NextVersionIdMarker,
                                              max_keys = MaxKeys,
                                              delimiter = Delimiter,
                                              is_truncated = IsTruncated}) ->
    KeyContents = [{'Version', key_content_to_simple_form(versions, Content)} ||
                      Content <- Contents],
    CommonPrefixes = [{'CommonPrefixes', [{'Prefix', [CommonPrefix]}]} ||
                         CommonPrefix <- CommonPrefixes],
    Body = [{'Name',             [Name]},
            {'Prefix',           [Prefix]},
            {'KeyMarker',        [KeyMarker]},
            {'VersionIdMarker',  [VersionIdMarker]}] ++
        [{'NextKeyMarker',  [M]} ||
            M <- [NextKeyMarker],
            M =/= undefined,
            IsTruncated] ++
        [{'NextVersionIdMarker',  [M]} ||
            M <- [NextVersionIdMarker],
            M =/= undefined,
            IsTruncated] ++
        [{'MaxKeys',     [MaxKeys]},
         {'Delimiter',   [Delimiter]},
         {'IsTruncated', [IsTruncated]}] ++
        KeyContents ++ CommonPrefixes,
    [{'ListBucketResult', [{'xmlns', ?S3_XMLNS}], Body}].

key_content_to_simple_form(objects, KeyContent) ->
    #list_objects_owner{id=Id, display_name=Name} = KeyContent?LOKC.owner,
    [{'Key',          [KeyContent?LOKC.key]},
     {'LastModified', [KeyContent?LOKC.last_modified]},
     {'ETag',         [KeyContent?LOKC.etag]},
     {'Size',         [KeyContent?LOKC.size]},
     {'StorageClass', [KeyContent?LOKC.storage_class]},
     {'Owner',        [{'ID', [Id]},
                       {'DisplayName', [Name]}]}];

key_content_to_simple_form(versions, KeyContent) ->
    #list_objects_owner{id=Id, display_name=Name} = KeyContent?LOVKC.owner,
    [{'Key',          [KeyContent?LOVKC.key]},
     {'LastModified', [KeyContent?LOVKC.last_modified]},
     {'ETag',         [KeyContent?LOVKC.etag]},
     {'Size',         [KeyContent?LOVKC.size]},
     {'StorageClass', [KeyContent?LOVKC.storage_class]},
     {'IsLatest',     [KeyContent?LOVKC.is_latest]},
     {'VersionId',    [KeyContent?LOVKC.version_id]},
     {'Owner',        [{'ID', [Id]},
                       {'DisplayName', [Name]}]}].

list_buckets_response_to_xml(Resp) ->
    BucketsContent =
        make_internal_node('Buckets',
                           [bucket_to_xml(B?RCS_BUCKET.name,
                                          B?RCS_BUCKET.creation_date) ||
                               B <- Resp?LBRESP.buckets]),
    UserContent = user_to_xml_owner(Resp?LBRESP.user),
    Contents = [UserContent] ++ [BucketsContent],
    export_xml([make_internal_node('ListAllMyBucketsResult',
                                   [{'xmlns', ?S3_XMLNS}],
                                   Contents)]).

bucket_to_xml(Name, CreationDate) when is_binary(Name) ->
    bucket_to_xml(binary_to_list(Name), CreationDate);
bucket_to_xml(Name, CreationDate) ->
    make_internal_node('Bucket',
                       [make_external_node('Name', Name),
                        make_external_node('CreationDate', rts:iso8601_s(CreationDate))]).

user_to_xml_owner(?RCS_USER{id = CanonicalId,
                            display_name = Name}) ->
    make_internal_node('Owner', [make_external_node('ID', [CanonicalId]),
                                 make_external_node('DisplayName', [Name])]).

%% @doc Assemble the xml for the set of grantees for an acl.
make_grants(Grants) ->
    make_grants(Grants, []).

%% @doc Assemble the xml for the set of grantees for an acl.
make_grants([], Acc) ->
    lists:flatten(Acc);
make_grants([?ACL_GRANT{grantee = #{display_name := GranteeName,
                                    canonical_id := GranteeId},
                        perms = Perms} | RestGrants], Acc) ->
    Grant = [make_grant(GranteeName, GranteeId, Perm) || Perm <- Perms],
    make_grants(RestGrants, [Grant | Acc]);
make_grants([?ACL_GRANT{grantee = Group,
                        perms = Perms} | RestGrants], Acc) ->
    Grant = [make_grant(Group, Perm) || Perm <- Perms],
    make_grants(RestGrants, [Grant | Acc]).

%% @doc Assemble the xml for a group grantee for an acl.
make_grant(Group, Permission) ->
    Attributes = [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                  {'xsi:type', "Group"}],
    GranteeContent = [make_external_node('URI', uri_for_group(Group))],
    GrantContent =
        [make_internal_node('Grantee', Attributes, GranteeContent),
         make_external_node('Permission', Permission)],
    make_internal_node('Grant', GrantContent).

%% @doc Assemble the xml for a single grantee for an acl.
make_grant(DisplayName, CanonicalId, Permission) ->
    Attributes = [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                  {'xsi:type', "CanonicalUser"}],
    GranteeContent = [make_external_node('ID', CanonicalId),
                      make_external_node('DisplayName', DisplayName)],
    GrantContent =
        [make_internal_node('Grantee', Attributes, GranteeContent),
         make_external_node('Permission', Permission)],
    make_internal_node('Grant', GrantContent).

%% @doc Map a ACL group atom to its corresponding URI.
uri_for_group('AllUsers') ->
    ?ALL_USERS_GROUP;
uri_for_group('AuthUsers') ->
    ?AUTH_USERS_GROUP.

%% @doc Convert a Riak CS user record to XML
user_record_to_xml(User) ->
    export_xml([user_node(User)]).

%% @doc Convert a set of Riak CS user records to XML
user_records_to_xml(Users) ->
    UserNodes = [user_node(User) || User <- Users],
    export_xml([make_internal_node('Users', UserNodes)]).

user_node(?RCS_USER{name = Name,
                    email = Email,
                    display_name = DisplayName,
                    key_id = KeyID,
                    key_secret = KeySecret,
                    id = CanonicalID,
                    status = Status}) ->
    StatusStr = case Status of
                    enabled ->
                        "enabled";
                    _ ->
                        "disabled"
                end,
    Content = [make_external_node(K, V)
               || {K, V} <- [{'Email', Email},
                             {'DisplayName', DisplayName},
                             {'Path', Name},
                             {'Name', Name},
                             {'KeyId', KeyID},
                             {'KeySecret', KeySecret},
                             {'Id', CanonicalID},
                             {'Status', StatusStr}]],
    make_internal_node('User', Content).


%% we stick to IAM specs with this one, in contrast to user_node which
%% serves CS-specific, legacy get_user call over plain http.
iam_user_node(?IAM_USER{arn = Arn,
                        name = UserName,
                        create_date = CreateDate,
                        path = Path,
                        id = UserId,
                        password_last_used = PasswordLastUsed,
                        permissions_boundary = PermissionsBoundary,
                        tags = Tags}) ->
    C = lists:flatten(
          [{'Arn', [make_arn(Arn)]},
           {'Path', [binary_to_list(Path)]},
           {'CreateDate', [rts:iso8601_s(CreateDate)]},
           {'UserName', [binary_to_list(UserName)]},
           {'UserId', [binary_to_list(UserId)]},
           [{'PasswordLastUsed', [rts:iso8601_s(PasswordLastUsed)]}
            || PasswordLastUsed /= undefined,
               PasswordLastUsed /= null],
           [{'PermissionsBoundary', [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                                     {'xsi:type', "PermissionsBoundary"}],
             make_permissions_boundary(PermissionsBoundary)} || PermissionsBoundary /= null,
                                                                PermissionsBoundary /= undefined],
           {'Tags', [tag_node(T) || T <- Tags]}
          ]),
    {'User', C}.

create_user_response_to_xml(#create_user_response{user = User, request_id = RequestId}) ->
    CreateUserResult = iam_user_node(User),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'CreateUserResult', [CreateUserResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('CreateUserResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).


get_user_response_to_xml(#get_user_response{user = User, request_id = RequestId}) ->
    GetUserResult = iam_user_node(User),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'GetUserResult', [GetUserResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('GetUserResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

delete_user_response_to_xml(#delete_user_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('DeleteUserResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

list_users_response_to_xml(#list_users_response{users = RR,
                                                request_id = RequestId,
                                                is_truncated = IsTruncated,
                                                marker = Marker}) ->
    ListUsersResult =
        lists:flatten(
          [{'Users', [iam_user_node(R) || R <- RR]},
           {'IsTruncated', [atom_to_list(IsTruncated)]},
           [{'Marker', Marker} || Marker /= undefined]]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListUsersResult', ListUsersResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListUsersResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).



role_record_to_xml(Role) ->
    export_xml([role_node(Role)]).

role_records_to_xml(Roles) ->
    NN = [role_node(R) || R <- Roles],
    export_xml([{'Roles', NN}]).

role_node(?IAM_ROLE{arn = Arn,
                    assume_role_policy_document = AssumeRolePolicyDocument,
                    create_date = CreateDate,
                    description = Description,
                    max_session_duration = MaxSessionDuration,
                    path = Path,
                    permissions_boundary = PermissionsBoundary,
                    role_id = RoleId,
                    role_last_used = RoleLastUsed,
                    tags = Tags,
                    role_name = RoleName}) ->
    C = lists:flatten(
          [{'Arn', [make_arn(Arn)]},
           [{'AssumeRolePolicyDocument', [binary_to_list(AssumeRolePolicyDocument)]} || AssumeRolePolicyDocument /= undefined],
           {'CreateDate', [rts:iso8601_s(CreateDate)]},
           [{'Description', [binary_to_list(Description)]} || Description /= undefined,
                                                              Description /= null],
           [{'MaxSessionDuration', [integer_to_list(MaxSessionDuration)]} || MaxSessionDuration /= undefined,
                                                                             MaxSessionDuration /= null],
           {'Path', [binary_to_list(Path)]},
           [{'PermissionsBoundary', [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                                     {'xsi:type', "PermissionsBoundary"}],
             make_permissions_boundary(PermissionsBoundary)} || PermissionsBoundary /= null,
                                                                PermissionsBoundary /= undefined],
           {'RoleId', [binary_to_list(RoleId)]},
           [{'RoleLastUsed', [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                              {'xsi:type', "RoleLastUsed"}],
             make_role_last_used(RoleLastUsed)} || RoleLastUsed /= undefined],
           {'RoleName', [binary_to_list(RoleName)]},
           {'Tags', [tag_node(T) || T <- Tags]}
          ]),
    {'Role', C}.

make_arn(BareArn) when is_binary(BareArn) ->
    binary_to_list(BareArn).
%% make_arn(?S3_ARN{provider = Provider,
%%                  service = Service,
%%                  region = Region,
%%                  id = Id,
%%                  path = Path}) ->
%%     binary_to_list(
%%       iolist_to_binary(
%%         [atom_to_list(Provider), $:, atom_to_list(Service), $:, Region, $:, Id, $/, Path])).

make_role_last_used(?IAM_ROLE_LAST_USED{last_used_date = LastUsedDate,
                                        region = Region}) ->
    [{'LastUsedDate', [rts:iso8601_s(LastUsedDate)]} || LastUsedDate =/= undefined ]
        ++ [{'Region', [binary_to_list(Region)]} || Region =/= undefined].

make_permissions_boundary(BareArn) when is_binary(BareArn) ->
    make_permissions_boundary(?IAM_PERMISSION_BOUNDARY{permissions_boundary_arn = BareArn});
make_permissions_boundary(?IAM_PERMISSION_BOUNDARY{permissions_boundary_arn = PermissionsBoundaryArn,
                                                   permissions_boundary_type = PermissionsBoundaryType}) ->
    [{'PermissionsBoundaryArn', [make_arn(PermissionsBoundaryArn)]},
     {'PermissionsBoundaryType', [binary_to_list(PermissionsBoundaryType)]}
    ].


create_role_response_to_xml(#create_role_response{role = Role, request_id = RequestId}) ->
    CreateRoleResult = role_node(Role),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'CreateRoleResult', [CreateRoleResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('CreateRoleResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).


get_role_response_to_xml(#get_role_response{role = Role, request_id = RequestId}) ->
    GetRoleResult = role_node(Role),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'GetRoleResult', [GetRoleResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('GetRoleResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

delete_role_response_to_xml(#delete_role_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('DeleteRoleResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

list_roles_response_to_xml(#list_roles_response{roles = RR,
                                                request_id = RequestId,
                                                is_truncated = IsTruncated,
                                                marker = Marker}) ->
    ListRolesResult =
        lists:flatten(
          [{'Roles', [role_node(R) || R <- RR]},
           {'IsTruncated', [atom_to_list(IsTruncated)]},
           [{'Marker', Marker} || Marker /= undefined]]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListRolesResult', ListRolesResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListRolesResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).


policy_node(?IAM_POLICY{arn = Arn,
                        path = Path,
                        attachment_count = AttachmentCount,
                        create_date = CreateDate,
                        description = Description,
                        default_version_id = DefaultVersionId,
                        is_attachable = IsAttachable,
                        permissions_boundary_usage_count = PermissionsBoundaryUsageCount,
                        policy_document = PolicyDocument,
                        policy_id = PolicyId,
                        policy_name = PolicyName,
                        tags = Tags,
                        update_date = UpdateDate}) ->
    C = lists:flatten(
          [{'Arn', [make_arn(Arn)]},
           {'Path', [binary_to_list(Path)]},
           {'CreateDate', [rts:iso8601_s(CreateDate)]},
           [{'Description', [binary_to_list(Description)]} || Description /= undefined,
                                                              Description /= null],
           {'DefaultVersionId', [binary_to_list(DefaultVersionId)]},
           {'AttachmentCount', [integer_to_list(AttachmentCount)]},
           {'IsAttachable', [atom_to_list(IsAttachable)]},
           {'PermissionsBoundaryUsageCount', [integer_to_list(PermissionsBoundaryUsageCount)]},
           {'PolicyDocument', [binary_to_list(PolicyDocument)]},
           {'PolicyId', [binary_to_list(PolicyId)]},
           {'PolicyName', [binary_to_list(PolicyName)]},
           {'Tags', [tag_node(T) || T <- Tags]},
           {'UpdateDate', [rts:iso8601_s(UpdateDate)]}
          ]),
    {'Policy', C}.

create_policy_response_to_xml(#create_policy_response{policy = Policy, request_id = RequestId}) ->
    CreatePolicyResult = policy_node(Policy),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'CreatePolicyResult', [CreatePolicyResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('CreatePolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).


get_policy_response_to_xml(#get_policy_response{policy = Policy, request_id = RequestId}) ->
    GetPolicyResult = policy_node(Policy),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'GetPolicyResult', [GetPolicyResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('GetPolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

delete_policy_response_to_xml(#delete_policy_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('DeletePolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

list_policies_response_to_xml(#list_policies_response{policies = RR,
                                                      request_id = RequestId,
                                                      is_truncated = IsTruncated,
                                                      marker = Marker}) ->
    ListPoliciesResult =
        lists:flatten(
          [{'Policies', [policy_node(R) || R <- RR]},
           {'IsTruncated', [atom_to_list(IsTruncated)]},
           [{'Marker', Marker} || Marker /= undefined]]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListPoliciesResult', ListPoliciesResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListPoliciesResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).




attach_role_policy_response_to_xml(#attach_role_policy_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('AttachRolePolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

detach_role_policy_response_to_xml(#detach_role_policy_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('DetachRolePolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

attach_user_policy_response_to_xml(#attach_user_policy_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('AttachUserPolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

detach_user_policy_response_to_xml(#detach_user_policy_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('DetachUserPolicyResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).


policy_node_for_laup_result(Arn, Name) ->
    C = lists:flatten(
          [{'PolicyArn', [make_arn(Arn)]},
           {'PolicyName', [binary_to_list(Name)]}
          ]),
    {'AttachedPolicies', C}.

list_attached_user_policies_response_to_xml(#list_attached_user_policies_response{policies = RR,
                                                                                  request_id = RequestId,
                                                                                  is_truncated = IsTruncated,
                                                                                  marker = Marker}) ->
    ListAttachedUserPoliciesResult =
        lists:flatten(
          [[policy_node_for_laup_result(A, N) || {A, N} <- RR],
           {'IsTruncated', [atom_to_list(IsTruncated)]},
           [{'Marker', Marker} || Marker /= undefined]]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListAttachedUserPoliciesResult', ListAttachedUserPoliciesResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListAttachedUserPoliciesResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).


list_attached_role_policies_response_to_xml(#list_attached_role_policies_response{policies = RR,
                                                                                  request_id = RequestId,
                                                                                  is_truncated = IsTruncated,
                                                                                  marker = Marker}) ->
    ListAttachedRolePoliciesResult =
        lists:flatten(
          [[policy_node_for_laup_result(A, N) || {A, N} <- RR],
           {'IsTruncated', [atom_to_list(IsTruncated)]},
           [{'Marker', Marker} || Marker /= undefined]]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListAttachedRolePoliciesResult', ListAttachedRolePoliciesResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListAttachedRolePoliciesResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).


saml_provider_record_to_xml(P) ->
    export_xml([saml_provider_node(P)]).

saml_provider_records_to_xml(PP) ->
    NN = [saml_provider_node(P) || P <- PP],
    export_xml([make_internal_node('SAMLProviders', NN)]).


saml_provider_node(?IAM_SAML_PROVIDER{arn = Arn,
                                      create_date = CreateDate,
                                      saml_metadata_document = SAMLMetadataDocument,
                                      tags = Tags,
                                      valid_until = ValidUntil}) ->
    C = [{'Arn', [binary_to_list(Arn)]},
         {'SAMLMetadataDocument', [binary_to_list(SAMLMetadataDocument)]},
         {'CreateDate', [rts:iso8601_s(CreateDate)]},
         {'ValidUntil', [rts:iso8601_s(ValidUntil)]},
         {'Tags', [tag_node(T) || T <- Tags]}
        ],
    {'SAMLProvider', C}.

saml_provider_node_for_create(Arn, Tags) ->
    C = [{'SAMLProviderArn', [binary_to_list(Arn)]},
         {'Tags', [], [tag_node(T) || T <- Tags]}
        ],
    {'CreateSAMLProviderResult', C}.

saml_provider_node_for_get(CreateDate, ValidUntil, SAMLMetadataDocument, Tags) ->
    C = [{'CreateDate', [rts:iso8601_s(CreateDate)]},
         {'ValidUntil', [rts:iso8601_s(ValidUntil)]},
         {'SAMLMetadataDocument', [binary_to_list(SAMLMetadataDocument)]},
         {'Tags', [], [tag_node(T) || T <- Tags]}
        ],
    {'GetSAMLProviderResult', C}.

saml_provider_node_for_list(Arn, CreateDate, ValidUntil) ->
    C = [{'Arn', [make_arn(Arn)]},
         {'CreateDate', [rts:iso8601_s(CreateDate)]},
         {'ValidUntil', [rts:iso8601_s(ValidUntil)]}
        ],
    {'SAMLProviderListEntry', C}.

create_saml_provider_response_to_xml(#create_saml_provider_response{saml_provider_arn = BareArn,
                                                                    tags = Tags,
                                                                    request_id = RequestId}) ->
    CreateSAMLProviderResult = saml_provider_node_for_create(BareArn, Tags),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [CreateSAMLProviderResult,
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('CreateSAMLProviderResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

get_saml_provider_response_to_xml(#get_saml_provider_response{create_date = CreateDate,
                                                              valid_until = ValidUntil,
                                                              saml_metadata_document = SAMLMetadataDocument,
                                                              tags = Tags,
                                                              request_id = RequestId}) ->
    GetSAMLProviderResult = saml_provider_node_for_get(CreateDate, ValidUntil, SAMLMetadataDocument, Tags),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [GetSAMLProviderResult,
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('GetSAMLProviderResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

delete_saml_provider_response_to_xml(#delete_saml_provider_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('DeleteSAMLProviderResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

list_saml_providers_response_to_xml(#list_saml_providers_response{saml_provider_list = RR,
                                                                  request_id = RequestId}) ->
    ListSAMLProvidersResult =
        [{'SAMLProviderList', [saml_provider_node_for_list(Arn, CreateDate, ValidUntil)
                               || #saml_provider_list_entry{arn = Arn,
                                                            create_date = CreateDate,
                                                            valid_until = ValidUntil} <- RR]}],
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListSAMLProvidersResult', ListSAMLProvidersResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListSAMLProvidersResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).

assume_role_with_saml_response_to_xml(#assume_role_with_saml_response{assumed_role_user = AssumedRoleUser,
                                                                      audience = Audience,
                                                                      credentials = Credentials,
                                                                      issuer = Issuer,
                                                                      name_qualifier = NameQualifier,
                                                                      packed_policy_size = PackedPolicySize,
                                                                      source_identity = SourceIdentity,
                                                                      subject = Subject,
                                                                      subject_type = SubjectType,
                                                                      request_id = RequestId}) ->
    AssumeRoleWithSAMLResult =
        lists:flatten(
          [{'AssumedRoleUser', make_assumed_role_user(AssumedRoleUser)},
           {'Audience', [binary_to_list(Audience)]},
           {'Credentials', make_credentials(Credentials)},
           {'Issuer', [binary_to_list(Issuer)]},
           {'NameQualifier', [binary_to_list(NameQualifier)]},
           {'PackedPolicySize', [integer_to_list(PackedPolicySize)]},
           [{'SourceIdentity', [binary_to_list(SourceIdentity)]} || SourceIdentity /= <<>>],
           {'Subject', [binary_to_list(Subject)]},
           {'SubjectType', [binary_to_list(SubjectType)]}
          ]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'AssumeRoleWithSAMLResult', AssumeRoleWithSAMLResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('AssumeRoleWithSAMLResponse',
                                   [{'xmlns', ?STS_XMLNS}],
                                   C)], []).
make_assumed_role_user(#assumed_role_user{arn = Arn,
                                          assumed_role_id = AssumedRoleId}) ->
    [{'Arn', [binary_to_list(Arn)]},
     {'AssumedRoleId', [binary_to_list(AssumedRoleId)]}].

make_credentials(#credentials{access_key_id = AccessKeyId,
                              secret_access_key = SecretAccessKey,
                              session_token = SessionToken,
                              expiration = Expiration}) ->
    [{'AccessKeyId', [binary_to_list(AccessKeyId)]},
     {'SecretAccessKey', [binary_to_list(SecretAccessKey)]},
     {'SessionToken', [binary_to_list(SessionToken)]},
     {'Expiration', [rts:iso8601_s(Expiration)]}
    ].


temp_session_node(#temp_session{ assumed_role_user = AssumedRoleUser
                               , role = Role
                               , credentials = Credentials
                               , duration_seconds = DurationSeconds
                               , created = Created
                               , inline_policy = InlinePolicy
                               , session_policies = SessionPolicies
                               , subject = Subject
                               , source_identity = SourceIdentity
                               , email = Email
                               , user_id = UserId
                               , canonical_id = CanonicalID
                               }) ->
    C = lists:flatten(
          [{'AssumedRoleUser', make_assumed_role_user(AssumedRoleUser)},
           role_node(Role),
           {'Credentials', make_credentials(Credentials)},
           {'DurationSeconds', [integer_to_list(DurationSeconds)]},
           {'Created', [rts:iso8601_s(Created)]},
           [{'InlinePolicy', [binary_to_list(InlinePolicy) || InlinePolicy /= undefined]}],
           {'SessionPolicies', [session_policy_node(A) || A <- SessionPolicies]},
           {'Subject', [binary_to_list(Subject)]},
           {'SourceIdentity', [binary_to_list(SourceIdentity)]},
           {'Email', [binary_to_list(Email)]},
           {'UserId', [binary_to_list(UserId)]},
           {'CanonicalID', [binary_to_list(CanonicalID)]}
          ]),
    {'TempSession', C}.

session_policy_node(A) ->
    {'SessionPolicy', [binary_to_list(A)]}.


list_temp_sessions_response_to_xml(#list_temp_sessions_response{temp_sessions = RR,
                                                                is_truncated = IsTruncated,
                                                                marker = Marker,
                                                                request_id = RequestId}) ->
    ListTempSessionsResult =
        lists:flatten(
          [{'TempSessions', [temp_session_node(R) || R <- RR]},
           {'IsTruncated', [atom_to_list(IsTruncated)]},
           [{'Marker', Marker} || Marker /= undefined]]),
    ResponseMetadata = make_internal_node('RequestId', [binary_to_list(RequestId)]),
    C = [{'ListTempSessionsResult', ListTempSessionsResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListTempSessionsResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).


tag_node(?IAM_TAG{key = Key,
                  value = Value}) ->
    {'Tag', [{'Key', [binary_to_list(Key)]},
             {'Value', [binary_to_list(Value)]}]}.


make_internal_node(Name, Content) ->
    {Name, Content}.

make_internal_node(Name, Attributes, Content) ->
    {Name, Attributes, Content}.

make_external_node(Name, Content) ->
    {Name, [format_value(Content)]}.


%% @doc Convert value depending on its type into strings
format_value(undefined) ->
    [];
format_value(Val) when is_atom(Val) ->
    atom_to_list(Val);
format_value(Val) when is_binary(Val) ->
    binary_to_list(Val);
format_value(Val) when is_integer(Val) ->
    integer_to_list(Val);
format_value(Val) when is_list(Val) ->
    Val;
format_value(Val) when is_float(Val) ->
    io_lib:format("~p", [Val]).


%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

acl_to_xml_test() ->
    Xml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID2</ID><DisplayName>tester2</DisplayName></Grantee><Permission>WRITE</Permission></Grant><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>">>,
    Grants1 = [?ACL_GRANT{grantee = #{display_name => <<"tester1">>,
                                      canonical_id => <<"TESTID1">>},
                          perms = ['READ']},
               ?ACL_GRANT{grantee = #{display_name => <<"tester2">>,
                                      canonical_id => <<"TESTID2">>},
                          perms = ['WRITE']}],
    Grants2 = [?ACL_GRANT{grantee = #{display_name => <<"tester2">>,
                                      canonical_id => <<"TESTID1">>},
                          perms = ['READ']},
               ?ACL_GRANT{grantee = #{display_name => <<"tester1">>,
                                      canonical_id => <<"TESTID2">>},
                          perms = ['WRITE']}],
    Now = os:system_time(millisecond),
    Acl1 = ?ACL{owner = #{display_name => <<"tester1">>,
                          canonical_id => <<"TESTID1">>,
                          key_id => <<"TESTKEYID1">>},
                grants = Grants1,
                creation_time = Now},
    Acl2 = ?ACL{owner = #{display_name => <<"tester1">>,
                          canonical_id => <<"TESTID1">>,
                          key_id => <<"TESTKEYID1">>},
                grants = Grants2,
                creation_time = Now},
    ?assertEqual(Xml, riak_cs_xml:to_xml(Acl1)),
    ?assertNotEqual(Xml, riak_cs_xml:to_xml(Acl2)).

list_objects_response_to_xml_test() ->
    Xml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><Delimiter></Delimiter><IsTruncated>false</IsTruncated><Contents><Key>testkey1</Key><LastModified>2012-11-29T17:50:30.000Z</LastModified><ETag>\"fba9dede6af29711d7271245a35813428\"</ETag><Size>12345</Size><StorageClass>STANDARD</StorageClass><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner></Contents><Contents><Key>testkey2</Key><LastModified>2012-11-29T17:52:30.000Z</LastModified><ETag>\"43433281b2f27731ccf53597645a3985\"</ETag><Size>54321</Size><StorageClass>STANDARD</StorageClass><Owner><ID>TESTID2</ID><DisplayName>tester2</DisplayName></Owner></Contents></ListBucketResult>">>,
    Owner1 = #list_objects_owner{id = <<"TESTID1">>, display_name = <<"tester1">>},
    Owner2 = #list_objects_owner{id = <<"TESTID2">>, display_name = <<"tester2">>},
    Content1 = ?LOKC{key = <<"testkey1">>,
                     last_modified = riak_cs_wm_utils:to_iso_8601("Thu, 29 Nov 2012 17:50:30 GMT"),
                     etag = <<"\"fba9dede6af29711d7271245a35813428\"">>,
                     size = 12345,
                     owner = Owner1,
                     storage_class = <<"STANDARD">>},
    Content2 = ?LOKC{key = <<"testkey2">>,
                     last_modified = riak_cs_wm_utils:to_iso_8601("Thu, 29 Nov 2012 17:52:30 GMT"),
                     etag = <<"\"43433281b2f27731ccf53597645a3985\"">>,
                     size = 54321,
                     owner = Owner2,
                     storage_class = <<"STANDARD">>},
    ListObjectsResponse = ?LORESP{name = <<"bucket">>,
                                  max_keys = 1000,
                                  prefix = undefined,
                                  delimiter = undefined,
                                  marker = undefined,
                                  is_truncated = false,
                                  contents = [Content1, Content2],
                                  common_prefixes = []},
    ?assertEqual(Xml, riak_cs_xml:to_xml(ListObjectsResponse)).

user_record_to_xml_test() ->
    Xml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><User><Email>barf@spaceballs.com</Email><DisplayName>barf</DisplayName><Path>barfolomew</Path><Name>barfolomew</Name><KeyId>barf_key</KeyId><KeySecret>secretsauce</KeySecret><Id>1234</Id><Status>enabled</Status></User>">>,
    User = ?RCS_USER{name="barfolomew",
                     display_name="barf",
                     email="barf@spaceballs.com",
                     key_id="barf_key",
                     key_secret="secretsauce",
                     id="1234",
                     status=enabled},
    io:format("~p", [riak_cs_xml:to_xml(User)]),
    ?assertEqual(Xml, riak_cs_xml:to_xml(User)).

-endif.
