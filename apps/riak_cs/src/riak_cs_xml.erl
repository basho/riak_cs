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
         to_xml/1]).

-define(XML_SCHEMA_INSTANCE, "http://www.w3.org/2001/XMLSchema-instance").

-type xml_attributes() :: [{atom(), string()}].
-type xml_external_node() :: {atom(), [string()]}.
-type xml_internal_node() :: {atom(), [xml_internal_node() | xml_external_node()]} |
                         {atom(), xml_attributes(), [xml_internal_node() | xml_external_node()]}.

%% these types should be defined in xmerl.hrl or xmerl.erl
%% for now they're defined here for convenience.
-type xmlElement() :: #xmlElement{}.
-type xmlText() :: #xmlText{}.
-type xmlComment() :: #xmlComment{}.
-type xmlPI() :: #xmlPI{}.
-type xmlDocument() :: #xmlDocument{}.
-type xmlNode() ::  xmlElement() | xmlText() | xmlComment() |
                    xmlPI() | xmlDocument().
-export_type([xmlNode/0, xmlElement/0, xmlText/0]).

%% Types for simple forms
-type xml_tag() :: atom().
-type xml_content() :: [xml_element()].
-type xml_element() :: {xml_tag(), xml_attributes(), xml_content()} |
                   {xml_tag(), xml_content()} |
                   xml_tag() |
                   iodata() |
                   integer() |
                   float().         % Really Needed?
-type simple_form() :: xml_content().

%% ===================================================================
%% Public API
%% ===================================================================


%% @doc parse XML and produce xmlElement (other comments and else are bad)
%% in R15B03 (and maybe later version), xmerl_scan:string/2 may return any
%% xml nodes, such as defined as xmlNode() above. It it unsafe because
%% `String' is the Body sent from client, which can be anything.
-spec scan(string()) -> {ok, xmlElement()} | {error, malformed_xml}.
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
to_xml(#acl_v1{}=Acl) ->
    acl_to_xml(Acl);
to_xml(?LBRESP{}=ListBucketsResp) ->
    list_buckets_response_to_xml(ListBucketsResp);
to_xml(Resp) when is_record(Resp, list_objects_response);
                  is_record(Resp, list_object_versions_response) ->
    SimpleForm = list_objects_response_to_simple_form(Resp),
    to_xml(SimpleForm);
to_xml(?RCS_USER{}=User) ->
    user_record_to_xml(User);
to_xml({users, Users}) ->
    user_records_to_xml(Users);
to_xml(?IAM_ROLE{}=Role) ->
    role_record_to_xml(Role);
to_xml({roles, RR}) ->
    role_records_to_xml(RR);
to_xml(#create_role_response{} = R) ->
    create_role_response_to_xml(R);
to_xml(#get_role_response{} = R) ->
    get_role_response_to_xml(R);
to_xml(#delete_role_response{} = R) ->
    delete_role_response_to_xml(R);
to_xml(#list_roles_response{} = R) ->
    list_roles_response_to_xml(R).



%% ===================================================================
%% Internal functions
%% ===================================================================

export_xml(XmlDoc) ->
    export_xml(XmlDoc, [{prolog, ?XML_PROLOG}]).
export_xml(XmlDoc, Opts) ->
    list_to_binary(xmerl:export_simple(XmlDoc, xmerl_xml, Opts)).

%% @doc Convert simple form into XML.
-spec simple_form_to_xml(simple_form()) -> iodata().
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
                        make_external_node('CreationDate', CreationDate)]).

user_to_xml_owner(?RCS_USER{canonical_id=CanonicalId, display_name=Name}) ->
    make_internal_node('Owner', [make_external_node('ID', [CanonicalId]),
                                 make_external_node('DisplayName', [Name])]).

%% @doc Assemble the xml for the set of grantees for an acl.
make_grants(Grantees) ->
    make_grants(Grantees, []).

%% @doc Assemble the xml for the set of grantees for an acl.
make_grants([], Acc) ->
    lists:flatten(Acc);
make_grants([?ACL_GRANT{grantee = #{display_name := GranteeName,
                                    canonical_id := GranteeId},
                        perms = Perms} | RestGrantees], Acc) ->
    Grantee = [make_grant(GranteeName, GranteeId, Perm) || Perm <- Perms],
    make_grants(RestGrantees, [Grantee | Acc]);
make_grants([?ACL_GRANT{grantee = Group,
                        perms = Perms} | RestGrantees], Acc) ->
    Grantee = [make_grant(Group, Perm) || Perm <- Perms],
    make_grants(RestGrantees, [Grantee | Acc]).

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

user_node(?RCS_USER{email=Email,
                    display_name=DisplayName,
                    name=Name,
                    key_id=KeyID,
                    key_secret=KeySecret,
                    canonical_id=CanonicalID,
                    status=Status}) ->
    StatusStr = case Status of
                    enabled ->
                        "enabled";
                    _ ->
                        "disabled"
                end,
    Content = [make_external_node(K, V)
               || {K, V} <- [{'Email', Email},
                             {'DisplayName', DisplayName},
                             {'Name', Name},
                             {'KeyId', KeyID},
                             {'KeySecret', KeySecret},
                             {'Id', CanonicalID},
                             {'Status', StatusStr}]],
    make_internal_node('User', Content).


role_record_to_xml(Role) ->
    export_xml([role_node(Role)]).

role_records_to_xml(Roles) ->
    NN = [role_node(R) || R <- Roles],
    export_xml([make_internal_node('Roles', NN)]).

role_node(?IAM_ROLE{arn = Arn,
                    assume_role_policy_document = AssumeRolePolicyDocument,
                    create_date = CreateDate,
                    description = Description,
                    max_session_duration = MaxSessionDuration,
                    path = Path,
                    permissions_boundary = PermissionsBoundary,
                    role_id = RoleId,
                    role_last_used = RoleLastUsed,
                    role_name = RoleName}) ->
    C = lists:flatten(
          [[{'Arn', make_arn(Arn)} || Arn /= undefined],
           [{'AssumeRolePolicyDocument', AssumeRolePolicyDocument} || AssumeRolePolicyDocument /= undefined],
           {'CreateDate', binary_to_list(CreateDate)},
           [{'Description', Description} || Description /= undefined],
           [{'MaxSessionDuration', list_to_integer(MaxSessionDuration)} || MaxSessionDuration /= undefined],
           {'Path', Path},
           [{'PermissionsBoundary', [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                                     {'xsi:type', "PermissionsBoundary"}],
             make_permission_boundary(PermissionsBoundary)} || PermissionsBoundary /= undefined],
           {'RoleId', RoleId},
           [{'RoleLastUsed', [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                              {'xsi:type', "RoleLastUsed"}],
             make_role_last_used(RoleLastUsed)} || RoleLastUsed /= undefined],
           [{'RoleName', RoleName} || RoleName /= undefined]
          ]),
    {'Role', [make_external_node(K, V) || {K, V} <- C]}.

make_arn(BareArn) when is_list(BareArn) ->
    BareArn;
make_arn(?S3_ARN{provider = Provider,
                 service = Service,
                 region = Region,
                 id = Id,
                 path = Path}) ->
    lists:flatten([Provider, Service, Region, Id, Path]).

make_role_last_used(?IAM_ROLE_LAST_USED{last_used_date = LastUsedDate,
                                        region = Region}) ->
    [{'LastUsedDate', LastUsedDate} || LastUsedDate =/= undefined ]
        ++ [{'Region', Region} || Region =/= undefined].

make_permission_boundary(BareArn) when is_list(BareArn);
                                       is_binary(BareArn) ->
    make_permission_boundary(?IAM_PERMISSION_BOUNDARY{permissions_boundary_arn = BareArn,
                                                      permissions_boundary_type = "Policy"});
make_permission_boundary(?IAM_PERMISSION_BOUNDARY{permissions_boundary_arn = PermissionsBoundaryArn,
                                                  permissions_boundary_type = PermissionsBoundaryType}) ->
    C = [{'PermissionsBoundaryArn', make_arn(PermissionsBoundaryArn)},
         {'PermissionsBoundaryType', PermissionsBoundaryType}
        ],
    C.

%% make_tags(TT) ->
%%     [make_tag(T) || T <- TT].
%% make_tag(?IAM_TAG{key = Key,
%%                   value = Value}) ->
%%     {'Tag', [{'Key', [Key]}, {'Value', [Value]}]}.


create_role_response_to_xml(#create_role_response{role = Role, request_id = RequestId}) ->
    CreateRoleResult = role_node(Role),
    ResponseMetadata = make_internal_node('RequestId', [RequestId]),
    C = [{'CreateRoleResult', [CreateRoleResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('CreateRoleResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).


get_role_response_to_xml(#get_role_response{role = Role, request_id = RequestId}) ->
    GetRoleResult = role_node(Role),
    ResponseMetadata = make_internal_node('RequestId', [RequestId]),
    C = [{'GetRoleResult', [GetRoleResult]},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('GetRoleResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   C)], []).

delete_role_response_to_xml(#delete_role_response{request_id = RequestId}) ->
    ResponseMetadata = make_internal_node('RequestId', [RequestId]),
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
    ResponseMetadata = make_internal_node('RequestId', [RequestId]),
    C = [{'ListRolesResult', ListRolesResult},
         {'ResponseMetadata', [ResponseMetadata]}],
    export_xml([make_internal_node('ListRolesResponse',
                                   [{'xmlns', ?IAM_XMLNS}],
                                   lists:flatten(C))], []).


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
    Grants1 = [{{"tester1", "TESTID1"}, ['READ']},
               {{"tester2", "TESTID2"}, ['WRITE']}],
    Grants2 = [{{"tester2", "TESTID1"}, ['READ']},
               {{"tester1", "TESTID2"}, ['WRITE']}],
    Acl1 = riak_cs_acl_utils:acl("tester1", "TESTID1", "TESTKEYID1", Grants1),
    Acl2 = riak_cs_acl_utils:acl("tester1", "TESTID1", "TESTKEYID1", Grants2),
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
    Xml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><User><Email>barf@spaceballs.com</Email><DisplayName>barf</DisplayName><Name>barfolomew</Name><KeyId>barf_key</KeyId><KeySecret>secretsauce</KeySecret><Id>1234</Id><Status>enabled</Status></User>">>,
    User = ?RCS_USER{name="barfolomew",
                     display_name="barf",
                     email="barf@spaceballs.com",
                     key_id="barf_key",
                     key_secret="secretsauce",
                     canonical_id="1234",
                     status=enabled},
    ?assertEqual(Xml, riak_cs_xml:to_xml(User)).

-endif.
