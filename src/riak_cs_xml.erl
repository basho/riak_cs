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

%% @doc A collection functions for going to or from XML to an erlang
%% record type.

-module(riak_cs_xml).

-include("riak_cs.hrl").
-include("riak_cs_api.hrl").
-include("list_objects.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include_lib("xmerl/include/xmerl.hrl").

%% Public API
-export([scan/1,
         to_xml/1]).

-define(XML_SCHEMA_INSTANCE, "http://www.w3.org/2001/XMLSchema-instance").

-type attributes() :: [{atom(), string()}].
-type external_node() :: {atom(), [string()]}.
-type internal_node() :: {atom(), [internal_node() | external_node()]} |
                         {atom(), attributes(), [internal_node() | external_node()]}.

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
-type tag() :: atom().
-type content() :: [element()].
-type element() :: {tag(), attributes(), content()} |
                   {tag(), content()} |
                   tag() |
                   iodata() |
                   integer() |
                   float().         % Really Needed?
-type simple_form() :: content().

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
to_xml(?LORESP{}=ListObjsResp) ->
    SimpleForm = list_objects_response_to_simple_form(ListObjsResp),
    to_xml(SimpleForm);
to_xml(?RCS_USER{}=User) ->
    user_record_to_xml(User);
to_xml({users, Users}) ->
    user_records_to_xml(Users).

%% ===================================================================
%% Internal functions
%% ===================================================================

export_xml(XmlDoc) ->
    list_to_binary(xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}])).

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
acl_to_xml(Acl) ->
    Content = [make_internal_node('Owner', owner_content(acl_owner(Acl))),
               make_internal_node('AccessControlList', make_grants(acl_grants(Acl)))],
    XmlDoc = [make_internal_node('AccessControlPolicy', Content)],
    export_xml(XmlDoc).

-spec acl_grants(?ACL{} | #acl_v1{}) -> [acl_grant()].
acl_grants(?ACL{grants=Grants}) ->
    Grants;
acl_grants(#acl_v1{grants=Grants}) ->
    Grants.

-spec acl_owner(?ACL{} | #acl_v1{}) -> {string(), string()}.
acl_owner(?ACL{owner=Owner}) ->
    {OwnerName, OwnerId, _} = Owner,
    {OwnerName, OwnerId};
acl_owner(#acl_v1{owner=Owner}) ->
    Owner.

-spec owner_content({string(), string()}) -> [external_node()].
owner_content({OwnerName, OwnerId}) ->
    [make_external_node('ID', OwnerId),
     make_external_node('DisplayName', OwnerName)].

list_objects_response_to_simple_form(Resp) ->
    KeyContents = [{'Contents', key_content_to_simple_form(Content)} ||
                      Content <- (Resp?LORESP.contents)],
    CommonPrefixes = [{'CommonPrefixes', [{'Prefix', [CommonPrefix]}]} ||
                         CommonPrefix <- Resp?LORESP.common_prefixes],
    Contents = [{'Name',       [Resp?LORESP.name]},
                {'Prefix',     [Resp?LORESP.prefix]},
                {'Marker',     [Resp?LORESP.marker]}] ++
                %% use a list-comprehension trick to only include
                %% the `NextMarker' element if it's not `undefined'
               [{'NextMarker',  [NextMarker]} ||
                   NextMarker <- [Resp?LORESP.next_marker],
                   NextMarker =/= undefined,
                   Resp?LORESP.is_truncated] ++
               [{'MaxKeys',     [Resp?LORESP.max_keys]},
                {'Delimiter',   [Resp?LORESP.delimiter]},
                {'IsTruncated', [Resp?LORESP.is_truncated]}] ++
        KeyContents ++ CommonPrefixes,
    [{'ListBucketResult', [{'xmlns', ?S3_XMLNS}], Contents}].

key_content_to_simple_form(KeyContent) ->
    #list_objects_owner_v1{id=Id, display_name=Name} = KeyContent?LOKC.owner,
    [{'Key',          [KeyContent?LOKC.key]},
     {'LastModified', [KeyContent?LOKC.last_modified]},
     {'ETag',         [KeyContent?LOKC.etag]},
     {'Size',         [KeyContent?LOKC.size]},
     {'StorageClass', [KeyContent?LOKC.storage_class]},
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

-spec make_internal_node(atom(), term()) -> internal_node().
make_internal_node(Name, Content) ->
    {Name, Content}.

-spec make_internal_node(atom(), attributes(), term()) -> internal_node().
make_internal_node(Name, Attributes, Content) ->
    {Name, Attributes, Content}.

-spec make_external_node(atom(), term()) -> external_node().
make_external_node(Name, Content) ->
    {Name, [format_value(Content)]}.

%% @doc Assemble the xml for the set of grantees for an acl.
-spec make_grants([acl_grant()]) -> [internal_node()].
make_grants(Grantees) ->
    make_grants(Grantees, []).

%% @doc Assemble the xml for the set of grantees for an acl.
-spec make_grants([acl_grant()], [[internal_node()]]) -> [internal_node()].
make_grants([], Acc) ->
    lists:flatten(Acc);
make_grants([{{GranteeName, GranteeId}, Perms} | RestGrantees], Acc) ->
    Grantee = [make_grant(GranteeName, GranteeId, Perm) || Perm <- Perms],
    make_grants(RestGrantees, [Grantee | Acc]);
make_grants([{Group, Perms} | RestGrantees], Acc) ->
    Grantee = [make_grant(Group, Perm) || Perm <- Perms],
    make_grants(RestGrantees, [Grantee | Acc]).

%% @doc Assemble the xml for a group grantee for an acl.
-spec make_grant(atom(), acl_perm()) -> internal_node().
make_grant(Group, Permission) ->
    Attributes = [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                  {'xsi:type', "Group"}],
    GranteeContent = [make_external_node('URI', uri_for_group(Group))],
    GrantContent =
        [make_internal_node('Grantee', Attributes, GranteeContent),
         make_external_node('Permission', Permission)],
    make_internal_node('Grant', GrantContent).

%% @doc Assemble the xml for a single grantee for an acl.
-spec make_grant(string(), string(), acl_perm()) -> internal_node().
make_grant(DisplayName, CanonicalId, Permission) ->
    Attributes = [{'xmlns:xsi', ?XML_SCHEMA_INSTANCE},
                  {'xsi:type', "CanonicalUser"}],
    GranteeContent = [make_external_node('ID', CanonicalId),
                      make_external_node('DisplayName', DisplayName)],
    GrantContent =
        [make_internal_node('Grantee', Attributes, GranteeContent),
         make_external_node('Permission', Permission)],
    make_internal_node('Grant', GrantContent).

-spec format_value(atom() | integer() | binary() | list()) -> string().
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

%% @doc Map a ACL group atom to its corresponding URI.
-spec uri_for_group(atom()) -> string().
uri_for_group('AllUsers') ->
    ?ALL_USERS_GROUP;
uri_for_group('AuthUsers') ->
    ?AUTH_USERS_GROUP.

%% @doc Convert a Riak CS user record to XML
-spec user_record_to_xml(rcs_user()) -> binary().
user_record_to_xml(User) ->
    export_xml([user_node(User)]).

%% @doc Convert a set of Riak CS user records to XML
-spec user_records_to_xml([rcs_user()]) -> binary().
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
    Content = [make_external_node('Email', Email),
               make_external_node('DisplayName', DisplayName),
               make_external_node('Name', Name),
               make_external_node('KeyId', KeyID),
               make_external_node('KeySecret', KeySecret),
               make_external_node('Id', CanonicalID),
               make_external_node('Status', StatusStr)],
    make_internal_node('User', Content).

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
    Owner1 = #list_objects_owner_v1{id = <<"TESTID1">>, display_name = <<"tester1">>},
    Owner2 = #list_objects_owner_v1{id = <<"TESTID2">>, display_name = <<"tester2">>},
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
