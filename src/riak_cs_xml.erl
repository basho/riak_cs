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

%% Public API
-export([export_xml/1,
         to_xml/1]).

-define(XML_SCHEMA_INSTANCE, "http://www.w3.org/2001/XMLSchema-instance").

-type attributes() :: [{atom(), string()}].
-type external_node() :: {atom(), [string()]}.
-type internal_node() :: {atom(), [internal_node() | external_node()]} |
                         {atom(), attributes(), [internal_node() | external_node()]}.

%% ===================================================================
%% Public API
%% ===================================================================

%% This function is temporary and should be removed once all XML
%% handling has been moved into this module.
%% @TODO Remove this asap!
export_xml(XmlDoc) ->
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}]), unicode, unicode).

-spec to_xml(term()) -> binary().
to_xml(undefined) ->
    [];
to_xml([]) ->
    [];
to_xml(?ACL{}=Acl) ->
    acl_to_xml(Acl);
to_xml(#acl_v1{}=Acl) ->
    acl_to_xml(Acl);
to_xml(?LBRESP{}=ListBucketsResp) ->
    list_buckets_response_to_xml(ListBucketsResp);
to_xml(?LORESP{}=ListObjsResp) ->
    list_objects_response_to_xml(ListObjsResp).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Convert an internal representation of an ACL
%% into XML.
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

list_objects_response_to_xml(Resp) ->
    KeyContents = [key_content_to_xml(Content) ||
                   Content <- (Resp?LORESP.contents)],
    CommonPrefixes = [common_prefix_to_xml(Prefix) ||
                         Prefix <- Resp?LORESP.common_prefixes],
    Contents = [make_external_node('Name', Resp?LORESP.name),
                make_external_node('Prefix', Resp?LORESP.prefix),
                make_external_node('Marker', Resp?LORESP.marker),
                make_external_node('MaxKeys', Resp?LORESP.max_keys),
                make_external_node('Delimiter', Resp?LORESP.delimiter),
                make_external_node('IsTruncated', Resp?LORESP.is_truncated)] ++
        KeyContents ++ CommonPrefixes,
    export_xml([make_internal_node('ListBucketResult', [{'xmlns', ?S3_XMLNS}],
                                   Contents)]).

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
    make_internal_node('Owner', [make_external_node('ID', CanonicalId),
                                 make_external_node('DisplayName', Name)]).

key_content_to_xml(KeyContent) ->
    Contents =
        [make_external_node('Key', KeyContent?LOKC.key),
         make_external_node('LastModified', KeyContent?LOKC.last_modified),
         make_external_node('ETag', KeyContent?LOKC.etag),
         make_external_node('Size', KeyContent?LOKC.size),
         make_external_node('StorageClass', KeyContent?LOKC.storage_class),
         make_owner(KeyContent?LOKC.owner)],
    make_internal_node('Contents', Contents).

-spec common_prefix_to_xml(binary()) -> internal_node().
common_prefix_to_xml(CommonPrefix) ->
    make_internal_node('CommonPrefixes',
                       [make_external_node('Prefix', CommonPrefix)]).

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

-spec make_owner(list_objects_owner()) -> internal_node().
make_owner(#list_objects_owner_v1{id=Id, display_name=Name}) ->
    Content = [make_external_node('ID', Id),
               make_external_node('DisplayName', Name)],
    make_internal_node('Owner', Content).

-spec format_value(atom() | integer() | binary() | list()) -> string().
format_value(undefined) ->
    [];
format_value(Val) when is_atom(Val) ->
    atom_to_list(Val);
format_value(Val) when is_binary(Val) ->
    unicode:characters_to_list(Val, unicode);
format_value(Val) when is_integer(Val) ->
    integer_to_list(Val);
format_value(Val) when is_list(Val) ->
    Val.

%% @doc Map a ACL group atom to its corresponding URI.
-spec uri_for_group(atom()) -> string().
uri_for_group('AllUsers') ->
    ?ALL_USERS_GROUP;
uri_for_group('AuthUsers') ->
    ?AUTH_USERS_GROUP.

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

-endif.
