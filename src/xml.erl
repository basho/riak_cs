%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc A collection functions for going to or from XML to an erlang
%% record type.

-module(xml).

-include("riak_cs.hrl").
-include("list_objects.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([to_xml/1]).

%% ===================================================================
%% Public API
%% ===================================================================

to_xml(undefined) ->
    [];
to_xml([]) ->
    [];
to_xml(?ACL{}=Acl) ->
    acl_to_xml(Acl);
to_xml(?LORESP{}=ListObjsResp) ->
    list_objects_response_to_xml(ListObjsResp).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Convert an internal representation of an ACL
%% into XML.
-spec acl_to_xml(acl()) -> binary().
acl_to_xml(?ACL{owner=Owner, grants=Grants}) ->
    {OwnerName, OwnerId, _} = Owner,
    XmlDoc =
        [{'AccessControlPolicy',
          [
           {'Owner',
            [
             {'ID', [OwnerId]},
             {'DisplayName', [OwnerName]}
            ]},
           {'AccessControlList', grants_xml(Grants)}
          ]}],
    export_xml(XmlDoc);
acl_to_xml(#acl_v1{owner=Owner, grants=Grants}) ->
    {OwnerName, OwnerId} = Owner,
    XmlDoc =
        [{'AccessControlPolicy',
          [
           {'Owner',
            [
             {'ID', [OwnerId]},
             {'DisplayName', [OwnerName]}
            ]},
           {'AccessControlList', grants_xml(Grants)}
          ]}],
    export_xml(XmlDoc).

list_objects_response_to_xml(Resp) ->
    Contents = [key_content_to_xml(Content) ||
                   Content <- (Resp?LORESP.contents)],
    %% CommonPrefixes = common_prefixes_to_xml(Resp?LORESP.common_prefixes),
    CommonPrefixes = [],
    XmlDoc = [{'ListBucketResult',
               [{'Name', [format_value(Resp?LORESP.name)]},
                {'Prefix', [format_value(Resp?LORESP.prefix)]},
                {'Marker', [format_value(Resp?LORESP.marker)]},
                {'MaxKeys', [format_value(Resp?LORESP.max_keys)]},
                {'Delimiter', [format_value(Resp?LORESP.delimiter)]},
                {'IsTruncated', [format_value(Resp?LORESP.is_truncated)]}] ++
                   Contents ++ CommonPrefixes}],
    export_xml(XmlDoc).

key_content_to_xml(KeyContent) ->
    %% Key is just not a unicode string but just a raw binary like
    %% <<229,159,188,231,142,137>>. We need to convert to unicode
    %% string like [22524,29577]. This does not affect ascii
    %% characters.
    KeyString = unicode:characters_to_list(KeyContent?LOKC.key, unicode),
    LastModified = riak_cs_wm_utils:to_iso_8601(KeyContent?LOKC.last_modified),
    {'Contents', [{'Key', [KeyString]},
                  {'LastModified', [LastModified]},
                  {'ETag', ["\"" ++ format_value(KeyContent?LOKC.etag) ++ "\""]},
                  {'Size', [format_value(KeyContent?LOKC.size)]},
                  {'StorageClass', [format_value(KeyContent?LOKC.storage_class)]},
                  owner_element(KeyContent?LOKC.owner)]}.

%% @doc Assemble the xml for the set of grantees for an acl.
-spec grants_xml([acl_grant()]) -> term().
grants_xml(Grantees) ->
    grants_xml(Grantees, []).

%% @doc Assemble the xml for the set of grantees for an acl.
-spec grants_xml([acl_grant()], list()) -> list().
grants_xml([], Acc) ->
    lists:flatten(Acc);
grants_xml([HeadGrantee | RestGrantees], Acc) ->
    case HeadGrantee of
        {{GranteeName, GranteeId}, Perms} ->
            GranteeXml = [grant_xml(GranteeName, GranteeId, Perm) ||
                             Perm <- Perms];
        {Group, Perms} ->
            GranteeXml = [grant_xml(Group, Perm) ||
                             Perm <- Perms]
    end,
    grants_xml(RestGrantees, [GranteeXml | Acc]).

%% @doc Assemble the xml for a group grantee for an acl.
-spec grant_xml(atom(), acl_perm()) -> term().
grant_xml(Group, Permission) ->
    {'Grant',
     [
      {'Grantee',
       [{'xmlns:xsi', "http://www.w3.org/2001/XMLSchema-instance"},
        {'xsi:type', "Group"}],
       [
        {'URI', [uri_for_group(Group)]}
       ]},
      {'Permission', [atom_to_list(Permission)]}
     ]}.

%% @doc Assemble the xml for a single grantee for an acl.
-spec grant_xml(string(), string(), acl_perm()) -> term().
grant_xml(DisplayName, CanonicalId, Permission) ->
    {'Grant',
     [
      {'Grantee',
       [{'xmlns:xsi', "http://www.w3.org/2001/XMLSchema-instance"},
        {'xsi:type', "CanonicalUser"}],
       [
        {'ID', [CanonicalId]},
        {'DisplayName', [DisplayName]}
       ]},
      {'Permission', [atom_to_list(Permission)]}
     ]}.

owner_element(#list_objects_owner_v1{id=Id, display_name=Name}) ->
    {'Owner', [{'ID', [format_value(Id)]},
               {'DisplayName', [format_value(Name)]}]}.

export_xml(XmlDoc) ->
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}]), unicode, unicode).

format_value(undefined) ->
    [];
format_value(true) ->
    "true";
format_value(false) ->
    "false";
format_value(Val) when is_binary(Val) ->
    binary_to_list(Val);
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
    ?assertEqual(Xml, xml:to_xml(Acl1)),
    ?assertNotEqual(Xml, xml:to_xml(Acl2)).

list_objects_response_to_xml_test() ->
    Xml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><Delimiter></Delimiter><IsTruncated>false</IsTruncated><Contents><Key>testkey1</Key><LastModified>2012-11-29T17:50:30.000Z</LastModified><ETag>\"fba9dede6af29711d7271245a35813428\"</ETag><Size>12345</Size><StorageClass>STANDARD</StorageClass><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner></Contents><Contents><Key>testkey2</Key><LastModified>2012-11-29T17:52:30.000Z</LastModified><ETag>\"43433281b2f27731ccf53597645a3985\"</ETag><Size>54321</Size><StorageClass>STANDARD</StorageClass><Owner><ID>TESTID2</ID><DisplayName>tester2</DisplayName></Owner></Contents></ListBucketResult>">>,
    Owner1 = #list_objects_owner_v1{id = <<"TESTID1">>, display_name = <<"tester1">>},
    Owner2 = #list_objects_owner_v1{id = <<"TESTID2">>, display_name = <<"tester2">>},
    Content1 = ?LOKC{key = <<"testkey1">>,
                     last_modified = "Thu, 29 Nov 2012 17:50:30 GMT",
                     etag = <<"fba9dede6af29711d7271245a35813428">>,
                     size = 12345,
                     owner = Owner1,
                     storage_class = <<"STANDARD">>},
    Content2 = ?LOKC{key = <<"testkey2">>,
                     last_modified = "Thu, 29 Nov 2012 17:52:30 GMT",
                     etag = <<"43433281b2f27731ccf53597645a3985">>,
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
    ?assertEqual(Xml, xml:to_xml(ListObjectsResponse)).

-endif.
