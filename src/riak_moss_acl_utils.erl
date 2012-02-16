%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc ACL utility functions

-module(riak_moss_acl_utils).

-include("riak_moss.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([acl/3,
         acl_from_xml/1,
         acl_to_xml/1,
         default_acl/2
        ]).

-type xmlElement() :: #xmlElement{}.
-type xmlText() :: #xmlText{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Construct an acl. The structure is the same for buckets
%% and objects.
-spec acl(string(), string(), [acl_grant()]) -> acl_v1().
acl(DisplayName, CanonicalId, Grants) ->
    OwnerData = {DisplayName, CanonicalId},
    ?ACL{owner=OwnerData,
         grants=Grants}.

%% @doc Construct a default acl. The structure is the same for buckets
%% and objects.
-spec default_acl(string(), string()) -> acl_v1().
default_acl(DisplayName, CanonicalId) ->
    acl(DisplayName,
        CanonicalId,
        [{{DisplayName, CanonicalId}, ['FULL_CONTROL']}]).

%% @doc Convert an XML document representing an ACL into
%% an internal representation.
-spec acl_from_xml(string()) -> acl_v1().
acl_from_xml(Xml) ->
    {ParsedData, _Rest} = xmerl_scan:string(Xml, []),
    process_acl_contents(ParsedData#xmlElement.content, ?ACL{}).

%% @doc Convert an internal representation of an ACL
%% into XML.
-spec acl_to_xml(acl_v1()) -> string().
acl_to_xml(Acl) ->
    {OwnerName, OwnerId} = Acl?ACL.owner,
    XmlDoc =
        [{'AccessControlPolicy',
          [
           {'Owner',
            [
             {'ID', [OwnerId]},
             {'DisplayName', [OwnerName]}
            ]},
           {'AccessControlList', grants_xml(Acl?ACL.grants)}
          ]}],
    unicode:characters_to_list(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}])).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Assemble the xml for the set of grantees for an acl.
-spec grants_xml([acl_grant()]) -> term().
grants_xml(Grantees) ->
    grants_xml(Grantees, []).

%% @doc Assemble the xml for the set of grantees for an acl.
-spec grants_xml([acl_grant()], []) -> term().
grants_xml([], Acc) ->
    lists:flatten(Acc);
grants_xml([HeadGrantee | RestGrantees], Acc) ->
    {{GranteeName, GranteeId}, Perms} = HeadGrantee,
    GranteeXml = [grant_xml(GranteeName, GranteeId, Perm) || Perm <- Perms],
    grants_xml(RestGrantees, [GranteeXml | Acc]).

%% @doc Assemble the xml for a single grantee for an acl.
-spec grant_xml(string(), string(), acl_perm()) -> term().
grant_xml(DisplayName, CanonicalId, Permission) ->
    {'Grant',
     [
      {'Grantee',
       [
        {'ID', [CanonicalId]},
        {'DisplayName', [DisplayName]}
       ]},
      {'Permission', [atom_to_list(Permission)]}
     ]}.

%% @doc Process the top-level elements of the
-spec process_acl_contents([xmlElement()], acl_v1()) -> acl_v1().
process_acl_contents([], Acl) ->
    Acl;
process_acl_contents([HeadElement | RestElements], Acl) ->
    Content = HeadElement#xmlElement.content,
    lager:debug("Element name: ~p", [HeadElement#xmlElement.name]),
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Owner' ->
            UpdAcl = process_owner(Content, Acl);
        'AccessControlList' ->
            UpdAcl = process_grants(Content, Acl);
        _ ->
            lager:debug("Encountered unexpected element: ~p", [ElementName]),
            UpdAcl = Acl
    end,
    process_acl_contents(RestElements, UpdAcl).

%% @doc Process an XML element containing acl owner information.
-spec process_owner([xmlElement()], acl_v1()) -> acl_v1().
process_owner([], Acl) ->
    Acl;
process_owner([HeadElement | RestElements], Acl) ->
    Owner = Acl?ACL.owner,
    [Content] = HeadElement#xmlElement.content,
    Value = Content#xmlText.value,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'ID' ->
            lager:debug("Owner ID value: ~p", [Value]),
            {OwnerName, _} = Owner,
            UpdOwner = {OwnerName, Value};
        'DisplayName' ->
            lager:debug("Owner Name content: ~p", [Value]),
            {_, OwnerId} = Owner,
            UpdOwner = {Value, OwnerId};
        _ ->
            lager:debug("Encountered unexpected element: ~p", [ElementName]),
            UpdOwner = Owner
    end,
    process_owner(RestElements, Acl?ACL{owner=UpdOwner}).

%% @doc Process an XML element containing the grants for the acl.
-spec process_grants([xmlElement()], acl_v1()) -> acl_v1().
process_grants([], Acl) ->
    Acl;
process_grants([HeadElement | RestElements], Acl) ->
    Content = HeadElement#xmlElement.content,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Grant' ->
            Grant = process_grant(Content, {{"", ""}, []}),
            UpdAcl = Acl?ACL{grants=[Grant | Acl?ACL.grants]};
        _ ->
            lager:debug("Encountered unexpected grants element: ~p", [ElementName]),
            UpdAcl = Acl
    end,
    process_grants(RestElements, UpdAcl).

%% @doc Process an XML element containing the grants for the acl.
-spec process_grant([xmlElement()], acl_grant()) -> acl_grant().
process_grant([], Grant) ->
    Grant;
process_grant([HeadElement | RestElements], Grant) ->
    Content = HeadElement#xmlElement.content,
    ElementName = HeadElement#xmlElement.name,
    lager:debug("ElementName: ~p", [ElementName]),
    lager:debug("Content: ~p", [Content]),
    case ElementName of
        'Grantee' ->
            UpdGrant = process_grantee(Content, Grant);
        'Permission' ->
            UpdGrant = process_permission(Content, Grant);
        _ ->
            lager:debug("Encountered unexpected grant element: ~p", [ElementName]),
            UpdGrant = Grant
    end,
    process_grant(RestElements, UpdGrant).

%% @doc Process an XML element containing information about
%% an ACL permission grantee.
-spec process_grantee([xmlElement()], acl_grant()) -> acl_grant().
process_grantee([], Grant) ->
    Grant;
process_grantee([HeadElement | RestElements], Grant) ->
    [Content] = HeadElement#xmlElement.content,
    Value = Content#xmlText.value,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'ID' ->
            lager:debug("ID value: ~p", [Value]),
            {{Name, _}, Perms} = Grant,
            UpdGrant = {{Name, Value}, Perms};
        'DisplayName' ->
            lager:debug("Name value: ~p", [Value]),
            {{_, Id}, Perms} = Grant,
            UpdGrant = {{Value, Id}, Perms};
        _ ->
            UpdGrant = Grant
    end,
    process_grantee(RestElements, UpdGrant).

%% @doc Process an XML element containing information about
%% an ACL permission.
-spec process_permission(xmlText(), acl_grant()) -> acl_grant().
process_permission([Content], Grant) ->
    Value = list_to_existing_atom(Content#xmlText.value),
    {Grantee, Perms} = Grant,
    case lists:member(Value, Perms) of
        true ->
            UpdPerms = Perms;
        false ->
            UpdPerms = [Value | Perms]
    end,
    {Grantee, UpdPerms}.

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

%% @TODO Use eqc to do some more interesting case explorations.

default_acl_test() ->
    ExpectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    DefaultAcl = default_acl("tester1", "TESTID1"),
    ?assertEqual({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}]}, DefaultAcl),
    ?assertEqual(ExpectedXml, acl_to_xml(DefaultAcl)).

acl_from_xml_test() ->
    application:start(lager),
    Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    DefaultAcl = default_acl("tester1", "TESTID1"),
    ?assertEqual(DefaultAcl, acl_from_xml(Xml)).

acl_to_xml_test() ->
    Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee><ID>TESTID2</ID><DisplayName>tester2</DisplayName></Grantee><Permission>WRITE</Permission></Grant><Grant><Grantee><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>",
    Acl = acl("tester1", "TESTID1", [{{"tester1", "TESTID1"}, ['READ']},
                                     {{"tester2", "TESTID2"}, ['WRITE']}]),
    ?assertEqual(Xml, acl_to_xml(Acl)).

roundtrip_test() ->
    Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    ?assertEqual(Xml, acl_to_xml(acl_from_xml(Xml))).

-endif.
