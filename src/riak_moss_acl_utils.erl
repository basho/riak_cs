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
         default_acl/2,
         canned_acl/3,
         acl_from_xml/1,
         acl_to_xml/1,
         empty_acl_xml/0,
         requested_access/2
        ]).

-type xmlElement() :: #xmlElement{}.
-type xmlText() :: #xmlText{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Construct an acl. The structure is the same for buckets
%% and objects.
-spec acl(string(), string(), [acl_grant()]) -> acl().
acl(DisplayName, CanonicalId, Grants) ->
    OwnerData = {DisplayName, CanonicalId},
    ?ACL{owner=OwnerData,
         grants=Grants}.

%% @doc Construct a default acl. The structure is the same for buckets
%% and objects.
-spec default_acl(string(), string()) -> acl().
default_acl(DisplayName, CanonicalId) ->
    acl(DisplayName,
        CanonicalId,
        [{{DisplayName, CanonicalId}, ['FULL_CONTROL']}]).

%% @doc Map a x-amz-acl header value to an
%% internal acl representation.
-spec canned_acl(string(), string(), string()) -> acl().
canned_acl(undefined, {Name, CanonicalId}, _) ->
    default_acl(Name, CanonicalId);
canned_acl(HeaderVal, Owner, BucketOwner) ->
    {Name, CanonicalId} = Owner,
    acl(Name, CanonicalId, canned_acl_grants(HeaderVal,
                                             Owner,
                                             BucketOwner)).

%% @doc Convert an XML document representing an ACL into
%% an internal representation.
-spec acl_from_xml(string()) -> acl().
acl_from_xml(Xml) ->
    {ParsedData, _Rest} = xmerl_scan:string(Xml, []),
    process_acl_contents(ParsedData#xmlElement.content, ?ACL{}).

%% @doc Convert an internal representation of an ACL
%% into XML.
-spec acl_to_xml(acl()) -> binary().
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
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}])).

%% @doc Convert an internal representation of an ACL
%% into XML.
-spec empty_acl_xml() -> binary().
empty_acl_xml() ->
    XmlDoc =
        [{'AccessControlPolicy',
          [
          ]}],
    unicode:characters_to_binary(
      xmerl:export_simple(XmlDoc, xmerl_xml, [{prolog, ?XML_PROLOG}])).

%% @doc Map a request type to the type of ACL permissions needed
%% to complete the request.
-spec requested_access(atom(), string()) -> acl_perm().
requested_access(Method, QueryString) ->
    AclRequest = acl_request(QueryString),
    if
        Method == 'GET'
        andalso
        AclRequest == true->
            'READ_ACP';
        (Method == 'GET'
         orelse
         Method == 'HEAD') ->
            'READ';
        Method == 'PUT'
        andalso
        AclRequest == true->
            'WRITE_ACP';
        (Method == 'POST'
         orelse
         Method == 'DELETE')
        andalso
        AclRequest == true->
            undefined;
        Method == 'PUT'
        orelse
        Method == 'POST'
        orelse
        Method == 'DELETE' ->
            'WRITE';
        true ->
            undefined
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Check if the acl subresource is specified in the
%% list of query string parameters.
-spec acl_request([{string(), string()}]) -> boolean().
acl_request([]) ->
    false;
acl_request([{"acl", _} | _]) ->
    true;
acl_request([_ | Rest]) ->
    acl_request(Rest).

%% @doc Update the permissions for a grant in the provided
%% list of grants if an entry exists with matching grantee
%% data or add a grant to a list of grants.
-spec add_grant(acl_grant(), [acl_grant()]) -> [acl_grant()].
add_grant(NewGrant, Grants) ->
    {NewGrantee, NewPerms} = NewGrant,
    case [Grant || Grant={Grantee, _} <- Grants,
                   Grantee == NewGrantee] of
        [] ->
            [NewGrant | Grants];
        [{_, Perms} | _] ->
                [{NewGrantee, Perms ++ NewPerms} | Grants]
        end.

%% @doc Get the list of grants for a canned ACL
-spec canned_acl_grants(string(),
                        {string(), string()},
                        undefined | {string(), string()}) -> [acl_grant()].
canned_acl_grants("public-read", Owner, _) ->
    [{Owner, ['FULL_CONTROL']},
     {'AllUsers', ['READ']}];
canned_acl_grants("public-read-write", Owner, _) ->
    [{Owner, ['FULL_CONTROL']},
     {'AllUsers', ['READ', 'WRITE']}];
canned_acl_grants("authenticated-read", Owner, _) ->
    [{Owner, ['FULL_CONTROL']},
     {'AuthUsers', ['READ']}];
canned_acl_grants("bucket-owner-read", Owner, undefined) ->
    canned_acl_grants("private", Owner, undefined);
canned_acl_grants("bucket-owner-read", Owner, BucketOwner) ->
    [{Owner, ['FULL_CONTROL']},
     {BucketOwner, ['READ']}];
canned_acl_grants("bucket-owner-full-control", Owner, undefined) ->
    canned_acl_grants("private", Owner, undefined);
canned_acl_grants("bucket-owner-full-control", Owner, BucketOwner) ->
    [{Owner, ['FULL_CONTROL']},
     {BucketOwner, ['FULL_CONTROL']}];
canned_acl_grants(_, {Name, CanonicalId}, _) ->
    [{{Name, CanonicalId}, ['FULL_CONTROL']}].

%% @doc Get the canonical id of the user associated with
%% a given email address.
-spec canonical_for_email(string()) -> string().
canonical_for_email(Email) ->
    case riak_moss_utils:get_user_by_index(?EMAIL_INDEX,
                                           list_to_binary(Email)) of
        {ok, {User, _}} ->
            User?MOSS_USER.canonical_id;
        {error, Reason} ->
            lager:warning("Failed to retrieve canonical id for ~p. Reason: ~p", [Email, Reason]),
            []
    end.

%% @doc Assemble the xml for the set of grantees for an acl.
-spec grants_xml([acl_grant()]) -> term().
grants_xml(Grantees) ->
    grants_xml(Grantees, []).

%% @doc Assemble the xml for the set of grantees for an acl.
-spec grants_xml([acl_grant()], []) -> term().
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

%% @doc Get the display name of the user associated with
%% a given canonical id.
-spec name_for_canonical(string()) -> string().
name_for_canonical(CanonicalId) ->
    case riak_moss_utils:get_user_by_index(?ID_INDEX,
                                           list_to_binary(CanonicalId)) of
        {ok, {User, _}} ->
            User?MOSS_USER.display_name;
        {error, _} ->
            []
    end.

%% @doc Process the top-level elements of the
-spec process_acl_contents([xmlElement()], acl()) -> acl().
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
-spec process_owner([xmlElement()], acl()) -> acl().
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
-spec process_grants([xmlElement()], acl()) -> acl().
process_grants([], Acl) ->
    Acl;
process_grants([HeadElement | RestElements], Acl) ->
    Content = HeadElement#xmlElement.content,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Grant' ->
            Grant = process_grant(Content, {{"", ""}, []}),
            UpdAcl = Acl?ACL{grants=add_grant(Grant, Acl?ACL.grants)};
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
process_grantee([], {{[], CanonicalId}, _Perms}) ->
    %% Lookup the display name for the user with the
    %% canonical id of `CanonicalId'.
    DisplayName = name_for_canonical(CanonicalId),
    {{DisplayName, CanonicalId}, _Perms};
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
        'EmailAddress' ->
            lager:debug("Email value: ~p", [Value]),
            Id = canonical_for_email(Value),
            %% Get the canonical id for a given email address
            lager:debug("ID value: ~p", [Id]),
            {{Name, _}, Perms} = Grant,
            UpdGrant = {{Name, Id}, Perms};
        'URI' ->
            {_, Perms} = Grant,
            case Value of
                ?AUTH_USERS_GROUP ->
                    UpdGrant = {'AuthUsers', Perms};
                ?ALL_USERS_GROUP ->
                    UpdGrant = {'AllUsers', Perms};
                _ ->
                    %% Not yet supporting log delivery group
                    UpdGrant = Grant
            end;
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

%% @doc Map a ACL group atom to its corresponding URI.
-spec uri_for_group(atom()) -> string().
uri_for_group('AllUsers') ->
    ?ALL_USERS_GROUP;
uri_for_group('AuthUsers') ->
    ?AUTH_USERS_GROUP;
uri_for_group(_) ->
    [].

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

%% @TODO Use eqc to do some more interesting case explorations.

default_acl_test() ->
    ExpectedXml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>">>,
    DefaultAcl = default_acl("tester1", "TESTID1"),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, DefaultAcl),
    ?assertEqual(ExpectedXml, acl_to_xml(DefaultAcl)).

acl_from_xml_test() ->
    application:start(lager),
    Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    DefaultAcl = default_acl("tester1", "TESTID1"),
    Acl = acl_from_xml(Xml),
    ?assertEqual(DefaultAcl?ACL.grants, Acl?ACL.grants),
    ?assertEqual(DefaultAcl?ACL.owner, Acl?ACL.owner).

acl_to_xml_test() ->
    Xml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID2</ID><DisplayName>tester2</DisplayName></Grantee><Permission>WRITE</Permission></Grant><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>">>,
    Acl = acl("tester1", "TESTID1", [{{"tester1", "TESTID1"}, ['READ']},
                                     {{"tester2", "TESTID2"}, ['WRITE']}]),
    ?assertEqual(Xml, acl_to_xml(Acl)).

roundtrip_test() ->
    Xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    Xml2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>",
    ?assertEqual(Xml1, binary_to_list(acl_to_xml(acl_from_xml(Xml1)))),
    ?assertEqual(Xml2, binary_to_list(acl_to_xml(acl_from_xml(Xml2)))).

requested_access_test() ->
    ?assertEqual('READ', requested_access('GET', [])),
    ?assertEqual('READ_ACP', requested_access('GET', [{"acl", ""}])),
    ?assertEqual('WRITE', requested_access('PUT', [])),
    ?assertEqual('WRITE_ACP', requested_access('PUT', [{"acl", ""}])),
    ?assertEqual('WRITE', requested_access('POST', [])),
    ?assertEqual('WRITE', requested_access('DELETE', [])),
    ?assertEqual(undefined, requested_access('POST', [{"acl", ""}])),
    ?assertEqual(undefined, requested_access('DELETE', [{"acl", ""}])),
    ?assertEqual(undefined, requested_access('GARBAGE', [])),
    ?assertEqual(undefined, requested_access('GARBAGE', [{"acl", ""}])).

canned_acl_test() ->
    Owner  = {"tester1", "TESTID1"},
    BucketOwner = {"owner", "OWNERID"},
    DefaultAcl = canned_acl(undefined, Owner, undefined),
    PrivateAcl = canned_acl("private", Owner, undefined),
    PublicReadAcl = canned_acl("public-read", Owner, undefined),
    PublicRWAcl = canned_acl("public-read-write", Owner, undefined),
    AuthReadAcl = canned_acl("authenticated-read", Owner, undefined),
    BucketOwnerReadAcl1 = canned_acl("bucket-owner-read", Owner, undefined),
    BucketOwnerReadAcl2 = canned_acl("bucket-owner-read", Owner, BucketOwner),
    BucketOwnerFCAcl1 = canned_acl("bucket-owner-full-control", Owner, undefined),
    BucketOwnerFCAcl2 = canned_acl("bucket-owner-full-control", Owner, BucketOwner),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, DefaultAcl),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, PrivateAcl),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {'AllUsers', ['READ']}], _}, PublicReadAcl),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {'AllUsers', ['READ', 'WRITE']}], _}, PublicRWAcl),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {'AuthUsers', ['READ']}], _}, AuthReadAcl),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerReadAcl1),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {{"owner", "OWNERID"}, ['READ']}], _}, BucketOwnerReadAcl2),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerFCAcl1),
    ?assertMatch({acl_v1,{"tester1","TESTID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {{"owner", "OWNERID"}, ['FULL_CONTROL']}], _}, BucketOwnerFCAcl2).

-endif.
