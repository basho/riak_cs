%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc ACL utility functions

-module(riak_cs_acl_utils).

-include("riak_cs.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([acl/4,
         default_acl/3,
         canned_acl/4,
         acl_from_xml/3,
         empty_acl_xml/0,
         requested_access/2,
         check_grants/4,
         check_grants/5
        ]).

-type xmlElement() :: #xmlElement{}.
-type xmlText() :: #xmlText{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Construct an acl. The structure is the same for buckets
%% and objects.
-spec acl(string(), string(), string(), [acl_grant()]) -> #acl_v2{}.
acl(DisplayName, CanonicalId, KeyId, Grants) ->
    OwnerData = {DisplayName, CanonicalId, KeyId},
    ?ACL{owner=OwnerData,
         grants=Grants}.

%% @doc Construct a default acl. The structure is the same for buckets
%% and objects.
-spec default_acl(string(), string(), string()) -> #acl_v2{}.
default_acl(DisplayName, CanonicalId, KeyId) ->
    acl(DisplayName,
        CanonicalId,
        KeyId,
        [{{DisplayName, CanonicalId}, ['FULL_CONTROL']}]).

%% @doc Map a x-amz-acl header value to an
%% internal acl representation.
-spec canned_acl(undefined | string(), {_,_,_} | string(), undefined | string(), pid()) -> #acl_v2{}.
canned_acl(undefined, {Name, CanonicalId, KeyId}, _, _) ->
    default_acl(Name, CanonicalId, KeyId);
canned_acl(HeaderVal, Owner, BucketOwnerId, RiakPid) ->
    {Name, CanonicalId, KeyId} = Owner,
    acl(Name, CanonicalId, KeyId, canned_acl_grants(HeaderVal,
                                                    {Name, CanonicalId},
                                                    BucketOwnerId,
                                                    RiakPid)).

%% @doc Convert an XML document representing an ACL into
%% an internal representation.
-spec acl_from_xml(string(), string(), pid()) -> #acl_v2{}.
acl_from_xml(Xml, KeyId, RiakPid) ->
    {ParsedData, _Rest} = xmerl_scan:string(Xml, []),
    BareAcl = ?ACL{owner={[], [], KeyId}},
    process_acl_contents(ParsedData#xmlElement.content, BareAcl, RiakPid).

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
-type request_method() :: 'GET' | 'HEAD' | 'PUT' | 'POST' |
                          'DELETE' | 'Dialyzer happiness'.
-spec requested_access(request_method(), boolean()) -> acl_perm().
requested_access(Method, AclRequest) ->
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
        Method == 'Dialyzer happiness' ->
            'FULL_CONTROL';
        true ->
            undefined
    end.

-spec check_grants(undefined | rcs_user(), string(), atom(), pid()) -> 
                          boolean() | {true, string()}.
check_grants(User, Bucket, RequestedAccess, RiakPid) ->
    check_grants(User, Bucket, RequestedAccess, RiakPid, undefined).

-spec check_grants(undefined | rcs_user(), string(), atom(), pid(), acl()|undefined) -> 
                          boolean() | {true, string()}.
check_grants(undefined, Bucket, RequestedAccess, RiakPid, BucketAcl) ->
    riak_cs_acl:anonymous_bucket_access(Bucket, RequestedAccess, RiakPid, BucketAcl);
check_grants(User, Bucket, RequestedAccess, RiakPid, BucketAcl) ->
    riak_cs_acl:bucket_access(Bucket,
                              RequestedAccess,
                              User?RCS_USER.canonical_id,
                              RiakPid,
                              BucketAcl).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Update the permissions for a grant in the provided
%% list of grants if an entry exists with matching grantee
%% data or add a grant to a list of grants.
-spec add_grant(acl_grant(), [acl_grant()]) -> [acl_grant()].
add_grant(NewGrant, Grants) ->
    {NewGrantee, NewPerms} = NewGrant,
    SplitFun = fun(G) ->
                       {Grantee, _} = G,
                       Grantee =:= NewGrantee
               end,
    {GranteeGrants, OtherGrants} = lists:splitwith(SplitFun, Grants),
    case GranteeGrants of
        [] ->
            [NewGrant | Grants];
        _ ->
            %% `GranteeGrants' will nearly always be a single
            %% item list, but use a fold just in case.
            %% The combined list of perms should be small so
            %% using usort should not be too expensive.
            FoldFun = fun({_, Perms}, Acc) ->
                              lists:usort(Perms ++ Acc)
                      end,
            UpdPerms = lists:foldl(FoldFun, NewPerms, GranteeGrants),
            [{NewGrantee, UpdPerms} | OtherGrants]
        end.

%% @doc Get the list of grants for a canned ACL
-spec canned_acl_grants(string(),
                        {string(), string()},
                        undefined | {string(), string()},
                        pid()) -> [acl_grant()].
canned_acl_grants("public-read", Owner, _, _) ->
    [{Owner, ['FULL_CONTROL']},
     {'AllUsers', ['READ']}];
canned_acl_grants("public-read-write", Owner, _, _) ->
    [{Owner, ['FULL_CONTROL']},
     {'AllUsers', ['READ', 'WRITE']}];
canned_acl_grants("authenticated-read", Owner, _, _) ->
    [{Owner, ['FULL_CONTROL']},
     {'AuthUsers', ['READ']}];
canned_acl_grants("bucket-owner-read", Owner, undefined, _RiakPid) ->
    canned_acl_grants("private", Owner, undefined, _RiakPid);
canned_acl_grants("bucket-owner-read", Owner, BucketOwnerId, RiakPid) ->
    BucketOwner = get_owner_data(BucketOwnerId, RiakPid),
    [{Owner, ['FULL_CONTROL']},
     {BucketOwner, ['READ']}];
canned_acl_grants("bucket-owner-full-control", Owner, undefined, _RiakPid) ->
    canned_acl_grants("private", Owner, undefined, _RiakPid);
canned_acl_grants("bucket-owner-full-control", Owner, BucketOwnerId, RiakPid) ->
    BucketOwner = get_owner_data(BucketOwnerId, RiakPid),
    [{Owner, ['FULL_CONTROL']},
     {BucketOwner, ['FULL_CONTROL']}];
canned_acl_grants(_, {Name, CanonicalId}, _, _) ->
    [{{Name, CanonicalId}, ['FULL_CONTROL']}].

%% @doc Get the canonical id of the user associated with
%% a given email address.
-spec canonical_for_email(string(), pid()) -> string().
canonical_for_email(Email, RiakPid) ->
    case riak_cs_utils:get_user_by_index(?EMAIL_INDEX,
                                           list_to_binary(Email),
                                           RiakPid) of
        {ok, {User, _}} ->
            User?RCS_USER.canonical_id;
        {error, Reason} ->
            _ = lager:warning("Failed to retrieve canonical id for ~p. Reason: ~p", [Email, Reason]),
            []
    end.

%% @doc Get user display name and canonical id for a specified key id.
-spec get_owner_data(string(), pid()) -> {string(), string()}.
get_owner_data(KeyId, RiakPid) ->
    case catch riak_cs_utils:get_user_by_index(?ID_INDEX,
                                                 list_to_binary(KeyId),
                                                 RiakPid) of
        {ok, {User, _}} ->
            {User?RCS_USER.display_name, User?RCS_USER.canonical_id};
        _ ->
            {[], []}
    end.



%% @doc Get the display name of the user associated with
%% a given canonical id.
-spec name_for_canonical(string(), pid()) -> string().
name_for_canonical(CanonicalId, RiakPid) ->
    case riak_cs_utils:get_user_by_index(?ID_INDEX,
                                           list_to_binary(CanonicalId),
                                           RiakPid) of
        {ok, {User, _}} ->
            User?RCS_USER.display_name;
        {error, _} ->
            []
    end.

%% @doc Process the top-level elements of the
-spec process_acl_contents([xmlElement()], acl(), pid()) -> #acl_v2{}.
process_acl_contents([], Acl, _) ->
    Acl;
process_acl_contents([HeadElement | RestElements], Acl, RiakPid) ->
    Content = HeadElement#xmlElement.content,
    _ = lager:debug("Element name: ~p", [HeadElement#xmlElement.name]),
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Owner' ->
            UpdAcl = process_owner(Content, Acl);
        'AccessControlList' ->
            UpdAcl = process_grants(Content, Acl, RiakPid);
        _ ->
            _ = lager:debug("Encountered unexpected element: ~p", [ElementName]),
            UpdAcl = Acl
    end,
    process_acl_contents(RestElements, UpdAcl, RiakPid).

%% @doc Process an XML element containing acl owner information.
-spec process_owner([xmlElement()], acl()) -> #acl_v2{}.
process_owner([], Acl) ->
    Acl;
process_owner([HeadElement | RestElements], Acl) ->
    Owner = Acl?ACL.owner,
    [Content] = HeadElement#xmlElement.content,
    Value = Content#xmlText.value,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'ID' ->
            _ = lager:debug("Owner ID value: ~p", [Value]),
            {OwnerName, _, OwnerKeyId} = Owner,
            UpdOwner = {OwnerName, Value, OwnerKeyId};
        'DisplayName' ->
            _ = lager:debug("Owner Name content: ~p", [Value]),
            {_, OwnerId, OwnerKeyId} = Owner,
            UpdOwner = {Value, OwnerId, OwnerKeyId};
        _ ->
            _ = lager:debug("Encountered unexpected element: ~p", [ElementName]),
            UpdOwner = Owner
    end,
    process_owner(RestElements, Acl?ACL{owner=UpdOwner}).

%% @doc Process an XML element containing the grants for the acl.
-spec process_grants([xmlElement()], acl(), pid()) -> #acl_v2{}.
process_grants([], Acl, _) ->
    Acl;
process_grants([HeadElement | RestElements], Acl, RiakPid) ->
    Content = HeadElement#xmlElement.content,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Grant' ->
            Grant = process_grant(Content, {{"", ""}, []}, RiakPid),
            UpdAcl = Acl?ACL{grants=add_grant(Grant, Acl?ACL.grants)};
        _ ->
            _ = lager:debug("Encountered unexpected grants element: ~p", [ElementName]),
            UpdAcl = Acl
    end,
    process_grants(RestElements, UpdAcl, RiakPid).

%% @doc Process an XML element containing the grants for the acl.
-spec process_grant([xmlElement()], acl_grant(), pid()) -> acl_grant().
process_grant([], Grant, _) ->
    Grant;
process_grant([HeadElement | RestElements], Grant, RiakPid) ->
    Content = HeadElement#xmlElement.content,
    ElementName = HeadElement#xmlElement.name,
    _ = lager:debug("ElementName: ~p", [ElementName]),
    _ = lager:debug("Content: ~p", [Content]),
    case ElementName of
        'Grantee' ->
            UpdGrant = process_grantee(Content, Grant, RiakPid);
        'Permission' ->
            UpdGrant = process_permission(Content, Grant);
        _ ->
            _ = lager:debug("Encountered unexpected grant element: ~p", [ElementName]),
            UpdGrant = Grant
    end,
    process_grant(RestElements, UpdGrant, RiakPid).

%% @doc Process an XML element containing information about
%% an ACL permission grantee.
-spec process_grantee([xmlElement()], acl_grant(), pid()) -> acl_grant().
process_grantee([], {{[], CanonicalId}, _Perms}, RiakPid) ->
    %% Lookup the display name for the user with the
    %% canonical id of `CanonicalId'.
    DisplayName = name_for_canonical(CanonicalId, RiakPid),
    {{DisplayName, CanonicalId}, _Perms};
process_grantee([], Grant, _) ->
    Grant;
process_grantee([HeadElement | RestElements], Grant, RiakPid) ->
    [Content] = HeadElement#xmlElement.content,
    Value = Content#xmlText.value,
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'ID' ->
            _ = lager:debug("ID value: ~p", [Value]),
            {{Name, _}, Perms} = Grant,
            UpdGrant = {{Name, Value}, Perms};
        'DisplayName' ->
            _ = lager:debug("Name value: ~p", [Value]),
            {{_, Id}, Perms} = Grant,
            UpdGrant = {{Value, Id}, Perms};
        'EmailAddress' ->
            _ = lager:debug("Email value: ~p", [Value]),
            Id = canonical_for_email(Value, RiakPid),
            %% Get the canonical id for a given email address
            _ = lager:debug("ID value: ~p", [Id]),
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
    process_grantee(RestElements, UpdGrant, RiakPid).

%% @doc Process an XML element containing information about
%% an ACL permission.
-spec process_permission([xmlText()], acl_grant()) -> acl_grant().
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
    ExpectedXml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>">>,
    DefaultAcl = default_acl("tester1", "TESTID1", "TESTKEYID1"),
    ?assertMatch({acl_v2,{"tester1","TESTID1", "TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, DefaultAcl),
    ?assertEqual(ExpectedXml, riak_cs_xml:to_xml(DefaultAcl)).

acl_from_xml_test() ->
    Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    DefaultAcl = default_acl("tester1", "TESTID1", "TESTKEYID1"),
    Acl = acl_from_xml(Xml, "TESTKEYID1", undefined),
    {ExpectedOwnerName, ExpectedOwnerId, _} = DefaultAcl?ACL.owner,
    {ActualOwnerName, ActualOwnerId, _} = Acl?ACL.owner,
    ?assertEqual(DefaultAcl?ACL.grants, Acl?ACL.grants),
    ?assertEqual(ExpectedOwnerName, ActualOwnerName),
    ?assertEqual(ExpectedOwnerId, ActualOwnerId).

roundtrip_test() ->
    Xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    Xml2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>",
    ?assertEqual(Xml1, binary_to_list(riak_cs_xml:to_xml(acl_from_xml(Xml1, "TESTKEYID1", undefined)))),
    ?assertEqual(Xml2, binary_to_list(riak_cs_xml:to_xml(acl_from_xml(Xml2, "TESTKEYID2", undefined)))).

requested_access_test() ->
    ?assertEqual('READ', requested_access('GET', false)),
    ?assertEqual('READ_ACP', requested_access('GET', true)),
    ?assertEqual('WRITE', requested_access('PUT', false)),
    ?assertEqual('WRITE_ACP', requested_access('PUT', true)),
    ?assertEqual('WRITE', requested_access('POST', false)),
    ?assertEqual('WRITE', requested_access('DELETE', false)),
    ?assertEqual(undefined, requested_access('POST', true)),
    ?assertEqual(undefined, requested_access('DELETE', true)),
    ?assertEqual(undefined, requested_access('GARBAGE', false)),
    ?assertEqual(undefined, requested_access('GARBAGE', true)).

-ifdef(BROKEN_TEST).
canned_acl_test() ->
    Owner  = {"tester1", "TESTID1", "TESTKEYID1"},
    BucketOwnerId = "OWNERKEYID",
    Pid = undefined,
    DefaultAcl = canned_acl(undefined, Owner, undefined, Pid),
    PrivateAcl = canned_acl("private", Owner, undefined, Pid),
    PublicReadAcl = canned_acl("public-read", Owner, undefined, Pid),
    PublicRWAcl = canned_acl("public-read-write", Owner, undefined, Pid),
    AuthReadAcl = canned_acl("authenticated-read", Owner, undefined, Pid),
    BucketOwnerReadAcl1 = canned_acl("bucket-owner-read", Owner, undefined, Pid),
    BucketOwnerReadAcl2 = canned_acl("bucket-owner-read", Owner, BucketOwnerId, Pid),
    BucketOwnerFCAcl1 = canned_acl("bucket-owner-full-control", Owner, undefined, Pid),
    BucketOwnerFCAcl2 = canned_acl("bucket-owner-full-control", Owner, BucketOwnerId, Pid),

    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, DefaultAcl),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, PrivateAcl),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {'AllUsers', ['READ']}], _}, PublicReadAcl),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {'AllUsers', ['READ', 'WRITE']}], _}, PublicRWAcl),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {'AuthUsers', ['READ']}], _}, AuthReadAcl),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerReadAcl1),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {{[], []}, ['READ']}], _}, BucketOwnerReadAcl2),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerFCAcl1),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {{[], []}, ['FULL_CONTROL']}], _}, BucketOwnerFCAcl2).
-endif. %BROKEN_TEST

-endif.
