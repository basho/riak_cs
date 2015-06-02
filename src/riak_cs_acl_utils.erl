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

%% @doc ACL utility functions

-module(riak_cs_acl_utils).

-include("riak_cs.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% Public API
-export([acl/4,
         default_acl/3,
         canned_acl/3,
         specific_acl_grant/3,
         acl_from_xml/3,
         empty_acl_xml/0,
         requested_access/2,
         check_grants/4,
         check_grants/5,
         validate_acl/2
        ]).


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
-spec canned_acl(undefined | string(),
                 acl_owner(),
                 undefined | acl_owner()) -> #acl_v2{}.
canned_acl(undefined, {Name, CanonicalId, KeyId}, _) ->
    default_acl(Name, CanonicalId, KeyId);
canned_acl(HeaderVal, Owner, BucketOwner) ->
    {Name, CanonicalId, KeyId} = Owner,
    acl(Name, CanonicalId, KeyId, canned_acl_grants(HeaderVal,
                                                    Owner,
                                                    BucketOwner)).

%% @doc Turn a list of header-name, value pairs into an ACL. If the header
%% values don't parse correctly, return `{error, invalid_argument}'. If
%% the header references an email address that cannot be mapped to a
%% canonical id, return `{error, unresolved_grant_email}'. Otherwise
%% return an acl record.
-spec specific_acl_grant(Owner :: acl_owner(),
                         [{HeaderName :: acl_perm(),
                           HeaderValue :: string()}],
                         riak_client()) ->
    {ok, #acl_v2{}} |
    {error, 'invalid_argument'} |
    {error, 'unresolved_grant_email'}.
specific_acl_grant(Owner, Headers, RcPid) ->
    %% TODO: this function is getting a bit long and confusing
    Grants = [{HeaderName, parse_grant_header_value(GrantString)} ||
            {HeaderName, GrantString} <- Headers],
    case promote_failure([Grant || {_HeaderName, Grant} <- Grants]) of
        {error, invalid_argument}=E ->
            E;
        {ok, _GoodGrants} ->
            EmailsTranslated =  [{HeaderName, emails_to_ids(Grant, RcPid)} ||
                    {HeaderName, {ok, Grant}} <- Grants],
            case promote_failure([EmailOk || {_HeaderName, EmailOk} <- EmailsTranslated]) of
                {error, unresolved_grant_email}=E ->
                    E;
                {ok, _GoodEmails} ->
                    case valid_headers_to_grants([{HeaderName, Val} || {HeaderName, {ok, Val}} <- EmailsTranslated],
                                                 RcPid) of
                        {ok, AclGrants} ->
                            {DisplayName, CanonicalId, KeyId} = Owner,
                            {ok, acl(DisplayName, CanonicalId, KeyId, AclGrants)};
                        {error, invalid_argument}=E ->
                            E
                    end
            end
    end.

%% @doc Attempt to parse a list of ACL headers into a list
%% of `acl_grant()'s.
-spec valid_headers_to_grants(list(), riak_client()) ->
    {ok, list(acl_grant())} | {error, invalid_argument}.
valid_headers_to_grants(Pairs, RcPid) ->
    MaybeGrants = [header_to_acl_grants(HeaderName, Grants, RcPid) ||
            {HeaderName, Grants} <- Pairs],
    case promote_failure(MaybeGrants) of
        {ok, Grants} ->
            {ok, lists:foldl(fun add_grant/2, [], lists:flatten(Grants))};
        {error, invalid_argument}=E ->
            E
    end.

%% @doc Attempt to turn a `acl_perm()' and list of grants
%% into a list of `acl_grant()'s. At this point, email
%% addresses have already been resolved, and headers parsed.
-spec header_to_acl_grants(acl_perm(), list(), riak_client()) ->
    {ok, list(acl_grant())} | {error, invalid_argument}.
header_to_acl_grants(HeaderName, Grants, RcPid) ->
    MaybeGrantList = lists:map(fun (Identifier) ->
                    header_to_grant(HeaderName, Identifier, RcPid) end, Grants),
    case promote_failure(MaybeGrantList) of
        {ok, GrantList} ->
            {ok, lists:foldl(fun add_grant/2, [], GrantList)};
        {error, invalid_argument}=E ->
            E
    end.

%% Attempt to turn an `acl_perm()' and `grant_user_identifier()'
%% into an `acl_grant()'. If the `grant_user_identifier()' uses an
%% id, and the name can't be found, returns `{error, invalid_argument}'.
-spec header_to_grant(acl_perm(), {grant_user_identifier(), string()}, riak_client()) ->
    {ok, acl_grant()} | {error, invalid_argument}.
header_to_grant(Permission, {id, ID}, RcPid) ->
    case name_for_canonical(ID, RcPid) of
        {ok, DisplayName} ->
            {ok, {{DisplayName, ID}, [Permission]}};
        {error, invalid_argument}=E ->
            E
    end;
header_to_grant(Permission, {uri, URI}, _RcPid) ->
    case URI of
        ?ALL_USERS_GROUP ->
            {ok, {'AllUsers', [Permission]}};
        ?AUTH_USERS_GROUP ->
            {ok, {'AuthUsers', [Permission]}}
    end.

%% @doc Attempt to parse a header into
%% a list of grant identifiers and strings.
-type grant_user_identifier() :: 'emailAddress' | 'id' | 'uri'.
-spec parse_grant_header_value(string()) ->
    {ok, [{grant_user_identifier(), string()}]} |
    {error, invalid_argument} |
    {error, unresolved_grant_email}.
parse_grant_header_value(HeaderValue) ->
    Mappings = split_header_values_and_strip(HeaderValue),
    promote_failure(lists:map(fun parse_mapping/1, Mappings)).

%% @doc split a string like:
%% `"emailAddress=\"xyz@amazon.com\", emailAddress=\"abc@amazon.com\""'
%% into:
%% `["emailAddress=\"xyz@amazon.com\"",
%%  "emailAddress=\"abc@amazon.com\""]'
-spec split_header_values_and_strip(string()) -> [string()].
split_header_values_and_strip(Value) ->
    [string:strip(V) || V <- string:tokens(Value, ",")].

%% @doc Attempt to parse a single grant, like:
%% `"emailAddress=\"name@example.com\""'
%% If the value can't be parsed, return
%% `{error, invalid_argument}'.
-spec parse_mapping(string()) ->
    {ok, {grant_user_identifier(), Value :: string()}} |
    {error, invalid_argument}.
parse_mapping("emailAddress=" ++ QuotedEmail) ->
    wrap('emailAddress', remove_quotes(QuotedEmail));
parse_mapping("id=" ++ QuotedID) ->
    wrap('id', remove_quotes(QuotedID));
parse_mapping("uri=" ++ QuotedURI) ->
    case remove_quotes(QuotedURI) of
        {ok, NoQuote}=OK ->
            case valid_uri(NoQuote) of
                true ->
                    wrap('uri', OK);
                false ->
                    {error, 'invalid_argument'}
            end;
        {error, invalid_argument}=E ->
            E
    end;
parse_mapping(_Else) ->
    {error, invalid_argument}.

%% @doc Return true if `URI' is a valid group grant URI.
-spec valid_uri(string()) -> boolean().
valid_uri(URI) ->
    %% log delivery is not yet a supported option
    lists:member(URI, [?ALL_USERS_GROUP, ?AUTH_USERS_GROUP]).

%% @doc Combine the first and second argument, if the second
%% is wrapped in `ok'. Otherwise return the second argugment.
-spec wrap(atom(), {'ok', term()} | {'error', atom()}) ->
    {'error', atom()} | {ok, {atom(), term()}}.
wrap(_Atom, {error, invalid_argument}=E) ->
    E;
wrap(Atom, {ok, Value}) ->
    {ok, {Atom, Value}}.

%% If `String' is enclosed in quotation marks, remove them. Otherwise
%% return an error.
-spec remove_quotes(string()) -> {error, invalid_argument} | {ok, string()}.
remove_quotes(String) ->
    case starts_and_ends_with_quotes(String) of
        false ->
            {error, invalid_argument};
        true ->
            {ok, string:sub_string(String, 2, length(String) - 1)}
    end.

%% @doc Return true if `String' is enclosed in quotation
%% marks. The enclosed string must also be non-empty.
-spec starts_and_ends_with_quotes(string()) -> boolean().
starts_and_ends_with_quotes(String) ->
    length(String) > 2 andalso
    hd(String) =:= 34 andalso
    lists:last(String) =:= 34.

%% @doc Attempt to turn a list of grants that use email addresses
%% into a list of grants that only use canonical ids. Returns an error
%% if any of the emails cannot be turned into canonical ids.
-spec emails_to_ids(list(), riak_client()) -> {ok, list()} | {error, unresolved_grant_email}.
emails_to_ids(Grants, RcPid) ->
    {EmailGrants, RestGrants} = lists:partition(fun email_grant/1, Grants),
    Ids = [canonical_for_email(EmailAddress, RcPid) ||
            {emailAddress, EmailAddress} <- EmailGrants],
    case promote_failure(Ids) of
        {error, unresolved_grant_email}=E ->
            E;
        {ok, AllIds} ->
            {ok, RestGrants ++ [{id, ID} || ID <- AllIds]}
    end.

-spec email_grant({atom(), term}) -> boolean().
email_grant({Atom, _Val}) ->
    Atom =:= 'emailAddress'.

%% @doc Turn a list of ok-values or errors into either
%% an ok of list, or an error. Returns the latter is any
%% of the values in the input list are an error.
-spec promote_failure(list({ok, A} | {error, term()})) ->
    {ok, list(A)} | {'error', term()}.
promote_failure(List) ->
    %% this will reverse the list, but we don't care
    %% about order
    case lists:foldl(fun fail_either/2, {ok, []}, List) of
        {{error, _Reason}=E, _Acc} ->
            E;
        {ok, _Acc}=Ok ->
            Ok
    end.

%% @doc Return an error if either argument is an error. Otherwise,
%% cons the value from the first argument onto the accumulator
%% in the second.
-spec fail_either({ok, term()} | {error, term()},
                  {{error, term()} | 'ok', list()}) ->
    {ok | {error, term()}, list()}.
fail_either(_Elem, {{error, _Reason}=E, Acc}) ->
    {E, Acc};
fail_either(E={error, _Reason}, {_OkOrError, Acc}) ->
    %% don't cons the error onto the acc
    {E, Acc};
fail_either({ok, Val}, {_OkOrError, Acc}) ->
    {ok, [Val | Acc]}.

%% @doc Convert an XML document representing an ACL into
%% an internal representation.
-spec acl_from_xml(string(), string(), riak_client()) -> {ok, #acl_v2{}} |
    {error, 'invalid_argument'} |
    {error, 'unresolved_grant_email'} |
    {error, 'malformed_acl_error'}.
acl_from_xml(Xml, KeyId, RcPid) ->
    case riak_cs_xml:scan(Xml) of
        {error, malformed_xml} -> {error, malformed_acl_error};
        {ok, ParsedData} ->
            BareAcl = ?ACL{owner={[], [], KeyId}},
            process_acl_contents(ParsedData#xmlElement.content, BareAcl, RcPid)
    end.

%% @doc Convert an internal representation of an ACL
%% into XML.
-spec empty_acl_xml() -> binary().
empty_acl_xml() ->
    XmlDoc = [{'AccessControlPolicy',[]}],
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

-spec check_grants(undefined | rcs_user(), binary(), atom(), riak_client()) ->
    boolean() | {true, string()}.
check_grants(User, Bucket, RequestedAccess, RcPid) ->
    check_grants(User, Bucket, RequestedAccess, RcPid, undefined).

-spec check_grants(undefined | rcs_user(), binary(), atom(), riak_client(), acl()|undefined) ->
    boolean() | {true, string()}.
check_grants(undefined, Bucket, RequestedAccess, RcPid, BucketAcl) ->
    riak_cs_acl:anonymous_bucket_access(Bucket, RequestedAccess, RcPid, BucketAcl);
check_grants(User, Bucket, RequestedAccess, RcPid, BucketAcl) ->
    riak_cs_acl:bucket_access(Bucket,
                              RequestedAccess,
                              User?RCS_USER.canonical_id,
                              RcPid,
                              BucketAcl).

-spec validate_acl({ok, acl()} | {error, term()}, string()) ->
    {ok, acl()} | {error, access_denied}.
validate_acl({ok, Acl=?ACL{owner={_, Id, _}}}, Id) ->
    {ok, Acl};
validate_acl({ok, _}, _) ->
    {error, access_denied};
validate_acl({error, _}=Error, _) ->
    Error.

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
    {GranteeGrants, OtherGrants} = lists:partition(SplitFun, Grants),
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
                        acl_owner(),
                        undefined | acl_owner()) -> [acl_grant()].
canned_acl_grants("public-read", Owner, _) ->
    [{owner_grant(Owner), ['FULL_CONTROL']},
     {'AllUsers', ['READ']}];
canned_acl_grants("public-read-write", Owner, _) ->
    [{owner_grant(Owner), ['FULL_CONTROL']},
     {'AllUsers', ['READ', 'WRITE']}];
canned_acl_grants("authenticated-read", Owner, _) ->
    [{owner_grant(Owner), ['FULL_CONTROL']},
     {'AuthUsers', ['READ']}];
canned_acl_grants("bucket-owner-read", Owner, undefined) ->
    canned_acl_grants("private", Owner, undefined);
canned_acl_grants("bucket-owner-read", Owner, Owner) ->
    [{owner_grant(Owner), ['FULL_CONTROL']}];
canned_acl_grants("bucket-owner-read", Owner, BucketOwner) ->
    [{owner_grant(Owner), ['FULL_CONTROL']},
     {owner_grant(BucketOwner), ['READ']}];
canned_acl_grants("bucket-owner-full-control", Owner, undefined) ->
    canned_acl_grants("private", Owner, undefined);
canned_acl_grants("bucket-owner-full-control", Owner, Owner) ->
    [{owner_grant(Owner), ['FULL_CONTROL']}];
canned_acl_grants("bucket-owner-full-control", Owner, BucketOwner) ->
    [{owner_grant(Owner), ['FULL_CONTROL']},
     {owner_grant(BucketOwner), ['FULL_CONTROL']}];
canned_acl_grants(_, Owner, _) ->
    [{owner_grant(Owner), ['FULL_CONTROL']}].

-spec owner_grant({string(), string(), string()}) -> {string(), string()}.
owner_grant({Name, CanonicalId, _}) ->
    {Name, CanonicalId}.

%% @doc Get the canonical id of the user associated with
%% a given email address.
-spec canonical_for_email(string(), riak_client()) -> {ok, string()} |
    {error, unresolved_grant_email} .
canonical_for_email(Email, RcPid) ->
    case riak_cs_user:get_user_by_index(?EMAIL_INDEX,
                                         list_to_binary(Email),
                                         RcPid) of
        {ok, {User, _}} ->
            {ok, User?RCS_USER.canonical_id};
        {error, Reason} ->
            _ = lager:debug("Failed to retrieve canonical id for ~p. Reason: ~p", [Email, Reason]),
            {error, unresolved_grant_email}
    end.

%% @doc Get the display name of the user associated with
%% a given canonical id.
-spec name_for_canonical(string(), riak_client()) -> {ok, string()} |
    {error, 'invalid_argument'}.
name_for_canonical(CanonicalId, RcPid) ->
    case riak_cs_user:get_user_by_index(?ID_INDEX,
                                         list_to_binary(CanonicalId),
                                         RcPid) of
        {ok, {User, _}} ->
            {ok, User?RCS_USER.display_name};
        {error, _} ->
            {error, invalid_argument}
    end.

%% @doc Process the top-level elements of the
-spec process_acl_contents([riak_cs_xml:xmlElement()], acl(), riak_client()) ->
    {ok, #acl_v2{}} |
    {error, invalid_argument} |
    {error, unresolved_grant_email}.
process_acl_contents([], Acl, _) ->
    {ok, Acl};
process_acl_contents([#xmlElement{content=Content,
                                  name=ElementName}
                      | RestElements], Acl, RcPid) ->
    _ = lager:debug("Element name: ~p", [ElementName]),
    UpdAclRes =
                case ElementName of
        'Owner' ->
            process_owner(Content, Acl, RcPid);
        'AccessControlList' ->
            process_grants(Content, Acl, RcPid);
        _ ->
            _ = lager:debug("Encountered unexpected element: ~p", [ElementName]),
            Acl
    end,
    case UpdAclRes of
        {ok, UpdAcl} ->
            process_acl_contents(RestElements, UpdAcl, RcPid);
        {error, _}=Error ->
            Error
    end;
process_acl_contents([#xmlComment{} | RestElements], Acl, RcPid) ->
    process_acl_contents(RestElements, Acl, RcPid);
process_acl_contents([#xmlText{} | RestElements], Acl, RcPid) ->
    %% skip normalized space
    process_acl_contents(RestElements, Acl, RcPid).

%% @doc Process an XML element containing acl owner information.
-spec process_owner([riak_cs_xml:xmlNode()], acl(), riak_client()) -> {ok, #acl_v2{}}.
process_owner([], Acl=?ACL{owner={[], CanonicalId, KeyId}}, RcPid) ->
    case name_for_canonical(CanonicalId, RcPid) of
        {ok, DisplayName} ->
            {ok, Acl?ACL{owner={DisplayName, CanonicalId, KeyId}}};
        {error, _}=Error ->
            Error
    end;
process_owner([], Acl, _) ->
    {ok, Acl};
process_owner([#xmlElement{content=[Content],
                           name=ElementName} |
               RestElements], Acl, RcPid) ->
    Owner = Acl?ACL.owner,
    case Content of
        #xmlText{value=Value} ->
            UpdOwner =
                case ElementName of
                    'ID' ->
                        _ = lager:debug("Owner ID value: ~p", [Value]),
                        {OwnerName, _, OwnerKeyId} = Owner,
                        {OwnerName, Value, OwnerKeyId};
                    'DisplayName' ->
                        _ = lager:debug("Owner Name content: ~p", [Value]),
                        {_, OwnerId, OwnerKeyId} = Owner,
                        {Value, OwnerId, OwnerKeyId};
                    _ ->
                        _ = lager:debug("Encountered unexpected element: ~p", [ElementName]),
                        Owner
            end,
            process_owner(RestElements, Acl?ACL{owner=UpdOwner}, RcPid);
        _ ->
            process_owner(RestElements, Acl, RcPid)
    end;
process_owner([_ | RestElements], Acl, RcPid) ->
    %% this pattern matches with text, comment, etc..
    process_owner(RestElements, Acl, RcPid).

%% @doc Process an XML element containing the grants for the acl.
-spec process_grants([riak_cs_xml:xmlNode()], acl(), riak_client()) ->
    {ok, #acl_v2{}} |
    {error, invalid_argument} |
    {error, unresolved_grant_email}.
process_grants([], Acl, _) ->
    {ok, Acl};
process_grants([#xmlElement{content=Content,
                            name=ElementName} |
                RestElements], Acl, RcPid) ->
    UpdAcl =
             case ElementName of
        'Grant' ->
            Grant = process_grant(Content, {{"", ""}, []}, Acl?ACL.owner, RcPid),
            case Grant of
                {error, _} ->
                    Grant;
                _ ->
                    Acl?ACL{grants=add_grant(Grant, Acl?ACL.grants)}
            end;
        _ ->
            _ = lager:debug("Encountered unexpected grants element: ~p", [ElementName]),
            Acl
    end,
    case UpdAcl of
        {error, _} -> UpdAcl;
        _ -> process_grants(RestElements, UpdAcl, RcPid)
    end;
process_grants([ #xmlComment{} | RestElements], Acl, RcPid) ->
    process_grants(RestElements, Acl, RcPid);
process_grants([ #xmlText{} | RestElements], Acl, RcPid) ->
    process_grants(RestElements, Acl, RcPid).

%% @doc Process an XML element containing the grants for the acl.
-spec process_grant([riak_cs_xml:xmlElement()], acl_grant(), acl_owner(), riak_client()) ->
    acl_grant() | {error, atom()}.
process_grant([], Grant, _, _) ->
    Grant;
process_grant([#xmlElement{content=Content,
                           name=ElementName} |
               RestElements], Grant, AclOwner, RcPid) ->
    _ = lager:debug("ElementName: ~p", [ElementName]),
    _ = lager:debug("Content: ~p", [Content]),
    UpdGrant =
               case ElementName of
        'Grantee' ->
            process_grantee(Content, Grant, AclOwner, RcPid);
        'Permission' ->
            process_permission(Content, Grant);
        _ ->
            _ = lager:debug("Encountered unexpected grant element: ~p", [ElementName]),
            Grant
    end,
    case UpdGrant of
        {error, _}=Error ->
            Error;
        _ ->
            process_grant(RestElements, UpdGrant, AclOwner, RcPid)
    end;
process_grant([#xmlComment{}|RestElements], Grant, Owner, RcPid) ->
    process_grant(RestElements, Grant, Owner, RcPid);
process_grant([#xmlText{}|RestElements], Grant, Owner, RcPid) ->
    process_grant(RestElements, Grant, Owner, RcPid).

%% @doc Process an XML element containing information about
%% an ACL permission grantee.
-spec process_grantee([riak_cs_xml:xmlElement()], acl_grant(), acl_owner(), riak_client()) ->
    acl_grant() |
    {error, invalid_argument} |
    {error, unresolved_grant_email}.
process_grantee([], {{[], CanonicalId}, _Perms}, {DisplayName, CanonicalId, _}, _) ->
    {{DisplayName, CanonicalId}, _Perms};
process_grantee([], {{[], CanonicalId}, _Perms}, _, RcPid) ->
    %% Lookup the display name for the user with the
    %% canonical id of `CanonicalId'.
    case name_for_canonical(CanonicalId, RcPid) of
        {ok, DisplayName} ->
            {{DisplayName, CanonicalId}, _Perms};
        {error, _}=Error ->
            Error
    end;
process_grantee([], Grant, _, _) ->
    Grant;
process_grantee([#xmlElement{content=[Content],
                             name=ElementName} |
                 RestElements], Grant, AclOwner, RcPid) ->
    Value = Content#xmlText.value,
    case ElementName of
        'ID' ->
            _ = lager:debug("ID value: ~p", [Value]),
            {{Name, _}, Perms} = Grant,
            UpdGrant = {{Name, Value}, Perms};
        'EmailAddress' ->
            _ = lager:debug("Email value: ~p", [Value]),
            UpdGrant =
                       case canonical_for_email(Value, RcPid) of
                {ok, Id} ->
                    %% Get the canonical id for a given email address
                    _ = lager:debug("ID value: ~p", [Id]),
                    {{Name, _}, Perms} = Grant,
                    {{Name, Id}, Perms};
                {error, _}=Error ->
                    Error
            end;
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
    case UpdGrant of
        {error, _} ->
            UpdGrant;
        _ ->
            process_grantee(RestElements, UpdGrant, AclOwner, RcPid)
    end;
process_grantee([#xmlText{}|RestElements], Grant, Owner, RcPid) ->
    process_grantee(RestElements, Grant, Owner, RcPid);
process_grantee([#xmlComment{}|RestElements], Grant, Owner, RcPid) ->
    process_grantee(RestElements, Grant, Owner, RcPid).

%% @doc Process an XML element containing information about
%% an ACL permission.
-spec process_permission([riak_cs_xml:xmlText()], acl_grant()) -> acl_grant().
process_permission([Content], Grant) ->
    Value = list_to_existing_atom(Content#xmlText.value),
    {Grantee, Perms} = Grant,
    UpdPerms = case lists:member(Value, Perms) of
                   true -> Perms;
                   false -> [Value | Perms]
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
    {ok, Acl} = acl_from_xml(Xml, "TESTKEYID1", undefined),
    {ExpectedOwnerName, ExpectedOwnerId, _} = DefaultAcl?ACL.owner,
    {ActualOwnerName, ActualOwnerId, _} = Acl?ACL.owner,
    ?assertEqual(DefaultAcl?ACL.grants, Acl?ACL.grants),
    ?assertEqual(ExpectedOwnerName, ActualOwnerName),
    ?assertEqual(ExpectedOwnerId, ActualOwnerId).

roundtrip_test() ->
    Xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    Xml2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>",
    {ok, AclFromXml1} = acl_from_xml(Xml1, "TESTKEYID1", undefined),
    {ok, AclFromXml2} = acl_from_xml(Xml2, "TESTKEYID2", undefined),
    ?assertEqual(Xml1, binary_to_list(riak_cs_xml:to_xml(AclFromXml1))),
    ?assertEqual(Xml2, binary_to_list(riak_cs_xml:to_xml(AclFromXml2))).

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

canned_acl_test() ->
    Owner  = {"tester1", "TESTID1", "TESTKEYID1"},
    BucketOwner = {"owner1", "OWNERID1", "OWNERKEYID1"},
    DefaultAcl = canned_acl(undefined, Owner, undefined),
    PrivateAcl = canned_acl("private", Owner, undefined),
    PublicReadAcl = canned_acl("public-read", Owner, undefined),
    PublicRWAcl = canned_acl("public-read-write", Owner, undefined),
    AuthReadAcl = canned_acl("authenticated-read", Owner, undefined),
    BucketOwnerReadAcl1 = canned_acl("bucket-owner-read", Owner, undefined),
    BucketOwnerReadAcl2 = canned_acl("bucket-owner-read", Owner, BucketOwner),
    BucketOwnerReadAcl3 = canned_acl("bucket-owner-read", Owner, Owner),
    BucketOwnerFCAcl1 = canned_acl("bucket-owner-full-control", Owner, undefined),
    BucketOwnerFCAcl2 = canned_acl("bucket-owner-full-control", Owner, BucketOwner),
    BucketOwnerFCAcl3 = canned_acl("bucket-owner-full-control", Owner, Owner),

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
                   {{"owner1", "OWNERID1"}, ['READ']}], _}, BucketOwnerReadAcl2),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerReadAcl3),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerFCAcl1),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']},
                   {{"owner1", "OWNERID1"},  ['FULL_CONTROL']}], _}, BucketOwnerFCAcl2),
    ?assertMatch({acl_v2,{"tester1","TESTID1","TESTKEYID1"},
                  [{{"tester1","TESTID1"},['FULL_CONTROL']}], _}, BucketOwnerFCAcl3).


indented_xml_with_comments() ->
    Xml="<!-- comments here --> <AccessControlPolicy> <!-- comments here -->"
        "  <Owner> <!-- blah blah blah -->"
        "    <ID>eb874c6afce06925157eda682f1b3c6eb0f3b983bbee3673ae62f41cce21f6b1</ID>"
        " <!-- c -->   <DisplayName>admin</DisplayName> <!--"
        " \n --> </Owner>"
        "  <AccessControlList> <!-- c -->"
        "    <Grant>  <!-- c -->"
        "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">"
        "    <!-- c -->     <ID>eb874c6afce06925157eda682f1b3c6eb0f3b983bbee3673ae62f41cce21f6b1</ID>  <!-- c -->"
        "        <DisplayName>admin</DisplayName> <!-- c -->"
        "      </Grantee> <!-- c -->"
        "      <Permission>FULL_CONTROL</Permission> <!-- c -->"
        "    </Grant> <!-- c -->"
        "  </AccessControlList> <!-- c -->"
        "</AccessControlPolicy>    <!-- c -->",
    Xml.

comment_space_test() ->
    Xml = indented_xml_with_comments(),
    %% if cs782 alive, error:{badrecord,xmlElement} thrown here.
    {ok, ?ACL{} = Acl} = riak_cs_acl_utils:acl_from_xml(Xml, boom, foo),
    %% Compare the result with the one from XML without comments and extra spaces
    StrippedXml0 = re:replace(Xml, "<!--[^-]*-->", "", [global]),
    StrippedXml1 = re:replace(StrippedXml0, " *<", "<", [global]),
    StrippedXml = binary_to_list(iolist_to_binary(re:replace(StrippedXml1, " *$", "", [global]))),
    {ok, ?ACL{} = AclFromStripped} = riak_cs_acl_utils:acl_from_xml(StrippedXml, boom, foo),
    ?assertEqual(AclFromStripped?ACL{creation_time=Acl?ACL.creation_time},
                 Acl),
    ok.


-endif.
