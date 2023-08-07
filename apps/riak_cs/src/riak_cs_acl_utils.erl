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

%% @doc ACL utility functions

-module(riak_cs_acl_utils).

-include("riak_cs.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API
-export([default_acl/1, default_acl/3,
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

%% @doc Construct a default acl. The structure is the same for buckets
%% and objects.
-spec default_acl(binary(), binary(), binary()) -> acl().
default_acl(DisplayName, CanonicalId, KeyId) ->
    default_acl(#{display_name => DisplayName,
                  canonical_id => CanonicalId,
                  key_id => KeyId}).
-spec default_acl(maps:map()) -> acl().
default_acl(Owner) ->
    ?ACL{owner = Owner,
         grants = [?ACL_GRANT{grantee = Owner,
                              perms = ['FULL_CONTROL']}]
        }.

%% @doc Map a x-amz-acl header value to an
%% internal acl representation.
-spec canned_acl(undefined | string(),
                 acl_owner(),
                 undefined | acl_owner()) -> acl().
canned_acl(undefined, Owner, _) ->
    default_acl(Owner);
canned_acl(HeaderVal, Owner, BucketOwner) ->
    ?ACL{owner = Owner,
         grants = canned_acl_grants(HeaderVal,
                                    Owner,
                                    BucketOwner)}.

%% @doc Turn a list of header-name, value pairs into an ACL. If the header
%% values don't parse correctly, return `{error, invalid_argument}'. If
%% the header references an email address that cannot be mapped to a
%% canonical id, return `{error, unresolved_grant_email}'. Otherwise
%% return an acl record.
-spec specific_acl_grant(Owner :: acl_owner(),
                         [{HeaderName :: acl_perm(),
                           HeaderValue :: string()}],
                         riak_client()) ->
    {ok, acl()} |
    {error, invalid_argument | unresolved_grant_email | unresolved_grant_canonical_id}.
specific_acl_grant(Owner, Headers, RcPid) ->
    %% TODO: this function is getting a bit long and confusing
    Grants = [{HeaderName, parse_grant_header_value(GrantString)}
              || {HeaderName, GrantString} <- Headers],
    case promote_failure([G || {_HeaderName, G} <- Grants]) of
        {error, invalid_argument} = E ->
            E;
        {ok, _GoodGrants} ->
            EmailsTranslated = [{HeaderName, emails_to_ids(G, RcPid)}
                                || {HeaderName, {ok, G}} <- Grants],
            case promote_failure([EmailOk || {_H, EmailOk} <- EmailsTranslated]) of
                {error, unresolved_grant_email} = E ->
                    E;
                {ok, _GoodEmails} ->
                    case valid_headers_to_grants(
                           [{HeaderName, Val} || {HeaderName, {ok, Val}} <- EmailsTranslated],
                           RcPid) of
                        {ok, AclGrants} ->
                            {ok, ?ACL{owner = Owner,
                                      grants = AclGrants}
                            };
                        {error, _} = E ->
                            E
                    end
            end
    end.

%% @doc Attempt to parse a list of ACL headers into a list
%% of `acl_grant()'s.
valid_headers_to_grants(Pairs, RcPid) ->
    MaybeGrants = [header_to_acl_grants(HeaderName, Grants, RcPid) ||
            {HeaderName, Grants} <- Pairs],
    case promote_failure(MaybeGrants) of
        {ok, Grants} ->
            {ok, lists:foldl(fun add_grant/2, [], lists:flatten(Grants))};
        {error, _} = E ->
            E
    end.

%% @doc Attempt to turn a `acl_perm()' and list of grants
%% into a list of `acl_grant()'s. At this point, email
%% addresses have already been resolved, and headers parsed.
header_to_acl_grants(HeaderName, Grants, RcPid) ->
    MaybeGrantList = lists:map(
                       fun (Id) -> header_to_grant(HeaderName, Id, RcPid) end,
                       Grants),
    case promote_failure(MaybeGrantList) of
        {ok, GrantList} ->
            {ok, lists:foldl(fun add_grant/2, [], GrantList)};
        {error, _} = E ->
            E
    end.

%% Attempt to turn an `acl_perm()' and `grant_user_identifier()'
%% into an `acl_grant()'. If the `grant_user_identifier()' uses an
%% id, and the name can't be found, returns `{error, invalid_argument}'.
header_to_grant(Permission, {id, ID}, RcPid) ->
    case name_for_canonical(ID, RcPid) of
        {ok, DisplayName} ->
            {ok, {{DisplayName, ID}, [Permission]}};
        {error, _} = E ->
            E
    end;
header_to_grant(Permission, {uri, URI}, _RcPid) ->
    case URI of
        ?ALL_USERS_GROUP ->
            {ok, ?ACL_GRANT{grantee = 'AllUsers',
                            perms = [Permission]}};
        ?AUTH_USERS_GROUP ->
            {ok, ?ACL_GRANT{grantee = 'AuthUsers',
                            perms = [Permission]}}
    end.

%% @doc Attempt to parse a header into
%% a list of grant identifiers and strings.
-type grant_user_identifier() :: 'emailAddress' | 'id' | 'uri'.
-spec parse_grant_header_value(string()) ->
    {ok, [{grant_user_identifier(), string()}]} |
    {error, invalid_argument | unresolved_grant_email | unresolved_grant_canonical_id}.
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
valid_uri(URI) ->
    %% log delivery is not yet a supported option
    lists:member(URI, [?ALL_USERS_GROUP, ?AUTH_USERS_GROUP]).

%% @doc Combine the first and second argument, if the second
%% is wrapped in `ok'. Otherwise return the second argugment.
wrap(_Atom, {error, invalid_argument}=E) ->
    E;
wrap(Atom, {ok, Value}) ->
    {ok, {Atom, Value}}.

%% If `String' is enclosed in quotation marks, remove them. Otherwise
%% return an error.
remove_quotes(String) ->
    case starts_and_ends_with_quotes(String) of
        false ->
            {error, invalid_argument};
        true ->
            {ok, string:sub_string(String, 2, length(String) - 1)}
    end.

%% @doc Return true if `String' is enclosed in quotation
%% marks. The enclosed string must also be non-empty.
starts_and_ends_with_quotes(String) ->
    length(String) > 2 andalso
    hd(String) =:= 34 andalso
    lists:last(String) =:= 34.

%% @doc Attempt to turn a list of grants that use email addresses
%% into a list of grants that only use canonical ids. Returns an error
%% if any of the emails cannot be turned into canonical ids.
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

email_grant({Atom, _Val}) ->
    Atom =:= 'emailAddress'.

%% @doc Turn a list of ok-values or errors into either
%% an ok of list, or an error. Returns the latter is any
%% of the values in the input list are an error.
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
fail_either(_Elem, {{error, _Reason}=E, Acc}) ->
    {E, Acc};
fail_either(E={error, _Reason}, {_OkOrError, Acc}) ->
    %% don't cons the error onto the acc
    {E, Acc};
fail_either({ok, Val}, {_OkOrError, Acc}) ->
    {ok, [Val | Acc]}.

%% @doc Convert an XML document representing an ACL into
%% an internal representation.
-spec acl_from_xml(string(), binary(), riak_client()) ->
          {ok, acl()} | {error, invalid_argument | unresolved_grant_email | malformed_acl_error}.
acl_from_xml(Xml, KeyId, RcPid) ->
    case riak_cs_xml:scan(Xml) of
        {error, malformed_xml} -> {error, malformed_acl_error};
        {ok, ParsedData} ->
            BareAcl = ?ACL{owner = #{display_name => undefined,
                                     key_id => KeyId}},
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

-spec check_grants(undefined | rcs_user(), binary(), atom(), riak_client(), acl() | undefined) ->
          boolean() | {true, string()}.
check_grants(undefined, Bucket, RequestedAccess, RcPid, BucketAcl) ->
    riak_cs_acl:anonymous_bucket_access(Bucket, RequestedAccess, RcPid, BucketAcl);
check_grants(User, Bucket, RequestedAccess, RcPid, BucketAcl) ->
    riak_cs_acl:bucket_access(Bucket,
                              RequestedAccess,
                              User?RCS_USER.canonical_id,
                              RcPid,
                              BucketAcl).

-spec validate_acl({ok, acl()} | {error, term()}, binary()) ->
          {ok, acl()} | {error, access_denied}.
validate_acl({ok, Acl = ?ACL{owner = #{canonical_id := Id}}}, Id) ->
    {ok, Acl};
validate_acl({ok, _}, _) ->
    {error, access_denied};
validate_acl({error, _} = Error, _) ->
    Error.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Update the permissions for a grant in the provided
%% list of grants if an entry exists with matching grantee
%% data or add a grant to a list of grants.
add_grant(NewGrant, Grants) ->
    ?ACL_GRANT{grantee = NewGrantee,
               perms = NewPerms} = NewGrant,
    SplitFun = fun(?ACL_GRANT{grantee = Grantee}) ->
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
            FoldFun = fun(?ACL_GRANT{perms = Perms}, Acc) ->
                              lists:usort(Perms ++ Acc)
                      end,
            UpdPerms = lists:foldl(FoldFun, NewPerms, GranteeGrants),
            [?ACL_GRANT{grantee = NewGrantee,
                        perms = UpdPerms} | OtherGrants]
    end.

%% @doc Get the list of grants for a canned ACL
-spec canned_acl_grants(string(),
                        acl_owner(),
                        undefined | acl_owner()) -> [acl_grant()].
canned_acl_grants("public-read", Owner, _) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']},
     ?ACL_GRANT{grantee = 'AllUsers', perms = ['READ']}];
canned_acl_grants("public-read-write", Owner, _) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']},
     ?ACL_GRANT{grantee = 'AllUsers', perms = ['READ', 'WRITE']}];
canned_acl_grants("authenticated-read", Owner, _) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']},
     ?ACL_GRANT{grantee = 'AuthUsers', perms = ['READ']}];
canned_acl_grants("bucket-owner-read", Owner, undefined) ->
    canned_acl_grants("private", Owner, undefined);
canned_acl_grants("bucket-owner-read", Owner, Owner) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']}];
canned_acl_grants("bucket-owner-read", Owner, BucketOwner) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']},
     ?ACL_GRANT{grantee = BucketOwner, perms = ['READ']}];
canned_acl_grants("bucket-owner-full-control", Owner, undefined) ->
    canned_acl_grants("private", Owner, undefined);
canned_acl_grants("bucket-owner-full-control", Owner, Owner) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']}];
canned_acl_grants("bucket-owner-full-control", Owner, BucketOwner) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']},
     ?ACL_GRANT{grantee = BucketOwner, perms = ['FULL_CONTROL']}];
canned_acl_grants(_, Owner, _) ->
    [?ACL_GRANT{grantee = Owner, perms = ['FULL_CONTROL']}].


%% @doc Get the canonical id of the user associated with
%% a given email address.
canonical_for_email(Email, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_user(#{email => Email}, Pbc) of
        {ok, {User, _}} ->
            {ok, User?RCS_USER.canonical_id};
        {error, Reason} ->
            logger:notice("Failed to find user with email ~s: ~p", [Email, Reason]),
            {error, unresolved_grant_email}
    end.

%% @doc Get the display name of the user associated with
%% a given canonical id.
name_for_canonical(Id, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_user(#{canonical_id => Id}, Pbc) of
        {ok, {User, _}} ->
            {ok, User?RCS_USER.display_name};
        {error, Reason} ->
            logger:notice("Failed to find user with canonical_id ~s: ~p", [Id, Reason]),
            {error, unresolved_grant_canonical_id}
    end.

user_details_for_canonical(Id, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_user(#{canonical_id => Id}, Pbc) of
        {ok, {?RCS_USER{email = Email, display_name = DisplayName}, _}} ->
            {ok, {Email, DisplayName}};
        {error, Reason} ->
            logger:notice("Failed to find user with canonical_id ~s: ~p", [Id, Reason]),
            {error, unresolved_grant_id}
    end.

%% @doc Process the top-level elements of the
process_acl_contents([], Acl, _) ->
    {ok, Acl};
process_acl_contents([#xmlElement{content=Content,
                                  name=ElementName}
                     | RestElements], Acl, RcPid) ->
    UpdAclRes =
        case ElementName of
            'Owner' ->
                process_owner(Content, Acl, RcPid);
            'AccessControlList' ->
                process_grants(Content, Acl, RcPid);
            _ ->
                logger:notice("Unexpected element encountered while processing ACL content: ~p", [ElementName]),
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
process_owner([], Acl = ?ACL{owner = #{canonical_id := CanonicalId} = Owner}, RcPid) ->
    case maps:get(display_name, Owner, undefined) of
        undefined ->
            case name_for_canonical(CanonicalId, RcPid) of
                {ok, DisplayName} ->
                    {ok, Acl?ACL{owner = Owner#{display_name => DisplayName}}};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {ok, Acl}
    end;
process_owner([], Acl, _) ->
    {ok, Acl};
process_owner([#xmlElement{content = [Content],
                           name = ElementName} |
               RestElements], Acl, RcPid) ->
    Owner = Acl?ACL.owner,
    case Content of
        #xmlText{value = Value} ->
            UpdOwner =
                case ElementName of
                    'ID' ->
                        Owner#{canonical_id => list_to_binary(Value)};
                    'DisplayName' ->
                        Owner#{display_name => list_to_binary(Value)};
                    _ ->
                        logger:warning("Encountered unexpected element: ~p", [ElementName]),
                        Owner
            end,
            process_owner(RestElements, Acl?ACL{owner = UpdOwner}, RcPid);
        _ ->
            process_owner(RestElements, Acl, RcPid)
    end;
process_owner([_ | RestElements], Acl, RcPid) ->
    %% this pattern matches with text, comment, etc..
    process_owner(RestElements, Acl, RcPid).

%% @doc Process an XML element containing the grants for the acl.
process_grants([], Acl, _) ->
    {ok, Acl};
process_grants([#xmlElement{content = Content,
                            name = ElementName} |
                RestElements], Acl, RcPid) ->
    UpdAcl =
        case ElementName of
            'Grant' ->
                Grant = process_grant(
                          Content,
                          ?ACL_GRANT{grantee = #{display_name => undefined},
                                     perms = []},
                          Acl?ACL.owner, RcPid),
                case Grant of
                    {error, _} ->
                        Grant;
                    _ ->
                        Acl?ACL{grants = add_grant(Grant, Acl?ACL.grants)}
                end;
            _ ->
                logger:warning("Encountered unexpected grants element: ~p", [ElementName]),
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
process_grant([], Grant, _, _) ->
    Grant;
process_grant([#xmlElement{content = Content,
                           name = ElementName} |
               RestElements], Grant, AclOwner, RcPid) ->
    UpdGrant =
        case ElementName of
            'Grantee' ->
                process_grantee(Content, Grant, AclOwner, RcPid);
            'Permission' ->
                process_permission(Content, Grant);
            _ ->
                logger:warning("Encountered unexpected grant element: ~p", [ElementName]),
                {error, invalid_argument}
        end,
    case UpdGrant of
        {error, _} = Error ->
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
process_grantee([], G0 = ?ACL_GRANT{grantee = Gee0}, AclOwner, RcPid) ->
    case Gee0 of
        #{email := Email,
          canonical_id := CanonicalId,
          display_name := DisplayName} when is_binary(Email),
                                            is_binary(CanonicalId),
                                            is_binary(DisplayName) ->
            G0;
        #{email := Email} = M ->
            MaybeConflictingId =
                case maps:get(canonical_id, M, undefined) of
                    undefined ->
                        undefined;
                    Defined ->
                        Defined
                end,
            case canonical_for_email(Email, RcPid) of
                {ok, CanonicalId} when MaybeConflictingId /= undefined,
                                       CanonicalId /= MaybeConflictingId ->
                    logger:notice("ACL has both Email (~s) and ID (~s) but the user with this email has a different canonical_id; "
                                  "treating this ACL as invalid", [Email, MaybeConflictingId]),
                    {error, conflicting_grantee_canonical_id};
                {ok, CanonicalId} ->
                    G0?ACL_GRANT{grantee = Gee0#{canonical_id => CanonicalId}};
                ER ->
                    ER
            end;
        #{canonical_id := CanonicalId} ->
            case AclOwner of
                #{display_name := DisplayName} = M ->
                    G0?ACL_GRANT{grantee = Gee0#{email => maps:get(email, M, undefined),
                                                 display_name => DisplayName}};
                _ ->
                    case user_details_for_canonical(CanonicalId, RcPid) of
                        {ok, {Email, DisplayName}} ->
                            G0?ACL_GRANT{grantee = Gee0#{email => Email,
                                                         display_name => DisplayName}};
                        ER ->
                            ER
                    end
            end;
        _GroupGrantee when is_atom(_GroupGrantee) ->
            G0
    end;
process_grantee([#xmlElement{content = [Content],
                             name = ElementName} |
                 RestElements], ?ACL_GRANT{grantee = Grantee} = G, AclOwner, RcPid) ->
    Value = list_to_binary(Content#xmlText.value),
    UpdGrant =
        case ElementName of
            'ID' ->
                G?ACL_GRANT{grantee = Grantee#{canonical_id => Value}};
            'EmailAddress' ->
                G?ACL_GRANT{grantee = Grantee#{email => Value}};
            'URI' ->
                case Value of
                    ?AUTH_USERS_GROUP ->
                        G?ACL_GRANT{grantee = 'AuthUsers'};
                    ?ALL_USERS_GROUP ->
                        G?ACL_GRANT{grantee = 'AllUsers'};
                    _ ->
                        %% Not yet supporting log delivery group
                        G
                end;
            _ ->
                G
        end,
    case UpdGrant of
        {error, _} ->
            UpdGrant;
        _ ->
            process_grantee(RestElements, UpdGrant, AclOwner, RcPid)
    end;
process_grantee([#xmlText{} | RestElements], Grant, Owner, RcPid) ->
    process_grantee(RestElements, Grant, Owner, RcPid);
process_grantee([#xmlComment{} | RestElements], Grant, Owner, RcPid) ->
    process_grantee(RestElements, Grant, Owner, RcPid).

%% @doc Process an XML element containing information about
%% an ACL permission.
process_permission([Content], G = ?ACL_GRANT{perms = Perms0}) ->
    Value = list_to_existing_atom(Content#xmlText.value),
    Perms9 = case lists:member(Value, Perms0) of
                 true -> Perms0;
                 false -> [Value | Perms0]
             end,
    G?ACL_GRANT{perms = Perms9}.



%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

default_acl_test() ->
    ExpectedXml = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>">>,
    DefaultAcl = default_acl(<<"tester1">>,
                             <<"TESTID1">>,
                             <<"TESTKEYID1">>),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}
                               ]
                     },
                 DefaultAcl),
    ?assertEqual(ExpectedXml, riak_cs_xml:to_xml(DefaultAcl)).

acl_from_xml_test() ->
    Xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy><Owner><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>TESTID1</ID><DisplayName>tester1</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
    DefaultAcl = default_acl(<<"tester1">>, <<"TESTID1">>, <<"TESTKEYID1">>),
    {ok, Acl} = acl_from_xml(Xml, <<"TESTKEYID1">>, undefined),
    #{display_name := ExpectedOwnerName,
      canonical_id := ExpectedOwnerId} = DefaultAcl?ACL.owner,
    #{display_name := ActualOwnerName,
      canonical_id := ActualOwnerId} = Acl?ACL.owner,
    ?ACL{grants = [?ACL_GRANT{grantee = #{display_name := ExpectedOwnerName,
                                          canonical_id := ExpectedOwnerId},
                              perms = ExpectedPerms}]} = DefaultAcl,
    ?ACL{grants = [?ACL_GRANT{grantee = #{display_name := ActualOwnerName,
                                          canonical_id := ActualOwnerId},
                              perms = ActualPerms}]} = Acl,
    ?assertEqual(ExpectedOwnerName, ActualOwnerName),
    ?assertEqual(ExpectedOwnerId, ActualOwnerId),
    ?assertEqual(ExpectedPerms, ActualPerms).

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
    Owner = #{display_name => <<"tester1">>,
              canonical_id => <<"TESTID1">>,
              key_id => <<"TESTKEYID1">>},
    BucketOwner = #{display_name => <<"owner1">>,
                    canonical_id => <<"OWNERID1">>,
                    key_id => <<"OWNERKEYID1">>},
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

    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}]
                     },
                 DefaultAcl),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}]
                     },
                 PrivateAcl),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']},
                                ?ACL_GRANT{grantee = 'AllUsers',
                                           perms = ['READ']}
                               ]
                     },
                 PublicReadAcl),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']},
                                ?ACL_GRANT{grantee = 'AllUsers',
                                           perms = ['READ', 'WRITE']
                                          }
                               ]
                     },
                 PublicRWAcl),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']},
                                ?ACL_GRANT{grantee = 'AuthUsers',
                                           perms = ['READ']}
                               ]
                     },
                 AuthReadAcl),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}]
                     },
                 BucketOwnerReadAcl1),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']},
                                ?ACL_GRANT{grantee = #{display_name := <<"owner1">>,
                                                       canonical_id := <<"OWNERID1">>},
                                           perms = ['READ']}
                               ]
                     },
                 BucketOwnerReadAcl2),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}
                               ]
                     },
                 BucketOwnerReadAcl3),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}
                               ]
                     },
                 BucketOwnerFCAcl1),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']},
                                ?ACL_GRANT{grantee = #{display_name := <<"owner1">>,
                                                       canonical_id := <<"OWNERID1">>},
                                           perms = ['FULL_CONTROL']}
                               ]
                     }, BucketOwnerFCAcl2),
    ?assertMatch(?ACL{owner = #{display_name := <<"tester1">>,
                                canonical_id := <<"TESTID1">>,
                                key_id := <<"TESTKEYID1">>},
                      grants = [?ACL_GRANT{grantee = #{display_name := <<"tester1">>,
                                                       canonical_id := <<"TESTID1">>},
                                           perms = ['FULL_CONTROL']}]
                     },
                 BucketOwnerFCAcl3).


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
    ?assertEqual(AclFromStripped?ACL{creation_time = Acl?ACL.creation_time},
                 Acl),
    ok.

acl_to_from_json_term_test() ->
    CreationTime = os:system_time(millisecond),
    Acl0 = ?ACL{owner = #{display_name => <<"tester1">>,
                          canonical_id => <<"TESTID1">>,
                          key_id => <<"TESTKEYID1">>},
                grants = [?ACL_GRANT{grantee = #{display_name => <<"tester1">>,
                                                 canonical_id => <<"TESTID1">>},
                                     perms = ['READ']},
                          ?ACL_GRANT{grantee = #{display_name => <<"tester2">>,
                                                 canonical_id => <<"TESTID2">>},
                                     perms = ['WRITE']}],
                creation_time = CreationTime},
    Json = riak_cs_json:to_json(Acl0),
    Acl9 = riak_cs_acl:exprec_acl(
             jsx:decode(Json, [{labels, atom}])),
    ?assertEqual(Acl0, Acl9).

-endif.
