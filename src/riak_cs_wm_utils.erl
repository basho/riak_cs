%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_wm_utils).

-export([service_available/2,
         service_available/3,
         lower_case_method/1,
         iso_8601_datetime/0,
         iso_8601_datetime/1,
         to_iso_8601/1,
         iso_8601_to_rfc_1123/1,
         to_rfc_1123/1,
         iso_8601_to_erl_date/1,
         streaming_get/6,
         find_and_auth_admin/3,
         find_and_auth_user/3,
         find_and_auth_user/4,
         find_and_auth_user/5,
         validate_auth_header/4,
         ensure_doc/2,
         respond_api_error/3,
         deny_access/2,
         deny_invalid_key/2,
         extract_key/2,
         extract_name/1,
         extract_canonical_id/1,
         extract_object_acl/1,
         maybe_update_context_with_acl_from_headers/2,
         maybe_acl_from_context_and_request/3,
         acl_from_headers/4,
         extract_acl_headers/1,
         has_acl_header_and_body/1,
         has_acl_header/1,
         has_canned_acl_and_header_grant/1,
         has_canned_acl_header/1,
         has_specific_acl_header/1,
         has_body/1,
         extract_amazon_headers/1,
         normalize_headers/1,
         extract_user_metadata/1,
         shift_to_owner/4,
         bucket_access_authorize_helper/4,
         object_access_authorize_helper/4,
         object_access_authorize_helper/5,
         check_object_authorization/8,
         translate_bucket_policy/2,
         fetch_bucket_owner/2,
         bucket_owner/1,
         extract_date/1,
         check_timeskew/1,
         content_length/1,
         valid_entity_length/3
        ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(QS_KEYID, "AWSAccessKeyId").
-define(QS_SIGNATURE, "Signature").

-type acl_or_error() ::  {ok, #acl_v2{}} |
                         {error, 'invalid_argument'} |
                         {error, 'unresolved_grant_email'}.

%% ===================================================================
%% Public API
%% ===================================================================

service_available(RD, Ctx) ->
    service_available(request_pool, RD, Ctx).

service_available(Pool, RD, Ctx) ->
    case riak_cs_utils:riak_connection(Pool) of
        {ok, RcPid} ->
            {true, RD, Ctx#context{riak_client=RcPid}};
        {error, _Reason} ->
            {false, RD, Ctx}
    end.

%% @doc Parse an authentication header string and determine
%%      the appropriate module to use to authenticate the request.
%%      The passthru auth can be used either with a KeyID or
%%      anonymously by leving the header empty.
-spec parse_auth_header(string(), boolean()) -> {atom(),
                                                 string() | undefined,
                                                 string() | undefined}.
parse_auth_header(KeyId, true) when KeyId =/= undefined ->
    {riak_cs_passthru_auth, KeyId, undefined};
parse_auth_header("AWS " ++ Key, _) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {riak_cs_s3_auth, KeyId, KeyData};
        Other -> Other
    end;
parse_auth_header(_, _) ->
    {riak_cs_blockall_auth, undefined, undefined}.

%% @doc Parse authentication query parameters and determine
%%      the appropriate module to use to authenticate the request.
%%      The passthru auth can be used either with a KeyID or
%%      anonymously by leving the header empty.
-spec parse_auth_params(string(), string(), boolean()) -> {atom(),
                                                           string() | undefined,
                                                           string() | undefined}.
parse_auth_params(KeyId, _, true) when KeyId =/= undefined ->
    {riak_cs_passthru_auth, KeyId, undefined};
parse_auth_params(undefined, _, true) ->
    {riak_cs_passthru_auth, undefined, undefined};
parse_auth_params(undefined, _, false) ->
    {riak_cs_blockall_auth, undefined, undefined};
parse_auth_params(_, undefined, _) ->
    {riak_cs_blockall_auth, undefined, undefined};
parse_auth_params(KeyId, Signature, _) ->
    {riak_cs_s3_auth, KeyId, Signature}.

%% @doc Lookup the user specified by the access headers, and call
%% `Next(RD, NewCtx)' if there is no auth error.
%%
%% If a user was successfully authed, the `user' and `user_object'
%% fields in the `#context' record passed to `Next' will be filled.
%% If the access is instead anonymous, those fields will be left as
%% they were passed to this function.
%%
%% If authorization fails (a bad key or signature is given, or the
%% Riak lookup fails), a tuple suitable for returning from a
%% webmachine resource's `forbidden/2' function is returned, with
%% appropriate error message included.
find_and_auth_user(RD, ICtx, Next) ->
    find_and_auth_user(RD, ICtx, Next, true).

find_and_auth_user(RD, ICtx, Next, AnonymousOk) ->
    find_and_auth_user(RD, ICtx, Next, fun(X) -> X end, AnonymousOk).

find_and_auth_user(RD,
                   #context{auth_bypass=AuthBypass,
                            riak_client=RcPid}=ICtx,
                   Next,
                   Conv2KeyCtx,
                   AnonymousOk) ->
    handle_validation_response(
      validate_auth_header(RD, AuthBypass, RcPid, ICtx),
      RD,
      ICtx,
      Next,
      Conv2KeyCtx,
      AnonymousOk).

find_and_auth_admin(RD, Ctx, AuthBypass) ->
    Next = fun(NewRD, NewCtx=#context{user=User}) ->
                   handle_auth_admin(NewRD,
                                     NewCtx,
                                     User,
                                     AuthBypass)
           end,
    find_and_auth_user(RD, Ctx, Next, AuthBypass).

handle_validation_response({ok, User, UserObj}, RD, Ctx, Next, _, _) ->
    %% given keyid and signature matched, proceed
    Next(RD, Ctx#context{user=User,
                         user_object=UserObj});
handle_validation_response({error, disconnected}, RD, Ctx, _Next, _, _Bool) ->
    {{halt, 503}, RD, Ctx};
handle_validation_response({error, Reason}, RD, Ctx, Next, _, true) ->
    %% no keyid was given, proceed anonymously
    _ = lager:debug("No user key: ~p", [Reason]),
    Next(RD, Ctx);
handle_validation_response({error, no_user_key}, RD, Ctx, _, Conv2KeyCtx, false) ->
    %% no keyid was given, deny access
    _ = lager:debug("No user key, deny"),
    deny_access(RD, Conv2KeyCtx(Ctx));
handle_validation_response({error, bad_auth}, RD, Ctx, _, Conv2KeyCtx, _) ->
    %% given keyid was found, but signature didn't match
    _ = lager:debug("bad_auth"),
    deny_access(RD, Conv2KeyCtx(Ctx));
handle_validation_response({error, notfound}, RD, Ctx, _, Conv2KeyCtx, _) ->
    %% no keyid was found
    _ = lager:debug("key_id not found"),
    deny_access(RD, Conv2KeyCtx(Ctx));
handle_validation_response({error, Reason}, RD, Ctx, _, Conv2KeyCtx, _) ->
    %% no matching keyid was found, or lookup failed
    _ = lager:debug("Authentication error: ~p", [Reason]),
    deny_invalid_key(RD, Conv2KeyCtx(Ctx)).

handle_auth_admin(RD, Ctx, undefined, true) ->
    {false, RD, Ctx};
handle_auth_admin(RD, Ctx, undefined, false) ->
    %% anonymous access disallowed
    riak_cs_wm_utils:deny_access(RD, Ctx);
handle_auth_admin(RD, Ctx, User, false) ->
    UserKeyId = User?RCS_USER.key_id,
    case riak_cs_config:admin_creds() of
        {ok, {Admin, _}} when Admin == UserKeyId ->
            %% admin account is allowed
            riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>,
                                        [], [<<"false">>, Admin]),
            {false, RD, Ctx};
        _ ->
            %% non-admin account is not allowed -> 403
            Res = riak_cs_wm_utils:deny_access(RD, Ctx),
            riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>, [], [<<"true">>]),
            Res
    end.

%% @doc Look for an Authorization header in the request, and validate
%% it if it exists.  Returns `{ok, User, UserObj}' if validation
%% succeeds, or `{error, KeyId, Reason}' if any step fails.
-spec validate_auth_header(#wm_reqdata{}, term(), riak_client(), #context{}|undefined) ->
                                  {ok, rcs_user(), riakc_obj:riakc_obj()} |
                                  {error, bad_auth | notfound | no_user_key | term()}.
validate_auth_header(RD, AuthBypass, RcPid, Ctx) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    case AuthHeader of
        undefined ->
            %% Check for auth info presented as query params
            KeyId0 = wrq:get_qs_value(?QS_KEYID, RD),
            EncodedSig = wrq:get_qs_value(?QS_SIGNATURE, RD),
            {AuthMod, KeyId, Signature} = parse_auth_params(KeyId0,
                                                            EncodedSig,
                                                            AuthBypass);
        _ ->
            {AuthMod, KeyId, Signature} = parse_auth_header(AuthHeader, AuthBypass)
    end,
    case riak_cs_user:get_user(KeyId, RcPid) of
        {ok, {User, UserObj}} when User?RCS_USER.status =:= enabled ->
            case AuthMod:authenticate(User, Signature, RD, Ctx) of
                ok ->
                    {ok, User, UserObj};
                {error, _Reason} ->
                    %% TODO: are the errors here of small enough
                    %% number that we could just handle them in
                    %% forbidden/2?
                    {error, bad_auth}
            end;
        {ok, _} ->
            %% Disabled account so return 403
            {error, bad_auth};
        {error, NE} when NE == notfound; NE == no_user_key ->
            %% anonymous access lookups don't need to be logged, and
            %% auth failures are logged by other means
            {error, NE};
        {error, Reason} ->
            %% other failures, like Riak fetch timeout, be loud about
            _ = lager:error("Retrieval of user record for ~p failed. Reason: ~p",
                            [KeyId, Reason]),
            {error, Reason}
    end.

%% @doc Utility function for building #key_contest
%% Spawns manifest FSM
-spec ensure_doc(term(), riak_client()) -> term().
ensure_doc(KeyCtx=#key_context{bucket_object=undefined,
                               bucket=Bucket}, RcPid) ->
    case riak_cs_bucket:fetch_bucket_object(Bucket, RcPid) of
        {ok, Obj} ->
            setup_manifest(KeyCtx#key_context{bucket_object = Obj}, RcPid);
        {error, Reason} when Reason =:= notfound orelse Reason =:= no_such_bucket ->
            KeyCtx#key_context{bucket_object = notfound}
    end;
ensure_doc(KeyCtx, _) ->
    KeyCtx.

setup_manifest(KeyCtx=#key_context{bucket=Bucket,
                                   key=Key}, RcPid) ->
    %% start the get_fsm
    BinKey = list_to_binary(Key),
    FetchConcurrency = riak_cs_lfs_utils:fetch_concurrency(),
    BufferFactor = riak_cs_lfs_utils:get_fsm_buffer_size_factor(),
    {ok, FsmPid} = riak_cs_get_fsm_sup:start_get_fsm(node(), Bucket, BinKey,
                                                     self(), RcPid,
                                                     FetchConcurrency,
                                                     BufferFactor),
    Manifest = riak_cs_get_fsm:get_manifest(FsmPid),
    KeyCtx#key_context{get_fsm_pid=FsmPid,
                       manifest=Manifest}.

%% @doc Produce an api error by using response_module.
respond_api_error(RD, Ctx, ErrorAtom) ->
    ResponseMod = Ctx#context.response_module,
    NewRD = maybe_log_user(RD, Ctx),
    ResponseMod:api_error(ErrorAtom, NewRD, Ctx).

%% @doc Only set the user for the access logger to catch if there is a
%% user to catch.
maybe_log_user(RD, Context) ->
    case Context#context.user of
        undefined ->
            RD;
        User ->
            riak_cs_access_log_handler:set_user(User, RD)
    end.

%% @doc Produce an access-denied error message from a webmachine
%% resource's `forbidden/2' function.
deny_access(RD, Ctx=#context{response_module=ResponseMod}) ->
    ResponseMod:api_error(access_denied, RD, Ctx);
deny_access(RD, Ctx) ->
    riak_cs_s3_response:api_error(access_denied, RD, Ctx).



%% @doc Produce an invalid-access-keyid error message from a
%% webmachine resource's `forbidden/2' function.
deny_invalid_key(RD, Ctx=#context{response_module=ResponseMod}) ->
    ResponseMod:api_error(invalid_access_key_id, RD, Ctx).

%% @doc In the case is a user is authorized to perform an operation on
%% a bucket but is not the owner of that bucket this function can be used
%% to switch to the owner's record if it can be retrieved
-spec shift_to_owner(#wm_reqdata{}, #context{}, string(), riak_client()) ->
                            {boolean(), #wm_reqdata{}, #context{}}.
shift_to_owner(RD, Ctx=#context{response_module=ResponseMod}, OwnerId, RcPid)
  when RcPid /= undefined ->
    case riak_cs_user:get_user(OwnerId, RcPid) of
        {ok, {Owner, OwnerObject}} when Owner?RCS_USER.status =:= enabled ->
            AccessRD = riak_cs_access_log_handler:set_user(Owner, RD),
            {false, AccessRD, Ctx#context{user=Owner,
                                          user_object=OwnerObject}};
        {ok, _} ->
            riak_cs_wm_utils:deny_access(RD, Ctx);
        {error, _} ->
            ResponseMod:api_error(bucket_owner_unavailable, RD, Ctx)
    end.

streaming_get(RcPool, RcPid, FsmPid, StartTime, UserName, BFile_str) ->
    case riak_cs_get_fsm:get_next_chunk(FsmPid) of
        {done, Chunk} ->
            ok = riak_cs_stats:update_with_start([object, get], StartTime),
            riak_cs_riak_client:checkin(RcPool, RcPid),
            riak_cs_dtrace:dt_object_return(riak_cs_wm_object, <<"object_get">>,
                                            [], [UserName, BFile_str]),
            {Chunk, done};
        {chunk, Chunk} ->
            {Chunk, fun() -> streaming_get(RcPool, RcPid, FsmPid, StartTime, UserName, BFile_str) end}
    end.

-spec lower_case_method(atom()) -> atom().
lower_case_method('GET') -> get;
lower_case_method('HEAD') -> head;
lower_case_method('POST') -> post;
lower_case_method('PUT') -> put;
lower_case_method('DELETE') -> delete;
lower_case_method('TRACE') -> trace;
lower_case_method('CONNECT') -> connect;
lower_case_method('OPTIONS') -> options.

%% @doc Get an ISO 8601 formatted timestamp representing
%% current time.
-spec iso_8601_datetime() -> string().
iso_8601_datetime() ->
    iso_8601_datetime(erlang:universaltime()).

-spec iso_8601_datetime(calendar:datetime()) -> string().
iso_8601_datetime({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    iso_8601_format(Year, Month, Day, Hour, Min, Sec).

%% @doc Convert an RFC 1123 date into an ISO 8601 formatted timestamp.
-spec to_iso_8601(string()) -> string().
to_iso_8601(Date) ->
    case httpd_util:convert_request_date(Date) of
        {{Year, Month, Day}, {Hour, Min, Sec}} ->
            iso_8601_format(Year, Month, Day, Hour, Min, Sec);
        bad_date ->
            %% Date is already in ISO 8601 format
            Date
    end.

-spec to_rfc_1123(string()) -> string().
to_rfc_1123(Date) when is_list(Date) ->
    case httpd_util:convert_request_date(Date) of
        {{_Year, _Month, _Day}, {_Hour, _Min, _Sec}} ->
            %% Date is already in RFC 1123 format
            Date;
        bad_date ->
            iso_8601_to_rfc_1123(Date)
    end.

%% @doc Convert an ISO 8601 date to RFC 1123 date. This function
%% assumes the input time is already in GMT time.
-spec iso_8601_to_rfc_1123(binary() | string()) -> string().
iso_8601_to_rfc_1123(Date) when is_list(Date) ->
    ErlDate = iso_8601_to_erl_date(Date),
    webmachine_util:rfc1123_date(ErlDate).

%% @doc Convert an ISO 8601 date to Erlang datetime format.
%% This function assumes the input time is already in GMT time.
-spec iso_8601_to_erl_date(binary() | string()) -> calendar:datetime().
iso_8601_to_erl_date(Date) when is_list(Date) ->
    iso_8601_to_erl_date(iolist_to_binary(Date));
iso_8601_to_erl_date(Date)  ->
    case Date of
        %% e.g. "2012-02-17T18:22:50.000Z"
        <<Yr:4/binary, _:1/binary, Mo:2/binary, _:1/binary, Da:2/binary,
          _T:1/binary,
          Hr:2/binary, _:1/binary, Mn:2/binary, _:1/binary, Sc:2/binary,
          _/binary>> ->
            {{b2i(Yr), b2i(Mo), b2i(Da)},
             {b2i(Hr), b2i(Mn), b2i(Sc)}};
        %% e.g. "20130524T000000Z"
        <<Yr:4/binary, Mo:2/binary, Da:2/binary, _:1/binary,
          Hr:2/binary, Mn:2/binary, Sc:2/binary, _/binary>> ->
            {{b2i(Yr), b2i(Mo), b2i(Da)},
             {b2i(Hr), b2i(Mn), b2i(Sc)}}
    end.

%% @doc Return a new context where the bucket and key for the s3
%% object have been inserted. It also does key length check. TODO: do
%% we check if the key is valid Unicode string or not?
-spec extract_key(#wm_reqdata{}, #context{}) ->
                         {ok, #context{}} | {error, {key_too_long, pos_integer()}}.
extract_key(RD,Ctx=#context{local_context=LocalCtx0}) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    %% need to unquote twice since we re-urlencode the string during rewrite in
    %% order to trick webmachine dispatching
    MaxKeyLen = riak_cs_config:max_key_length(),
    case mochiweb_util:unquote(mochiweb_util:unquote(wrq:path_info(object, RD))) of
        Key when length(Key) =< MaxKeyLen ->
            LocalCtx = LocalCtx0#key_context{bucket=Bucket, key=Key},
            {ok, Ctx#context{bucket=Bucket,
                             local_context=LocalCtx}};
        Key ->
            {error, {key_too_long, length(Key)}}
    end.

extract_name(User) when is_list(User) ->
    User;
extract_name(?RCS_USER{name=Name}) ->
    Name;
extract_name(_) ->
    "-unknown-".

%% @doc Add an ACL to the context, from parsing the headers. If there is
%% an error parsing the header, halt the request. If there is no ACL
%% information in the headers, use the default ACL.
-spec maybe_update_context_with_acl_from_headers(#wm_reqdata{}, #context{}) ->
                                                        {error, {{halt, term()}, #wm_reqdata{}, #context{}}} |
                                                        {ok, #context{}}.
maybe_update_context_with_acl_from_headers(RD,
                                           Ctx=#context{user=User,
                                                        bucket=BucketName,
                                                        local_context=LocalCtx,
                                                        riak_client=RcPid}) ->
    case bucket_obj_from_local_context(LocalCtx, BucketName, RcPid) of
        {ok, BucketObject} ->
            case maybe_acl_from_context_and_request(RD, Ctx, BucketObject) of
                {ok, {error, BadAclReason}} ->
                    {error, riak_cs_s3_response:api_error(BadAclReason, RD, Ctx)};
                %% pattern match on the ACL record type for a data-type
                %% sanity-check
                {ok, {ok, Acl=?ACL{}}} ->
                    {ok, Ctx#context{acl=Acl}};
                error ->
                    DefaultAcl = riak_cs_acl_utils:default_acl(User?RCS_USER.display_name,
                                                               User?RCS_USER.canonical_id,
                                                               User?RCS_USER.key_id),
                    {ok, Ctx#context{acl=DefaultAcl}}
            end;
        {error, Reason} ->
            _ = lager:error("Failed to retrieve bucket objects for reason ~p", [Reason]),
            {error, {{halt, 500}, RD, Ctx}}
    end.

-spec bucket_obj_from_local_context(term(), binary(), riak_client()) ->
                                           {ok, term()} | {'error', term()}.
bucket_obj_from_local_context(#key_context{bucket_object=BucketObject},
                              _BucketName, _RcPid) ->
    {ok, BucketObject};
bucket_obj_from_local_context(undefined, BucketName, RcPid) ->
    case riak_cs_bucket:fetch_bucket_object(BucketName, RcPid) of
        {error, notfound} ->
            {ok, undefined};
        {error, no_such_bucket} ->
            {ok, undefined};
        Else ->
            Else
    end.

%% @doc Return an ACL if one can be parsed from the headers. If there
%% are no ACL headers, return `error'. In this case, it's not unexpected
%% to get the `error' value back, but it's name is used for convention.
%% It could also reasonable be called `nothing'.
-spec maybe_acl_from_context_and_request(#wm_reqdata{}, #context{},
                                         riakc_obj:riakc_obj()) ->
                                                {ok, acl_or_error()} | error.
maybe_acl_from_context_and_request(RD, #context{user=User,
                                                riak_client=RcPid},
                                   BucketObj) ->
    case has_acl_header(RD) of
        true ->
            Headers = normalize_headers(RD),
            BucketOwner = bucket_owner(BucketObj),
            Owner = {User?RCS_USER.display_name,
                     User?RCS_USER.canonical_id,
                     User?RCS_USER.key_id},
            {ok, acl_from_headers(Headers, Owner, BucketOwner, RcPid)};
        false ->
            error
    end.

%% TODO: not sure if this should live here or in
%% `riak_cs_acl_utils'
%% @doc Create an acl from the request headers. At this point, we should
%% have already verified that there is only a canned acl header or specific
%% header grants.
-spec acl_from_headers(Headers :: list(),
                       Owner :: acl_owner(),
                       BucketOwner :: undefined | acl_owner(),
                       riak_client()) ->
                              acl_or_error().
acl_from_headers(Headers, Owner, BucketOwner, RcPid) ->
    %% TODO: time to make a macro for `"x-amz-acl"'
    %% `Headers' is an ordset. Is there a faster way to retrieve this? Or
    %% maybe a better data structure?
    case proplists:get_value("x-amz-acl", Headers, {error, undefined}) of
        {error, undefined} ->
            RenamedHeaders = extract_acl_headers(Headers),
            case RenamedHeaders of
                [] ->
                    {DisplayName, CanonicalId, KeyID} = Owner,
                    {ok, riak_cs_acl_utils:default_acl(DisplayName, CanonicalId, KeyID)};
                _Else ->
                    riak_cs_acl_utils:specific_acl_grant(Owner, RenamedHeaders, RcPid)
            end;
        Value ->
            {ok, riak_cs_acl_utils:canned_acl(Value, Owner, BucketOwner)}
    end.


%% @doc Extract the ACL-related headers from a list of headers.
-spec extract_acl_headers(term()) -> [{acl_perm(), string()}].
extract_acl_headers(Headers) ->
    lists:foldl(fun({HeaderName, Value}, Acc) ->
                        case header_name_to_perm(HeaderName) of
                            undefined ->
                                Acc;
                            HeaderAtom ->
                                [{HeaderAtom, Value} | Acc]
                        end
                end,
                [], Headers).

%% @doc Turn a ACL header into the corresponding
%% atom.
-spec header_name_to_perm(list()) -> atom().
header_name_to_perm("x-amz-grant-read") ->
    'READ';
header_name_to_perm("x-amz-grant-write") ->
    'WRITE';
header_name_to_perm("x-amz-grant-read-acp") ->
    'READ_ACP';
header_name_to_perm("x-amz-grant-write-acp") ->
    'WRITE_ACP';
header_name_to_perm("x-amz-grant-full-control") ->
    'FULL_CONTROL';
header_name_to_perm(_Else) ->
    undefined.

%% @doc Return true if the request has both:
%% 1. an ACL-related header
%% 2. a non-empty request body
-spec has_acl_header_and_body(#wm_reqdata{}) -> boolean().
has_acl_header_and_body(RD) ->
    has_acl_header(RD) andalso has_body(RD).

%% @doc Return true if the request has either
%% a canned ACL header, or a specific-grant header.
-spec has_acl_header(#wm_reqdata{}) -> boolean().
has_acl_header(RD) ->
    has_canned_acl_header(RD) orelse has_specific_acl_header(RD).

%% @doc Return true if the request has _both_ a
%% a canned header ACL and a specific-grant header.
-spec has_canned_acl_and_header_grant(#wm_reqdata{}) -> boolean().
has_canned_acl_and_header_grant(RD) ->
    has_canned_acl_header(RD) andalso has_specific_acl_header(RD).

%% @doc Return true if the request uses a canned ACL header.
-spec has_canned_acl_header(#wm_reqdata{}) -> boolean().
has_canned_acl_header(RD) ->
    wrq:get_req_header("x-amz-acl", RD) =/= undefined.

%% @doc Return true if the request has at least one
%% specific-grant header.
-spec has_specific_acl_header(#wm_reqdata{}) -> boolean().
has_specific_acl_header(RD) ->
    Headers = normalize_headers(RD),
    extract_acl_headers(Headers) =/= [].

%% @doc Return true if the request has a non-empty body.
-spec has_body(#wm_reqdata{}) -> boolean().
%% TODO: should we just check if the content-length is 0 instead?
has_body(RD) ->
    wrq:req_body(RD) =/= <<>>.

extract_amazon_headers(Headers) ->
    FilterFun =
        fun({K, V}, Acc) ->
                case lists:prefix("x-amz-", K) of
                    true ->
                        V2 = unicode:characters_to_binary(V, utf8),
                        [[K, ":", V2, "\n"] | Acc];
                    false ->
                        Acc
                end
        end,
    ordsets:from_list(lists:foldl(FilterFun, [], Headers)).

%% @doc Extract user metadata from request header
%% Expires, Content-Disposition, Content-Encoding, Cache-Control and x-amz-meta-*
%% TODO: pass in x-amz-server-side-encryption?
%% TODO: pass in x-amz-storage-class?
%% TODO: pass in x-amz-grant-* headers?
-spec extract_user_metadata(#wm_reqdata{}) -> proplists:proplist().
extract_user_metadata(RD) ->
    extract_user_metadata(get_request_headers(RD), []).

get_request_headers(RD) ->
    mochiweb_headers:to_list(wrq:req_headers(RD)).

normalize_headers(RD) ->
    Headers = get_request_headers(RD),
    FilterFun =
        fun({K, V}, Acc) ->
                LowerKey = string:to_lower(any_to_list(K)),
                [{LowerKey, V} | Acc]
        end,
    ordsets:from_list(lists:foldl(FilterFun, [], Headers)).

extract_user_metadata([], Acc) ->
    Acc;
extract_user_metadata([{Name, Value} | Headers], Acc)
  when Name =:= 'Expires' orelse Name =:= 'Content-Encoding'
       orelse Name =:= "Content-Disposition" orelse Name =:= 'Cache-Control' ->
    extract_user_metadata(
      Headers, [{any_to_list(Name), unicode:characters_to_list(Value, utf8)} | Acc]);
extract_user_metadata([{Name, Value} | Headers], Acc) when is_list(Name) ->
    LowerName = string:to_lower(any_to_list(Name)),
    case LowerName of
        "x-amz-meta" ++ _ ->
            extract_user_metadata(
              Headers, [{LowerName, unicode:characters_to_list(Value, utf8)} | Acc]);
        _ ->
            extract_user_metadata(Headers, Acc)
    end;
extract_user_metadata([_ | Headers], Acc) ->
    extract_user_metadata(Headers, Acc).

-spec bucket_access_authorize_helper(AccessType::atom(), boolean(),
                                     RD::term(), Ctx::#context{}) -> term().
bucket_access_authorize_helper(AccessType, Deletable, RD, Ctx) ->
    #context{riak_client=RcPid,
             policy_module=PolicyMod} = Ctx,
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, is_acl_request(AccessType)),
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    PermCtx = Ctx#context{bucket=Bucket,
                          requested_perm=RequestedAccess},
    handle_bucket_acl_policy_response(
      riak_cs_bucket:get_bucket_acl_policy(Bucket, PolicyMod, RcPid),
      AccessType,
      Deletable,
      RD,
      PermCtx).

handle_bucket_acl_policy_response({error, notfound}, _, _, RD, Ctx) ->
    ResponseMod = Ctx#context.response_module,
    ResponseMod:api_error(no_such_bucket, RD, Ctx);
handle_bucket_acl_policy_response({error, Reason}, _, _, RD, Ctx) ->
    ResponseMod = Ctx#context.response_module,
    ResponseMod:api_error(Reason, RD, Ctx);
handle_bucket_acl_policy_response({Acl, Policy}, AccessType, DeleteEligible, RD, Ctx) ->
    #context{bucket=Bucket,
             riak_client=RcPid,
             user=User,
             requested_perm=RequestedAccess} = Ctx,
    AclCheckRes = riak_cs_acl_utils:check_grants(User,
                                                 Bucket,
                                                 RequestedAccess,
                                                 RcPid,
                                                 Acl),
    Deletable = DeleteEligible andalso (RequestedAccess =:= 'WRITE'),
    handle_acl_check_result(AclCheckRes, Acl, Policy, AccessType, Deletable, RD, Ctx).

handle_acl_check_result(true, _, undefined, _, _, RD, Ctx) ->
    %% because users are not allowed to create/destroy
    %% buckets, we can assume that User is not
    %% undefined here
    AccessRD = riak_cs_access_log_handler:set_user(Ctx#context.user, RD),
    {false, AccessRD, Ctx};
handle_acl_check_result(true, _, Policy, AccessType, _, RD, Ctx) ->
    %% because users are not allowed to create/destroy
    %% buckets, we can assume that User is not
    %% undefined here
    User = Ctx#context.user,
    PolicyMod = Ctx#context.policy_module,
    AccessRD = riak_cs_access_log_handler:set_user(User, RD),
    Access = PolicyMod:reqdata_to_access(RD, AccessType,
                                         User?RCS_USER.canonical_id),
    case PolicyMod:eval(Access, Policy) of
        false ->     riak_cs_wm_utils:deny_access(AccessRD, Ctx);
        _ ->      {false, AccessRD, Ctx}
    end;
handle_acl_check_result({true, _OwnerId}, _, _, _, true, RD, Ctx) ->
    %% grants lied: this is a delete, and only the owner is allowed to
    %% do that; setting user for the request anyway, so the error
    %% tally is logged for them
    AccessRD = riak_cs_access_log_handler:set_user(Ctx#context.user, RD),
    riak_cs_wm_utils:deny_access(AccessRD, Ctx);
handle_acl_check_result({true, OwnerId}, _, _, _, _, RD, Ctx) ->
    %% this operation is allowed, but we need to get the owner's
    %% record, and log the access against them instead of the actor
    riak_cs_wm_utils:shift_to_owner(RD, Ctx, OwnerId, Ctx#context.riak_client);
handle_acl_check_result(false, _, undefined, _, _Deletable, RD, Ctx) ->
    %% No policy so emulate a policy eval failure to avoid code duplication
    handle_policy_eval_result(Ctx#context.user, false, undefined, RD, Ctx);
handle_acl_check_result(false, Acl, Policy, AccessType, _Deletable, RD, Ctx) ->
    #context{riak_client=RcPid,
             user=User0} = Ctx,
    PolicyMod = Ctx#context.policy_module,
    User = case User0 of
               undefined -> undefined;
               _ ->         User0?RCS_USER.canonical_id
           end,
    Access = PolicyMod:reqdata_to_access(RD, AccessType, User),
    PolicyResult = PolicyMod:eval(Access, Policy),
    OwnerId = riak_cs_acl:owner_id(Acl, RcPid),
    handle_policy_eval_result(User, PolicyResult, OwnerId, RD, Ctx).

handle_policy_eval_result(_, true, OwnerId, RD, Ctx) ->
    %% Policy says yes while ACL says no
    shift_to_owner(RD, Ctx, OwnerId, Ctx#context.riak_client);
handle_policy_eval_result(User, _, _, RD, Ctx) ->
    %% Policy says no
    #context{riak_client=RcPid,
             response_module=ResponseMod,
             user=User,
             bucket=Bucket} = Ctx,
    %% log bad requests against the actors that make them
    AccessRD = riak_cs_access_log_handler:set_user(User, RD),
    %% Check if the bucket actually exists so we can
    %% make the correct decision to return a 404 or 403
    case riak_cs_bucket:fetch_bucket_object(Bucket, RcPid) of
        {ok, _} ->
            riak_cs_wm_utils:deny_access(AccessRD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end.

-spec is_acl_request(atom()) -> boolean().
is_acl_request(ReqType) when ReqType =:= bucket_acl orelse
                             ReqType =:= object_acl ->
    true;
is_acl_request(_) ->
    false.

-type halt_or_bool() :: {halt, pos_integer()} | boolean().
-type authorized_response() :: {halt_or_bool(), RD :: #wm_reqdata{}, Ctx :: #context{}}.

-spec object_access_authorize_helper(AccessType::atom(), boolean(),
                                     RD:: #wm_reqdata{}, Ctx:: #context{}) ->
                                            authorized_response().
object_access_authorize_helper(AccessType, Deletable, RD, Ctx) ->
    object_access_authorize_helper(AccessType, Deletable, false, RD, Ctx).

-spec object_access_authorize_helper(AccessType::atom(), boolean(), boolean(),
                                     RD:: #wm_reqdata{}, Ctx:: #context{}) ->
                                            authorized_response().
object_access_authorize_helper(AccessType, Deletable, SkipAcl,
                               RD, #context{policy_module=PolicyMod,
                                            local_context=LocalCtx,
                                            user=User,
                                            riak_client=RcPid,
                                            response_module=ResponseMod}=Ctx)
  when ( AccessType =:= object_acl orelse
         AccessType =:= object_part orelse
         AccessType =:= object )
       andalso is_boolean(Deletable)
       andalso is_boolean(SkipAcl) ->
    #key_context{bucket_object=BucketObj} = LocalCtx,
    case translate_bucket_policy(PolicyMod, BucketObj) of
        {error, multiple_bucket_owners=E} ->
            %% We want to bail out early if there are siblings when
            %% retrieving the bucket policy
            ResponseMod:api_error(E, RD, Ctx);
        {error, notfound} ->
            %% The call to `fetch_bucket_object' returned `notfound'
            %% so we can assume to bucket does not exist.
            ResponseMod:api_error(no_such_bucket, RD, Ctx);
        Policy ->
            Method = wrq:method(RD),
            CanonicalId = extract_canonical_id(User),
            Access = PolicyMod:reqdata_to_access(RD, AccessType, CanonicalId),
            #key_context{bucket=_Bucket, bucket_object=BucketObj, manifest=Manifest} = LocalCtx,
            ObjectAcl = extract_object_acl(Manifest),

            case check_object_authorization(Access, SkipAcl, ObjectAcl,
                                            Policy, CanonicalId,
                                            PolicyMod, RcPid, BucketObj) of
                {error, actor_is_owner_but_denied_policy} ->
                    %% return forbidden or 404 based on the `Method' and `Deletable'
                    %% values
                    actor_is_owner_but_denied_policy(User, RD, Ctx, Method, Deletable);
                {ok, actor_is_owner_and_allowed_policy} ->
                    %% actor is the owner
                    %% Quota hook here
                    case riak_cs_quota:invoke_all_callbacks(User, Access, Ctx) of
                        {ok, RD2, Ctx2} ->
                            actor_is_owner_and_allowed_policy(User, RD2, Ctx2, LocalCtx);
                        {error, Module, Reason, RD3, Ctx3} ->
                            riak_cs_quota:handle_error(Module, Reason, RD3, Ctx3)
                    end;
                {error, {actor_is_not_owner_and_denied_policy, OwnerId}} ->
                    actor_is_not_owner_and_denied_policy(OwnerId, RD, Ctx,
                                                         Method, Deletable);
                {ok, {actor_is_not_owner_but_allowed_policy, OwnerId}} ->
                    %% actor is not the owner
                    %% Quota hook here
                    case riak_cs_quota:invoke_all_callbacks(OwnerId, Access, Ctx) of
                        {ok, RD2, Ctx2} ->
                            actor_is_not_owner_but_allowed_policy(User, OwnerId, RD2, Ctx2, LocalCtx);
                        {error, Module, Reason, RD3, Ctx3} ->
                            riak_cs_quota:handle_error(Module, Reason, RD3, Ctx3)
                    end;
                {ok, just_allowed_by_policy} ->
                    %% actor is not the owner, not permitted by ACL but permitted by policy
                    %% Quota hook here
                    OwnerId = riak_cs_acl:owner_id(ObjectAcl, RcPid),
                    case riak_cs_quota:invoke_all_callbacks(OwnerId, Access, Ctx) of
                        {ok, RD2, Ctx2} ->
                            just_allowed_by_policy(OwnerId, RD2, Ctx2, LocalCtx);
                        {error, Module, Reason, RD3, Ctx3} ->
                            riak_cs_quota:handle_error(Module, Reason, RD3, Ctx3)
                    end;
                {error, access_denied} ->
                    riak_cs_wm_utils:deny_access(RD, Ctx)
            end
    end.


-spec check_object_authorization(access(), boolean(), undefined|acl(), policy(),
                                 undefined|string(), atom(), riak_client(), riakc_obj:riakc_obj()) ->
                                        {ok, actor_is_owner_and_allowed_policy |
                                         {actor_is_not_owner_but_allowed_policy, string()} |
                                         just_allowed_by_policy} |
                                        {error, actor_is_owner_but_denied_policy |
                                         {actor_is_not_owner_and_denied_policy, string()} |
                                         access_denied}.
check_object_authorization(Access, SkipAcl, ObjectAcl, Policy,
                           CanonicalId, PolicyMod,
                           RcPid, BucketObj) ->
    #access_v1{method = Method, target = AccessType} = Access,
    RequestedAccess = requested_access_helper(AccessType, Method),
    Acl = case SkipAcl of
              true -> true;
              false -> riak_cs_acl:object_access(BucketObj,
                                                 ObjectAcl,
                                                 RequestedAccess,
                                                 CanonicalId,
                                                 RcPid)
          end,
    case {Acl, PolicyMod:eval(Access, Policy)} of
        {true, false} ->
            {error, actor_is_owner_but_denied_policy};
        {true, _} ->
            {ok, actor_is_owner_and_allowed_policy};
        {{true, OwnerId}, false} ->
            {error, {actor_is_not_owner_and_denied_policy, OwnerId}};
        {{true, OwnerId}, _} ->
            {ok, {actor_is_not_owner_but_allowed_policy, OwnerId}};
        {false, true} ->
            %% actor is not the owner, not permitted by ACL but permitted by policy
            {ok, just_allowed_by_policy};
        {false, _} ->
            %% policy says undefined or false
            %% ACL check failed, deny access
            {error, access_denied}
    end.

%% ===================================================================
%% object_acces_authorize_helper helper functions

-spec extract_canonical_id(rcs_user() | undefined) ->
                                  undefined | string().
extract_canonical_id(undefined) ->
    undefined;
extract_canonical_id(?RCS_USER{canonical_id=CanonicalID}) ->
    CanonicalID.

-spec requested_access_helper(object | object_part | object_acl, atom()) ->
                                     acl_perm().
requested_access_helper(object, Method) ->
    riak_cs_acl_utils:requested_access(Method, false);
requested_access_helper(object_part, Method) ->
    requested_access_helper(object, Method);
requested_access_helper(object_acl, Method) ->
    riak_cs_acl_utils:requested_access(Method, true).

-spec extract_object_acl(notfound | lfs_manifest()) ->
                                undefined | acl().
extract_object_acl(Manifest) ->
    riak_cs_manifest:object_acl(Manifest).

-spec translate_bucket_policy(atom(), riakc_obj:riakc_obj()) ->
                                     policy() |
                                     undefined |
                                     {error, multiple_bucket_owners} |
                                     {error, notfound}.
translate_bucket_policy(PolicyMod, BucketObj) ->
    case PolicyMod:bucket_policy(BucketObj) of
        {ok, P} ->
            P;
        {error, policy_undefined} ->
            undefined;
        {error, notfound}=Error1 ->
            Error1;
        {error, multiple_bucket_owners}=Error2 ->
            Error2
    end.

%% Helper functions for dealing with combinations of Object ACL
%% and (bucket) Policy


-spec actor_is_owner_but_denied_policy(User :: rcs_user(),
                                       RD :: term(),
                                       Ctx :: term(),
                                       Method :: atom(),
                                       Deletable :: boolean()) ->
                                              authorized_response().
actor_is_owner_but_denied_policy(User, RD, Ctx, Method, Deletable)
  when Method =:= 'PUT' orelse
       Method =:= 'POST' orelse
       (Deletable andalso Method =:= 'DELETE') ->
    AccessRD = riak_cs_access_log_handler:set_user(User, RD),
    riak_cs_wm_utils:deny_access(AccessRD, Ctx);
actor_is_owner_but_denied_policy(User, RD, Ctx, Method, Deletable)
  when Method =:= 'GET' orelse
       (Deletable andalso Method =:= 'HEAD') ->
    {{halt, {404, "Not Found"}}, riak_cs_access_log_handler:set_user(User, RD), Ctx}.

-spec actor_is_owner_and_allowed_policy(User :: rcs_user(),
                                        RD :: term(),
                                        Ctx :: term(),
                                        LocalCtx :: term()) ->
                                               authorized_response().
actor_is_owner_and_allowed_policy(undefined, RD, Ctx, _LocalCtx) ->
    {false, RD, Ctx};
actor_is_owner_and_allowed_policy(User, RD, Ctx, LocalCtx) ->
    AccessRD = riak_cs_access_log_handler:set_user(User, RD),
    UpdLocalCtx = LocalCtx#key_context{owner=User?RCS_USER.key_id},
    {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}}.

-spec actor_is_not_owner_and_denied_policy(OwnerId :: string(),
                                           RD :: term(),
                                           Ctx :: term(),
                                           Method :: atom(),
                                           Deletable :: boolean()) ->
                                                  authorized_response().
actor_is_not_owner_and_denied_policy(OwnerId, RD, Ctx, Method, Deletable)
  when Method =:= 'PUT' orelse
       (Deletable andalso Method =:= 'DELETE') ->
    AccessRD = riak_cs_access_log_handler:set_user(OwnerId, RD),
    riak_cs_wm_utils:deny_access(AccessRD, Ctx);
actor_is_not_owner_and_denied_policy(_OwnerId, RD, Ctx, Method, Deletable)
  when Method =:= 'GET' orelse
       (Deletable andalso Method =:= 'HEAD') ->
    {{halt, {404, "Not Found"}}, RD, Ctx}.

-spec actor_is_not_owner_but_allowed_policy(User :: rcs_user(),
                                            OwnerId :: string(),
                                            RD :: term(),
                                            Ctx :: term(),
                                            LocalCtx :: term()) ->
                                                   authorized_response().
actor_is_not_owner_but_allowed_policy(undefined, OwnerId, RD, Ctx, LocalCtx) ->
    %% This is an anonymous request so shift to the context of the
    %% owner for the remainder of the request.
    AccessRD = riak_cs_access_log_handler:set_user(OwnerId, RD),
    UpdCtx = Ctx#context{local_context=LocalCtx#key_context{owner=OwnerId}},
    shift_to_owner(AccessRD, UpdCtx, OwnerId, Ctx#context.riak_client);
actor_is_not_owner_but_allowed_policy(_, OwnerId, RD, Ctx, LocalCtx) ->
    AccessRD = riak_cs_access_log_handler:set_user(OwnerId, RD),
    UpdCtx = Ctx#context{local_context=LocalCtx#key_context{owner=OwnerId}},
    {false, AccessRD, UpdCtx}.

-spec just_allowed_by_policy(OwnerId :: string(),
                             RD :: term(),
                             Ctx :: term(),
                             LocalCtx :: term()) ->
                                    authorized_response().
just_allowed_by_policy(OwnerId, RD, Ctx, LocalCtx) ->
    AccessRD = riak_cs_access_log_handler:set_user(OwnerId, RD),
    UpdLocalCtx = LocalCtx#key_context{owner=OwnerId},
    {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}}.

-spec fetch_bucket_owner(binary(), riak_client()) -> undefined | acl_owner().
fetch_bucket_owner(Bucket, RcPid) ->
    case riak_cs_acl:fetch_bucket_acl(Bucket, RcPid) of
        {ok, Acl} ->
            Acl?ACL.owner;
        {error, Reason} ->
            _ = lager:debug("Failed to retrieve owner info for bucket ~p. Reason ~p", [Bucket, Reason]),
            undefined
    end.

-spec bucket_owner(undefined | riakc_obj:riakc_obj()) -> undefined | acl_owner().
bucket_owner(undefined) ->
    undefined;
bucket_owner(BucketObj) ->
    {ok, Acl} = riak_cs_acl:bucket_acl(BucketObj),
    Acl?ACL.owner.

-spec extract_date(#wm_reqdata{}) -> calendar:datetime().
extract_date(RD) ->
    Date1 = case wrq:get_req_header("x-amz-date", RD) of
               undefined -> wrq:get_req_header("date", RD);
               Date0 ->     Date0
            end,
    case httpd_util:convert_request_date(Date1) of
        {{_, _, _}, {_, _, _}} = Date ->
            Date;
        bad_date ->
            iso_8601_to_erl_date(Date1)
    end.

-spec check_timeskew(calendar:datetime()) -> boolean().
check_timeskew(ReqTimestamp) when is_tuple(ReqTimestamp)->
    ReqTimestampSec = calendar:datetime_to_gregorian_seconds(ReqTimestamp),
    NowSec = calendar:datetime_to_gregorian_seconds(calendar:now_to_universal_time(os:timestamp())),
    Skew = ReqTimestampSec - NowSec,
    %% This configuration is only for testing;
    case application:get_env(riak_cs, verify_client_clock_skew) of
        {ok, false} ->
            true;
        _ ->
            erlang:abs(Skew) < 900 %% 15 minutes
    end;
check_timeskew(_) ->
    false.

-spec content_length(#wm_reqdata{}) -> undefined | non_neg_integer() | {error, term()}.
content_length(RD) ->
    case wrq:get_req_header("Content-Length", RD) of
        undefined -> undefined;
        CL ->
            case (catch list_to_integer(CL)) of
                Length when is_integer(Length) andalso 0 =< Length -> Length;
                _Other -> {error, CL}
            end
    end.

%% `valid_entity_length' helper.
%% Other than PUT, any Content-Length is allowed including undefined.
%% For PUT, not Copy, Content-Length is mandatory (at least in v2 auth
%% scheme) and the value should be smaller than the upper bound of
%% single request entity size.
%% On the other hand, for PUT Copy, Content-Length is not mandatory.
%% If it exists, however, it should be ZERO.
valid_entity_length(MaxLen, RD, #context{response_module=ResponseMod,
                                         local_context=LocalCtx} = Ctx) ->
    case {wrq:method(RD), wrq:get_req_header("x-amz-copy-source", RD)} of
        {'PUT', undefined} ->
            MaxLen = riak_cs_lfs_utils:max_content_len(),
            case riak_cs_wm_utils:content_length(RD) of
                Length when is_integer(Length) andalso
                            Length =< MaxLen ->
                    UpdLocalCtx = LocalCtx#key_context{size=Length},
                    {true, RD, Ctx#context{local_context=UpdLocalCtx}};
                Length when is_integer(Length) ->
                    ResponseMod:api_error(entity_too_large, RD, Ctx);
                _ -> {false, RD, Ctx}
            end;
        {'PUT', _Source} ->
            case riak_cs_wm_utils:content_length(RD) of
                CL when CL =:= 0 orelse CL =:= undefined ->
                    UpdLocalCtx = LocalCtx#key_context{size=0},
                    {true, RD, Ctx#context{local_context=UpdLocalCtx}};
                _ -> {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V);
any_to_list(V) when is_integer(V) ->
    integer_to_list(V).

%% @doc Get an ISO 8601 formatted timestamp representing
%% current time.
-spec iso_8601_format(pos_integer(),
                      pos_integer(),
                      pos_integer(),
                      non_neg_integer(),
                      non_neg_integer(),
                      non_neg_integer()) -> string().
iso_8601_format(Year, Month, Day, Hour, Min, Sec) ->
    lists:flatten(
      io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.000Z",
                    [Year, Month, Day, Hour, Min, Sec])).

b2i(Bin) ->
    list_to_integer(binary_to_list(Bin)).
