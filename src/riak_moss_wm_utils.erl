%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_utils).

-export([service_available/2,
         parse_auth_header/2,
         ensure_doc/1,
         iso_8601_datetime/0,
         to_iso_8601/1,
         iso_8601_to_rfc_1123/1,
         to_rfc_1123/1,
         streaming_get/1,
         user_record_to_proplist/1,
         find_and_auth_user/3,
         deny_access/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

service_available(RD, KeyCtx=#key_context{context=Ctx}) ->
    case service_available(RD, Ctx) of
        {true, UpdRD, UpdCtx} ->
            {true, UpdRD, KeyCtx#key_context{context=UpdCtx}};
        {false, _, _} ->
            {false, RD, KeyCtx}
    end;
service_available(RD, Ctx) ->
    case riak_moss_utils:riak_connection() of
        {ok, Pid} ->
            {true, RD, Ctx#context{riakc_pid=Pid}};
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
    {riak_moss_passthru_auth, KeyId, undefined};
parse_auth_header(_, true) ->
    {riak_moss_passthru_auth, [], undefined};
parse_auth_header(undefined, false) ->
    {riak_moss_blockall_auth, undefined, undefined};
parse_auth_header("AWS " ++ Key, _) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {riak_moss_s3_auth, KeyId, KeyData};
        Other -> Other
    end;
parse_auth_header(_, _) ->
    {riak_moss_blockall_auth, undefined, undefined}.

%% @doc Lookup the user specified by the access headers, and call
%% `Next(RD, NewCtx)' if there is no auth error.
%%
%% If a user was successfully authed, the `user' and `user_vclock'
%% fields in the `#context' record passed to `Next' will be filled.
%% If the access is instead anonymous, those fields will be left as
%% they were passed to this function.
%%
%% If authorization fails (a bad key or signature is given, or the
%% Riak lookup fails), a tuple suitable for returning from a
%% webmachine resource's `forbidden/2' function is returned, with
%% appropriate error message included.
find_and_auth_user(RD, #context{auth_bypass=AuthBypass,
                                riakc_pid=RiakPid}=Ctx, Next) ->
    case validate_auth_header(RD, AuthBypass, RiakPid) of
        {ok, User, UserVclock} ->
            %% given keyid and signature matched, proceed
            NewCtx = Ctx#context{user=User,
                                 user_vclock=UserVclock},
            Next(RD, NewCtx);
        {error, no_user_key} ->
            %% no keyid was given, proceed anonymously
            Next(RD, Ctx);
        {error, bad_auth} ->
            %% given keyid was found, but signature didn't match
            deny_access(RD, Ctx);
        {error, _Reason} ->
            %% no matching keyid was found, or lookup failed
            deny_invalid_key(RD, Ctx)
    end.

%% @doc Look for an Authorization header in the request, and validate
%% it if it exists.  Returns `{ok, User, UserVclock}' if validation
%% succeeds, or `{error, KeyId, Reason}' if any step fails.
validate_auth_header(RD, AuthBypass, RiakPid) ->
    AuthHeader = wrq:get_req_header("authorization", RD),
    {AuthMod, KeyId, Signature} = parse_auth_header(AuthHeader, AuthBypass),
    case riak_moss_utils:get_user(KeyId, RiakPid) of
        {ok, {User, UserVclock}} ->
            Secret = User?MOSS_USER.key_secret,
            case AuthMod:authenticate(RD, Secret, Signature) of
                ok ->
                    {ok, User, UserVclock};
                {error, _Reason} ->
                    %% TODO: are the errors here of small enough
                    %% number that we could just handle them in
                    %% forbidden/2?
                    {error, bad_auth}
            end;
        {error, NE} when NE == notfound; NE == no_user_key ->
            %% anonymous access lookups don't need to be logged, and
            %% auth failures are logged by other meands
            {error, NE};
        {error, Reason} ->
            %% other failures, like Riak fetch timeout, be loud about
            lager:error("Retrieval of user record for ~p failed. Reason: ~p",
                        [KeyId, Reason]),
            {error, Reason}
    end.

%% @doc Produce an access-denied error message from a webmachine
%% resource's `forbidden/2' function.
deny_access(RD, Ctx) ->
    riak_moss_s3_response:api_error(access_denied, RD, Ctx).

%% @doc Prodice an invalid-access-keyid error message from a
%% webmachine resource's `forbidden/2' function.
deny_invalid_key(RD, Ctx) ->
    riak_moss_s3_response:api_error(invalid_access_key_id, RD, Ctx).

%% @doc Utility function for accessing
%%      a riakc_obj without retrieving
%%      it again if it's already in the
%%      Ctx
-spec ensure_doc(term()) -> term().
ensure_doc(Ctx=#key_context{get_fsm_pid=undefined,
                            bucket=Bucket,
                            key=Key,
                            context=InnerCtx}) ->
    RiakPid = InnerCtx#context.riakc_pid,
    %% start the get_fsm
    BinKey = list_to_binary(Key),
    {ok, Pid} = riak_moss_get_fsm_sup:start_get_fsm(node(), [Bucket, BinKey, self(), RiakPid]),
    Manifest = riak_moss_get_fsm:get_manifest(Pid),
    Ctx#key_context{get_fsm_pid=Pid, manifest=Manifest};
ensure_doc(Ctx) -> Ctx.

streaming_get(FsmPid) ->
    case riak_moss_get_fsm:get_next_chunk(FsmPid) of
        {done, Chunk} ->
            {Chunk, done};
        {chunk, Chunk} ->
            {Chunk, fun() -> streaming_get(FsmPid) end}
    end.

%% @doc Convert a moss_user record
%%      into a property list, likely
%%      for json encoding
-spec user_record_to_proplist(term()) -> list().
user_record_to_proplist(?MOSS_USER{email=Email,
                                   display_name=DisplayName,
                                   name=Name,
                                   key_id=KeyID,
                                   key_secret=KeySecret,
                                   canonical_id=CanonicalID}) ->
    [{<<"email">>, list_to_binary(Email)},
     {<<"display_name">>, list_to_binary(DisplayName)},
     {<<"name">>, list_to_binary(Name)},
     {<<"key_id">>, list_to_binary(KeyID)},
     {<<"key_secret">>, list_to_binary(KeySecret)},
     {<<"id">>, list_to_binary(CanonicalID)}];
user_record_to_proplist(#moss_user{name=Name,
                                   key_id=KeyID,
                                   key_secret=KeySecret,
                                   buckets = Buckets}) ->
    [{<<"name">>, list_to_binary(Name)},
     {<<"key_id">>, list_to_binary(KeyID)},
     {<<"key_secret">>, list_to_binary(KeySecret)},
     {<<"buckets">>, Buckets}].

%% @doc Get an ISO 8601 formatted timestamp representing
%% current time.
-spec iso_8601_datetime() -> string().
iso_8601_datetime() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = erlang:universaltime(),
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

%% @doc Convert an ISO 8601 date to RFC 1123 date
-spec iso_8601_to_rfc_1123(binary() | string()) -> string().
iso_8601_to_rfc_1123(Date) when is_list(Date) ->
    iso_8601_to_rfc_1123(iolist_to_binary(Date));
iso_8601_to_rfc_1123(Date) when is_binary(Date) ->
    %% e.g. "2012-02-17T18:22:50.000Z"
    <<Yr:4/binary, _:1/binary, Mo:2/binary, _:1/binary, Da:2/binary,
      _T:1/binary,
      Hr:2/binary, _:1/binary, Mn:2/binary, _:1/binary, Sc:2/binary,
      _/binary>> = Date,
    httpd_util:rfc1123_date({{b2i(Yr), b2i(Mo), b2i(Da)},
                             {b2i(Hr), b2i(Mn), b2i(Sc)}}).

%% ===================================================================
%% Internal functions
%% ===================================================================

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
