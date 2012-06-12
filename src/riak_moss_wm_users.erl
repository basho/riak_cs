%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_users).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         allowed_methods/2,
         produce_json/2,
         produce_xml/2,
         finish_request/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['GET', 'HEAD'], RD, Ctx}.

forbidden(RD, Ctx) ->
    Next = fun(NewRD, NewCtx=#context{user=User}) ->
                   forbidden(NewRD, NewCtx, User)
           end,
    riak_moss_wm_utils:find_and_auth_user(RD, Ctx, Next).

content_types_provided(RD, Ctx) ->
    {[{?XML_TYPE, produce_xml}, {?JSON_TYPE, produce_json}], RD, Ctx}.

produce_json(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    Boundary = unique_id(),
    UpdRD = wrq:set_resp_header("Content-Type",
                                "multipart/mixed; boundary="++Boundary,
                                RD),
    StatusQsVal = wrq:get_qs_value("status", RD),
    case StatusQsVal of
        "enabled" ->
            Status = enabled;
        "disabled" ->
            Status = disabled;
        _ ->
            Status = undefined
    end,
    {{stream, {<<>>, fun() -> stream_users(json, RiakPid, Boundary, Status) end}}, UpdRD, Ctx}.

produce_xml(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    Boundary = unique_id(),
    UpdRD = wrq:set_resp_header("Content-Type",
                                "multipart/mixed; boundary="++Boundary,
                                RD),
    StatusQsVal = wrq:get_qs_value("status", RD),
    case StatusQsVal of
        "enabled" ->
            Status = enabled;
        "disabled" ->
            Status = disabled;
        _ ->
            Status = undefined
    end,
    {{stream, {<<>>, fun() -> stream_users(xml, RiakPid, Boundary, Status) end}}, UpdRD, Ctx}.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    riak_moss_utils:close_riak_connection(RiakPid),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

forbidden(RD, Ctx, undefined) ->
    %% anonymous access disallowed
    riak_moss_wm_utils:deny_access(RD, Ctx);
forbidden(RD, Ctx, User) ->
    UserKeyId = User?MOSS_USER.key_id,
    case riak_moss_utils:get_admin_creds() of
        {ok, {Admin, _}} when Admin == UserKeyId ->
            %% admin account is allowed
            {false, RD, Ctx};
        _ ->
            %% no one else is allowed
            riak_moss_wm_utils:deny_access(RD, Ctx)
    end.

stream_users(Format, RiakPid, Boundary, Status) ->
    case riakc_pb_socket:stream_list_keys(RiakPid, ?USER_BUCKET) of
        {ok, ReqId} ->
            case riak_moss_utils:riak_connection() of
                {ok, RiakPid2} ->
                    Res = wait_for_users(Format, RiakPid2, ReqId, Boundary, Status),
                    riak_moss_utils:close_riak_connection(RiakPid2),
                    Res;
                {error, _Reason} ->
                    wait_for_users(Format, RiakPid, ReqId, Boundary, Status)
            end;
        {error, _Reason} ->
            {<<>>, done}
    end.

wait_for_users(Format, RiakPid, ReqId, Boundary, Status) ->
    receive
        {ReqId, {keys, UserIds}} ->
            UserDocs = lists:flatten([user_doc(Format, RiakPid, binary_to_list(UserId), Status) ||
                           UserId <- UserIds]),
            Doc = users_doc(UserDocs, Format, Boundary),
            {Doc, fun() -> wait_for_users(Format, RiakPid, ReqId, Boundary, Status) end};
        {ReqId, done} ->
            {list_to_binary(["\r\n--", Boundary, "--"]), done};
        _ ->
            wait_for_users(Format, RiakPid, ReqId, Boundary, Status)
    end.

%% @doc Compile a multipart entity for a set of user documents.
users_doc(UserDocs, xml, Boundary) ->
    XmlDoc = riak_moss_s3_response:export_xml([{'Users', UserDocs}]),
    ["\r\n--",
     Boundary,
     "\r\nContent-Type: ", ?XML_TYPE, "\r\n\r\n",
     XmlDoc];
users_doc(UserDocs, json, Boundary) ->
    ["\r\n--",
     Boundary,
     "\r\nContent-Type: ", ?JSON_TYPE, "\r\n\r\n",
     mochijson2:encode(UserDocs)].

%% @doc Generate xml or json terms representing
%% the information for a given user.
user_doc(Format, RiakPid, UserId, Status) ->
    case riak_moss_utils:get_user(UserId, RiakPid) of
        {ok, {User, _}} when User?MOSS_USER.status =:= Status;
                             Status =:= undefined ->
            case Format of
                xml ->
                    riak_moss_wm_utils:user_record_to_xml(User);
                json ->
                    riak_moss_wm_utils:user_record_to_json(User)
            end;
        {ok, _} ->
            %% Status is defined and does not match the account status
            [];
        {error, Reason} ->
            _ = lager:warning("Failed to fetch user record. KeyId: ~p"
                          " Reason: ~p", [UserId, Reason]),
            []
    end.

unique_id() ->
    Rand = crypto:sha(term_to_binary({make_ref(), now()})),
    <<I:160/integer>> = Rand,
    integer_to_list(I, 36).
