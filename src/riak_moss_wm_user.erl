%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_user).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         allowed_methods/2,
         produce_json/2,
         produce_xml/2,
         process_post/2,
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
    {['GET', 'HEAD', 'POST'], RD, Ctx}.

forbidden(RD, Ctx) ->
    case wrq:method(RD) of
        'POST' ->
            {false, RD, Ctx};
        _ ->
            Next = fun(NewRD, NewCtx=#context{user=User}) ->
                           AccessRD = riak_moss_access_logger:set_user(User, NewRD),
                           forbidden(AccessRD, NewCtx, User)
                   end,
            riak_moss_wm_utils:find_and_auth_user(RD, Ctx, Next)
    end.

content_types_provided(RD, Ctx) ->
    {[{?XML_TYPE, produce_xml}, {?JSON_TYPE, produce_json}], RD, Ctx}.

produce_json(RD, #context{user=User}=Ctx) ->
    MJ = {struct, riak_moss_wm_utils:user_record_to_proplist(User)},
    Body = mochijson2:encode(MJ),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

produce_xml(RD, #context{user=User}=Ctx) ->
    XmlUserRec =
        [{Key, [binary_to_list(Value)]} ||
            {Key, Value} <- riak_moss_wm_utils:user_record_to_proplist(User)],
    Doc = [{'User', XmlUserRec}],
    Body = riak_moss_s3_response:export_xml(Doc),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

%% @doc Create a user from a POST.
%%      and return the user object
%%      as JSON
-spec process_post(term(), term()) -> {true, term(), term}.
process_post(RD, Ctx) ->
    Body = wrq:req_body(RD),
    ParsedBody = mochiweb_util:parse_qs(binary_to_list(Body)),
    UserName = proplists:get_value("name", ParsedBody, ""),
    Email= proplists:get_value("email", ParsedBody, ""),
    case riak_moss_utils:create_user(UserName, Email) of
        {ok, UserRecord} ->
            PropListUser = riak_moss_wm_utils:user_record_to_proplist(UserRecord),
            CTypeWritten = wrq:set_resp_header("Content-Type", ?JSON_TYPE, RD),
            WrittenRD = wrq:set_resp_body(list_to_binary(
                                            mochijson2:encode(PropListUser)),
                                          CTypeWritten),
            {true, WrittenRD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    riak_moss_utils:close_riak_connection(RiakPid),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

%% @doc Calculate the etag of a response body
etag(Body) ->
    webmachine_util:quoted_string(
      riak_moss_utils:binary_to_hexlist(
        crypto:md5(Body))).

forbidden(RD, Ctx, undefined) ->
    %% anonymous access disallowed
    riak_moss_wm_utils:deny_access(RD, Ctx);
forbidden(RD, Ctx, User) ->
    UserKeyId = User?MOSS_USER.key_id,
    UserPathKey = user_key(RD),
    case UserPathKey of
        [] ->
            %% user is accessing own account
            %% @TODO Determine if logging this is appropriate
            %% and if we need to classify it differently.
            AccessRD = riak_moss_access_logger:set_user(User, RD),
            {false, AccessRD, Ctx};
         UserKeyId ->
            %% user is accessing own account
            %% @TODO Determine if logging this is appropriate
            %% and if we need to classify it differently.
            AccessRD = riak_moss_access_logger:set_user(User, RD),
            {false, AccessRD, Ctx};
        _ ->
            case riak_moss_utils:get_admin_creds() of
                {ok, {Admin, _}} when Admin == UserKeyId ->
                    %% admin can access any account
                    case riak_moss_utils:get_user(UserPathKey, Ctx#context.riakc_pid) of
                        {ok, {ReqUser, _}} ->
                            {false, RD, Ctx#context{user=ReqUser}};
                        {error, Reason} ->
                            _ = lager:warning("Failed to fetch user record. KeyId: ~p"
                                          " Reason: ~p", [UserPathKey, Reason]),
                            riak_moss_s3_response:api_error(invalid_access_key_id, RD, Ctx)
                    end;
                _ ->
                    %% no one else is allowed
                    riak_moss_wm_utils:deny_access(RD, Ctx)
            end
    end.

user_key(RD) ->
    case wrq:path_tokens(RD) of
        [KeyId|_] -> mochiweb_util:unquote(KeyId);
        _         -> []
    end.
