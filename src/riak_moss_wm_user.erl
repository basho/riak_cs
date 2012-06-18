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
         content_types_accepted/2,
         accept_json/2,
         accept_xml/2,
         allowed_methods/2,
         post_is_create/2,
         create_path/2,
         produce_json/2,
         produce_xml/2,
         finish_request/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(Config) ->
    dt_entry(<<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #context{auth_bypass=AuthBypass}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    dt_entry(<<"service_available">>),
    riak_moss_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    dt_entry(<<"allowed_methods">>),
    {['GET', 'HEAD', 'POST', 'PUT'], RD, Ctx}.

forbidden(RD, Ctx) ->
    dt_entry(<<"forbidden">>),
    Next = fun(NewRD, NewCtx=#context{user=User}) ->
                   forbidden(wrq:method(RD),
                             NewRD,
                             NewCtx,
                             User,
                             user_key(RD),
                             riak_moss_utils:anonymous_user_creation())
           end,
    UserAuthResponse = riak_moss_wm_utils:find_and_auth_user(RD, Ctx, Next),
    handle_user_auth_response(UserAuthResponse).

handle_user_auth_response({false, _RD, Ctx} = Ret) ->
    dt_return(<<"forbidden">>, [], [extract_name(Ctx#context.user), <<"false">>]),
    Ret;
handle_user_auth_response({{halt, Code}, _RD, Ctx} = Ret) ->
    dt_return(<<"forbidden">>, [Code], [extract_name(Ctx#context.user), <<"true">>]),
    Ret;
handle_user_auth_response({_Reason, _RD, Ctx} = Ret) ->
    dt_return(<<"forbidden">>, [-1], [extract_name(Ctx#context.user), <<"true">>]),
    Ret.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    dt_entry(<<"content_types_accepted">>),
    {[{?XML_TYPE, accept_xml}, {?JSON_TYPE, accept_json}], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    dt_entry(<<"content_types_provided">>),
    {[{?XML_TYPE, produce_xml}, {?JSON_TYPE, produce_json}], RD, Ctx}.

post_is_create(RD, Ctx) -> {true, RD, Ctx}.

create_path(RD, Ctx) -> {"/riak-cs/user", RD, Ctx}.

-spec accept_json(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_json(RD, Ctx=#context{user=undefined}) ->
    dt_entry(<<"accept_json">>),
    Body = wrq:req_body(RD),
    case catch mochijson2:decode(Body) of
        {struct, UserItems} ->
            ValidItems = lists:foldl(fun user_json_filter/2, [], UserItems),
            UserName = proplists:get_value(name, ValidItems, ""),
            Email= proplists:get_value(email, ValidItems, ""),
            case riak_moss_utils:create_user(UserName, Email) of
                {ok, User} ->
                    CTypeWritten = wrq:set_resp_header("Content-Type", ?JSON_TYPE, RD),
                    UserData = riak_moss_wm_utils:user_record_to_json(User),
                    JsonDoc = list_to_binary(mochijson2:encode(UserData)),
                    WrittenRD = wrq:set_resp_body(JsonDoc, CTypeWritten),
                    {true, WrittenRD, Ctx};
                {error, Reason} ->
                    riak_moss_s3_response:api_error(Reason, RD, Ctx)
            end;
        {'EXIT', _} ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx)
    end;
accept_json(RD, Ctx) ->
    dt_entry(<<"accept_json">>),
    Body = wrq:req_body(RD),
    case catch mochijson2:decode(Body) of
        {struct, UserItems} ->
            UpdateItems = lists:foldl(fun user_json_filter/2, [], UserItems),
            update_user(UpdateItems, RD, Ctx);
        {'EXIT', _} ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx)
    end.

-spec accept_xml(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_xml(RD, Ctx=#context{user=undefined}) ->
    dt_entry(<<"accept_xml">>),
    Body = binary_to_list(wrq:req_body(RD)),
    case catch xmerl_scan:string(Body, []) of
        {'EXIT', _} ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx);
        {ParsedData, _Rest} ->
            ValidItems = lists:foldl(fun user_xml_filter/2,
                                     [],
                                     ParsedData#xmlElement.content),
            UserName = proplists:get_value(name, ValidItems, ""),
            Email= proplists:get_value(email, ValidItems, ""),
            case riak_moss_utils:create_user(UserName, Email) of
                {ok, User} ->
                    CTypeWritten = wrq:set_resp_header("Content-Type", ?XML_TYPE, RD),
                    UserData = riak_moss_wm_utils:user_record_to_xml(User),
                    XmlDoc = riak_moss_s3_response:export_xml([UserData]),
                    WrittenRD = wrq:set_resp_body(XmlDoc, CTypeWritten),
                    {true, WrittenRD, Ctx};
                {error, Reason} ->
                    riak_moss_s3_response:api_error(Reason, RD, Ctx)
            end
    end;
accept_xml(RD, Ctx) ->
    dt_entry(<<"accept_xml">>),
    Body = binary_to_list(wrq:req_body(RD)),
    case catch xmerl_scan:string(Body, []) of
        {'EXIT', _} ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx);
        {ParsedData, _Rest} ->
            UpdateItems = lists:foldl(fun user_xml_filter/2,
                                      [],
                                      ParsedData#xmlElement.content),
            update_user(UpdateItems, RD, Ctx)
    end.

produce_json(RD, #context{user=User}=Ctx) ->
    dt_entry(<<"produce_json">>),
    Body = mochijson2:encode(
             riak_moss_wm_utils:user_record_to_json(User)),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

produce_xml(RD, #context{user=User}=Ctx) ->
    dt_entry(<<"produce_xml">>),
    XmlDoc = riak_moss_wm_utils:user_record_to_xml(User),
    Body = riak_moss_s3_response:export_xml([XmlDoc]),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    dt_entry(<<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    dt_entry(<<"finish_request">>, [1], []),
    riak_moss_utils:close_riak_connection(RiakPid),
    dt_return(<<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

-spec admin_check(boolean(), term(), term()) -> {boolean(), term(), term()}.
admin_check(true, RD, Ctx) ->
    {false, RD, Ctx#context{user=undefined}};
admin_check(false, RD, Ctx) ->
    riak_moss_wm_utils:deny_access(RD, Ctx).

%% @doc Calculate the etag of a response body
etag(Body) ->
    webmachine_util:quoted_string(
      riak_moss_utils:binary_to_hexlist(
        crypto:md5(Body))).

-spec forbidden(atom(),
                term(),
                term(),
                undefined | rcs_user(),
                string(),
                boolean()) ->
                       {boolean() | {halt, term()}, term(), term()}.
forbidden(_Method, RD, Ctx, undefined, _UserPathKey, false) ->
    %% anonymous access disallowed
    riak_moss_wm_utils:deny_access(RD, Ctx);
forbidden('POST', _RD, _Ctx, undefined, [], true) ->
    {false, _RD, _Ctx};
forbidden('PUT', _RD, _Ctx, undefined, [], true) ->
    {false, _RD, _Ctx};
forbidden('POST', RD, Ctx, User, [], true) ->
    %% Admin is creating a new user
    %% @TODO Replace the call to `is_admin' with a call to
    %% `riak_moss_utils:is_admin' before merging to master.
    admin_check(is_admin(User), RD, Ctx);
forbidden('PUT', RD, Ctx, User, [], true) ->
    admin_check(riak_moss_utils:is_admin(User), RD, Ctx);
forbidden(_Method, RD, Ctx, User, UserPathKey, true) when
      UserPathKey =:= User?RCS_USER.key_id ->
    %% User is accessing own account
    AccessRD = riak_moss_access_logger:set_user(User, RD),
    {false, AccessRD, Ctx};
forbidden(_Method, RD, Ctx, User, UserPathKey, true) ->
    %% @TODO Replace the call to `is_admin' with a call to
    %% `riak_moss_utils:is_admin' before merging to master.
    AdminCheckResult = admin_check(is_admin(User), RD, Ctx),
    get_user(AdminCheckResult, UserPathKey).

-spec get_user({boolean() | {halt, term()}, term(), term()}, string()) ->
                      {boolean() | {halt, term()}, term(), term()}.
get_user({false, RD, Ctx}, UserPathKey) ->
    handle_get_user_result(
      riak_moss_utils:get_user(UserPathKey, Ctx#context.riakc_pid),
      RD,
      Ctx);
get_user(AdminCheckResult, _) ->
    AdminCheckResult.

-spec handle_get_user_result({ok, {rcs_user(), term()}} | {error, term()},
                             term(),
                             term()) ->
                                    {boolean() | {halt, term()}, term(), term()}.

handle_get_user_result({ok, {User, _}}, RD, Ctx) ->
    {false, RD, Ctx#context{user=User}};
handle_get_user_result({error, Reason}, RD, Ctx) ->
    _ = lager:warning("Failed to fetch user record. KeyId: ~p"
                      " Reason: ~p", [user_key(RD), Reason]),
    riak_moss_s3_response:api_error(invalid_access_key_id, RD, Ctx).

%% @TODO Already add `riak_moss_utils:is_admin' in a later branch
%% so this will be removed in favor of that before merge to master.
is_admin(User) ->
    case riak_moss_utils:get_admin_creds() of
        {ok, {Admin, _}} when Admin == User?RCS_USER.key_id ->
            true;
        _ ->
            false
    end.

-spec update_user([{binary(), binary()}], term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
update_user(UpdateItems, RD, Ctx=#context{user=User,
                                          user_vclock=VClock,
                                          riakc_pid=RiakPid}) ->
    dt_entry(<<"update_user">>),
    case lists:keyfind(status, 1, UpdateItems) of
        {status, Status} ->
            case Status =:= User?MOSS_USER.status of
                true ->
                    {{halt, 200}, RD, Ctx};
                false ->
                    case riak_moss_utils:save_user(User?MOSS_USER{status=Status},
                                                   VClock,
                                                   RiakPid) of
                        ok ->
                            {{halt, 200}, RD, Ctx};
                        {error, Reason} ->
                            riak_moss_s3_response:api_error(Reason, RD, Ctx)
                    end
            end;
        false ->
            riak_moss_s3_response:api_error(invalid_user_update, RD, Ctx)
    end.

-spec user_json_filter({binary(), binary()}, [{atom(), term()}]) -> [{atom(), term()}].
user_json_filter({ItemKey, ItemValue}, Acc) ->
    case ItemKey of
        <<"email">> ->
            [{email, binary_to_list(ItemValue)} | Acc];
        <<"name">> ->
            [{name, binary_to_list(ItemValue)} | Acc];
        <<"status">> ->
            case ItemValue of
                <<"enabled">> ->
                    [{status, enabled} | Acc];
                <<"disabled">> ->
                    [{status, disabled} | Acc];
                _ ->
                    Acc
            end;
        _ ->
            Acc
    end.

user_key(RD) ->
    case wrq:path_tokens(RD) of
        [KeyId|_] -> mochiweb_util:unquote(KeyId);
        _         -> []
    end.

-spec user_xml_filter(#xmlText{} | #xmlElement{}, [{atom(), term()}]) -> [{atom(), term()}].
user_xml_filter(#xmlText{}, Acc) ->
    Acc;
user_xml_filter(Element, Acc) ->
    case Element#xmlElement.name of
        'Email' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    [{email, Content#xmlText.value} | Acc];
                false ->
                    Acc
            end;
        'Name' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    [{name, Content#xmlText.value} | Acc];
                false ->
                    Acc
            end;
        'Status' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    case Content#xmlText.value of
                        "enabled" ->
                            [{status, enabled} | Acc];
                        "disabled" ->
                            [{status, disabled} | Acc];
                        _ ->
                            Acc
                    end;
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.

extract_name(X) ->
    riak_moss_wm_utils:extract_name(X).

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).
