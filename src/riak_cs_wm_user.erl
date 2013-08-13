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

-module(riak_cs_wm_user).

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

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(Config) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = not proplists:get_value(admin_auth_enabled, Config),
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    {ok, #context{auth_bypass=AuthBypass,
                  api=Api,
                  response_module=RespModule}}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"service_available">>),
    riak_cs_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"allowed_methods">>),
    {['GET', 'HEAD', 'POST', 'PUT'], RD, Ctx}.

forbidden(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"forbidden">>),
    Method = wrq:method(RD),
    AnonOk = ((Method =:= 'PUT' orelse Method =:= 'POST') andalso
              riak_cs_config:anonymous_user_creation())
        orelse AuthBypass,
    Next = fun(NewRD, NewCtx=#context{user=User}) ->
                   forbidden(wrq:method(RD),
                             NewRD,
                             NewCtx,
                             User,
                             user_key(RD),
                             AnonOk)
           end,
    UserAuthResponse = riak_cs_wm_utils:find_and_auth_user(RD, Ctx, Next),
    handle_user_auth_response(UserAuthResponse).

handle_user_auth_response({false, _RD, Ctx} = Ret) ->
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>,
                                [], [riak_cs_wm_utils:extract_name(Ctx#context.user), <<"false">>]),
    Ret;
handle_user_auth_response({{halt, Code}, _RD, Ctx} = Ret) ->
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>,
                                [Code], [riak_cs_wm_utils:extract_name(Ctx#context.user), <<"true">>]),
    Ret;
handle_user_auth_response({_Reason, _RD, Ctx} = Ret) ->
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>,
                                [-1], [riak_cs_wm_utils:extract_name(Ctx#context.user), <<"true">>]),
    Ret.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"content_types_accepted">>),
    {[{?XML_TYPE, accept_xml}, {?JSON_TYPE, accept_json}], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"content_types_provided">>),
    {[{?XML_TYPE, produce_xml}, {?JSON_TYPE, produce_json}], RD, Ctx}.

post_is_create(RD, Ctx) -> {true, RD, Ctx}.

create_path(RD, Ctx) -> {"/riak-cs/user", RD, Ctx}.

-spec accept_json(#wm_reqdata{}, #context{}) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_json(RD, Ctx=#context{user=undefined}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"accept_json">>),
    Body = riak_cs_json:from_json(wrq:req_body(RD)),
    {UserName, Email} =
        riak_cs_json:value_or_default(
          riak_cs_json:get(Body, [{<<"name">>, <<"email">>}]),
          {<<>>, <<>>}),
    user_response(riak_cs_utils:create_user(binary_to_list(UserName),
                                                binary_to_list(Email)),
                      ?JSON_TYPE,
                      RD,
                      Ctx);
accept_json(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"accept_json">>),
    Body = wrq:req_body(RD),
    case catch mochijson2:decode(Body) of
        {struct, UserItems} ->
            UpdateItems = lists:foldl(fun user_json_filter/2, [], UserItems),
            user_response(update_user(UpdateItems, RD, Ctx),
                          ?JSON_TYPE,
                          RD,
                          Ctx);
        {'EXIT', _} ->
            riak_cs_s3_response:api_error(invalid_user_update, RD, Ctx)
    end.

-spec accept_xml(term(), term()) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_xml(RD, Ctx=#context{user=undefined}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"accept_xml">>),
    Body = binary_to_list(wrq:req_body(RD)),
    case catch xmerl_scan:string(Body, []) of
        {'EXIT', _} ->
            riak_cs_s3_response:api_error(invalid_user_update, RD, Ctx);
        {ParsedData, _Rest} ->
            ValidItems = lists:foldl(fun user_xml_filter/2,
                                     [],
                                     ParsedData#xmlElement.content),
            UserName = proplists:get_value(name, ValidItems, ""),
            Email= proplists:get_value(email, ValidItems, ""),
            user_response(riak_cs_utils:create_user(UserName, Email),
                          ?XML_TYPE,
                          RD,
                          Ctx)
    end;
accept_xml(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"accept_xml">>),
    Body = binary_to_list(wrq:req_body(RD)),
    case catch xmerl_scan:string(Body, []) of
        {'EXIT', _} ->
            riak_cs_s3_response:api_error(invalid_user_update, RD, Ctx);
        {ParsedData, _Rest} ->
            UpdateItems = lists:foldl(fun user_xml_filter/2,
                                      [],
                                      ParsedData#xmlElement.content),
            user_response(update_user(UpdateItems, RD, Ctx),
                          ?XML_TYPE,
                          RD,
                          Ctx)

    end.

produce_json(RD, #context{user=User}=Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"produce_json">>),
    Body = riak_cs_json:to_json(User),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

produce_xml(RD, #context{user=User}=Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"produce_xml">>),
    Body = riak_cs_xml:to_xml(User),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [1], []),
    riak_cs_utils:close_riak_connection(RiakPid),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finish_request">>, [1], []),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

-spec admin_check(boolean(), term(), term()) -> {boolean(), term(), term()}.
admin_check(true, RD, Ctx) ->
    {false, RD, Ctx#context{user=undefined}};
admin_check(false, RD, Ctx) ->
    riak_cs_wm_utils:deny_access(RD, Ctx).

%% @doc Calculate the etag of a response body
etag(Body) ->
        riak_cs_utils:etag_from_binary(riak_cs_utils:md5(Body)).

-spec forbidden(atom(),
                term(),
                term(),
                undefined | rcs_user(),
                string(),
                boolean()) ->
                       {boolean() | {halt, term()}, term(), term()}.
forbidden(_Method, RD, Ctx, undefined, _UserPathKey, false) ->
    %% anonymous access disallowed
    riak_cs_wm_utils:deny_access(RD, Ctx);
forbidden(_, _RD, _Ctx, undefined, [], true) ->
    {false, _RD, _Ctx};
forbidden(_, RD, Ctx, undefined, UserPathKey, true) ->
    get_user({false, RD, Ctx}, UserPathKey);
forbidden('POST', RD, Ctx, User, [], _) ->
    %% Admin is creating a new user
    admin_check(riak_cs_utils:is_admin(User), RD, Ctx);
forbidden('PUT', RD, Ctx, User, [], _) ->
    admin_check(riak_cs_utils:is_admin(User), RD, Ctx);
forbidden(_Method, RD, Ctx, User, UserPathKey, _) when
      UserPathKey =:= User?RCS_USER.key_id;
      UserPathKey =:= [] ->
    %% User is accessing own account
    AccessRD = riak_cs_access_log_handler:set_user(User, RD),
    {false, AccessRD, Ctx};
forbidden(_Method, RD, Ctx, User, UserPathKey, _) ->
    AdminCheckResult = admin_check(riak_cs_utils:is_admin(User), RD, Ctx),
    get_user(AdminCheckResult, UserPathKey).

-spec get_user({boolean() | {halt, term()}, term(), term()}, string()) ->
                      {boolean() | {halt, term()}, term(), term()}.
get_user({false, RD, Ctx}, UserPathKey) ->
    handle_get_user_result(
      riak_cs_utils:get_user(UserPathKey, Ctx#context.riakc_pid),
      RD,
      Ctx);
get_user(AdminCheckResult, _) ->
    AdminCheckResult.

-spec handle_get_user_result({ok, {rcs_user(), term()}} | {error, term()},
                             term(),
                             term()) ->
                                    {boolean() | {halt, term()}, term(), term()}.

handle_get_user_result({ok, {User, UserObj}}, RD, Ctx) ->
    {false, RD, Ctx#context{user=User, user_object=UserObj}};
handle_get_user_result({error, Reason}, RD, Ctx) ->
    _ = lager:warning("Failed to fetch user record. KeyId: ~p"
                      " Reason: ~p", [user_key(RD), Reason]),
    riak_cs_s3_response:api_error(invalid_access_key_id, RD, Ctx).

-spec update_user([{atom(), term()}], #wm_reqdata{}, #context{}) ->
    {ok, rcs_user()} | {halt, term()} | {error, term()}.
update_user(UpdateItems, RD, Ctx=#context{user=User}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"update_user">>),
    UpdateUserResult = update_user_record(User, UpdateItems, false),
    handle_update_result(UpdateUserResult, RD, Ctx).

-spec update_user_record('undefined' | rcs_user(), [{atom(), term()}], boolean())
                        -> {boolean(), rcs_user()}.
update_user_record(_User, [], RecordUpdated) ->
    {RecordUpdated, _User};
update_user_record(User=?RCS_USER{status=Status},
                   [{status, Status} | RestUpdates],
                   _RecordUpdated) ->
    update_user_record(User, RestUpdates, _RecordUpdated);
update_user_record(User, [{status, Status} | RestUpdates], _RecordUpdated) ->
    update_user_record(User?RCS_USER{status=Status}, RestUpdates, true);
update_user_record(User, [{name, Name} | RestUpdates], _RecordUpdated) ->
    update_user_record(User?RCS_USER{name=Name}, RestUpdates, true);
update_user_record(User, [{email, Email} | RestUpdates], _RecordUpdated) ->
    DisplayName = riak_cs_utils:display_name(Email),
    update_user_record(User?RCS_USER{email=Email, display_name=DisplayName},
                       RestUpdates,
                       true);
update_user_record(User=?RCS_USER{}, [{new_key_secret, true} | RestUpdates], _) ->
    update_user_record(riak_cs_utils:update_key_secret(User), RestUpdates, true);
update_user_record(_User, [_ | RestUpdates], _RecordUpdated) ->
    update_user_record(_User, RestUpdates, _RecordUpdated).

-spec handle_update_result({boolean(), rcs_user()}, term(), term()) ->
    {ok, rcs_user()} | {halt, term()} | {error, term()}.
handle_update_result({false, _User}, _RD, _Ctx) ->
    {halt, 200};
handle_update_result({true, User}, _RD, Ctx) ->
    #context{user_object=UserObj,
             riakc_pid=RiakPid} = Ctx,
    riak_cs_utils:update_user(User, UserObj, RiakPid).

-spec set_resp_data(string(), term(), term()) -> term().
set_resp_data(ContentType, RD, #context{user=User}) ->
    UserDoc = format_user_record(User, ContentType),
    wrq:set_resp_body(UserDoc, RD).

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
        <<"new_key_secret">> ->
            [{new_key_secret, ItemValue} | Acc];
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
        'NewKeySecret' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    case Content#xmlText.value of
                        "true" ->
                            [{new_key_secret, true} | Acc];
                        "false" ->
                            [{new_key_secret, false} | Acc];
                        _ ->
                            Acc
                    end;
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.

-spec user_response({ok, rcs_user()} | {halt, term()} | {error, term()},
                        string(), #wm_reqdata{}, #context{}) ->
                               {true | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
user_response({ok, User}, ContentType, RD, Ctx) ->
    UserDoc = format_user_record(User, ContentType),
    WrittenRD =
        wrq:set_resp_body(UserDoc,
                          wrq:set_resp_header("Content-Type", ContentType, RD)),
    {true, WrittenRD, Ctx};
user_response({halt, 200}, ContentType, RD, Ctx) ->
    {{halt, 200}, set_resp_data(ContentType, RD, Ctx), Ctx};
user_response({error, Reason}, _, RD, Ctx) ->
    riak_cs_s3_response:api_error(Reason, RD, Ctx).

-spec format_user_record(rcs_user(), string()) -> binary().
format_user_record(User, ?JSON_TYPE) ->
    riak_cs_json:to_json(User);
format_user_record(User, ?XML_TYPE) ->
    riak_cs_xml:to_xml(User).
