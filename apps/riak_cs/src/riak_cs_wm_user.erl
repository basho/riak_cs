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

-module(riak_cs_wm_user).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         content_types_accepted/2,
         accept_json/2,
         accept_xml/2,
         delete_resource/2,
         allowed_methods/2,
         post_is_create/2,
         create_path/2,
         produce_json/2,
         produce_xml/2,
         finish_request/2
        ]).

-ignore_xref([init/1,
              service_available/2,
              forbidden/2,
              content_types_provided/2,
              content_types_accepted/2,
              accept_json/2,
              accept_xml/2,
              delete_resource/2,
              allowed_methods/2,
              post_is_create/2,
              create_path/2,
              produce_json/2,
              produce_xml/2,
              finish_request/2
             ]).

-include("riak_cs.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = not proplists:get_value(admin_auth_enabled, Config),
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    {ok, #rcs_web_context{auth_bypass = AuthBypass,
                          api = Api,
                          response_module = RespModule}}.

-spec service_available(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
service_available(RD, Ctx) ->
    riak_cs_wm_utils:service_available(
      wrq:set_resp_headers(riak_cs_wm_utils:cors_headers(), RD), Ctx).

-spec allowed_methods(#wm_reqdata{}, #rcs_web_context{}) -> {[atom()], #wm_reqdata{}, #rcs_web_context{}}.
allowed_methods(RD, Ctx) ->
    {['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS'], RD, Ctx}.

-spec forbidden(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #rcs_web_context{}}.
forbidden(RD, Ctx) ->
    case wrq:method(RD) of
        'OPTIONS' ->
            {false, RD, Ctx};
        _ ->
            forbidden2(RD, Ctx)
    end.
forbidden2(RD, Ctx = #rcs_web_context{auth_bypass = AuthBypass}) ->
    Method = wrq:method(RD),
    AnonOk = ((Method =:= 'PUT' orelse Method =:= 'POST') andalso
              riak_cs_config:anonymous_user_creation())
        orelse AuthBypass,
    Next = fun(NewRD, NewCtx = #rcs_web_context{user = User}) ->
                   forbidden(wrq:method(RD),
                             NewRD,
                             NewCtx,
                             User,
                             user_key(RD),
                             AnonOk)
           end,
    riak_cs_wm_utils:find_and_auth_user(RD, Ctx, Next).

-spec content_types_accepted(#wm_reqdata{}, #rcs_web_context{}) ->
    {[{string(), atom()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_accepted(RD, Ctx) ->
    {[{?XML_TYPE, accept_xml}, {?JSON_TYPE, accept_json}], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    {[{?XML_TYPE, produce_xml}, {?JSON_TYPE, produce_json}], RD, Ctx}.

post_is_create(RD, Ctx) ->
    {true, RD, Ctx}.

create_path(RD, Ctx) ->
    {"/riak-cs/user", RD, Ctx}.

-spec accept_json(#wm_reqdata{}, #rcs_web_context{}) ->
    {boolean() | {halt, term()}, term(), term()}.
accept_json(RD, Ctx = #rcs_web_context{user = User}) ->
    FF = jsx:decode(wrq:req_body(RD), [{labels, atom}]),
    case check_required_fields(FF) of
        ok ->
            IAMExtra = #{permissions_boundary => maps:get(permissions_boundary, FF, undefined),
                         tags => maps:get(tags, FF, [])},
            Res =
                case wrq:method(RD) of
                    'POST' ->
                        riak_cs_user:create_user(maps:get(name, FF, <<>>),
                                                 maps:get(email, FF, <<>>),
                                                 IAMExtra);
                    'PUT' ->
                        update_user(maps:to_list(maps:merge(FF, IAMExtra)), User)
                end,
            user_response(Res, ?JSON_TYPE, RD, Ctx);
        ER ->
            user_response(ER, ?JSON_TYPE, RD, Ctx)
    end.
check_required_fields(#{email := Email,
                        name := Name}) when is_binary(Email),
                                            is_binary(Name) ->
    ok;
check_required_fields(_) ->
    {error, missing_parameter}.


-spec accept_xml(#wm_reqdata{}, #rcs_web_context{}) ->
    {boolean() | {halt, term()}, #wm_reqdata{}, #rcs_web_context{}}.
accept_xml(RD, Ctx = #rcs_web_context{user = User}) ->
    Body = binary_to_list(wrq:req_body(RD)),
    case riak_cs_xml:scan(Body) of
        {error, malformed_xml} ->
            riak_cs_aws_response:api_error(invalid_user_update, RD, Ctx);
        {ok, Xml} ->
            FF = lists:foldl(fun user_xml_filter/2, [], Xml#xmlElement.content),
            UserName = proplists:get_value(name, FF, ""),
            Email = proplists:get_value(email, FF, ""),
            Res =
                case wrq:method(RD) of
                    'POST' ->
                        riak_cs_user:create_user(UserName, Email);
                    'PUT' ->
                        update_user(maps:to_list(FF), User)
                end,
            user_response(Res, ?JSON_TYPE, RD, Ctx)
    end.

produce_json(RD, Ctx = #rcs_web_context{user = User}) ->
    Body = riak_cs_json:to_json(User),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.

produce_xml(RD, Ctx = #rcs_web_context{user = User}) ->
    Body = riak_cs_xml:to_xml(User),
    Etag = etag(Body),
    RD2 = wrq:set_resp_header("ETag", Etag, RD),
    {Body, RD2, Ctx}.


delete_resource(RD, Ctx = #rcs_web_context{user = User,
                                           response_module = ResponseMod}) ->
    case riak_cs_iam:delete_user(User) of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end.


-spec finish_request(#wm_reqdata{}, #rcs_web_context{}) ->
    {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
finish_request(RD, Ctx = #rcs_web_context{riak_client = undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx = #rcs_web_context{riak_client = RcPid}) ->
    riak_cs_riak_client:checkin(RcPid),
    {true, RD, Ctx#rcs_web_context{riak_client = undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

admin_check(true, RD, Ctx) ->
    {false, RD, Ctx};
admin_check(false, RD, Ctx) ->
    riak_cs_wm_utils:deny_access(RD, Ctx).

%% @doc Calculate the etag of a response body
etag(Body) ->
    riak_cs_utils:etag_from_binary(riak_cs_utils:md5(Body)).

forbidden(_Method, RD, Ctx, undefined, _UserPathKey, false) ->
    %% anonymous access disallowed
    riak_cs_wm_utils:deny_access(RD, Ctx);
forbidden(_, RD, Ctx, undefined, <<>>, true) ->
    {false, RD, Ctx};
forbidden(_, RD, Ctx, undefined, UserPathKey, true) ->
    get_user({false, RD, Ctx}, UserPathKey);
forbidden('POST', RD, Ctx, User, <<>>, _) ->
    %% Admin is creating a new user
    admin_check(riak_cs_user:is_admin(User), RD, Ctx);
forbidden('PUT', RD, Ctx, User, <<>>, _) ->
    admin_check(riak_cs_user:is_admin(User), RD, Ctx);
forbidden(_Method, RD, Ctx, User, UserPathKey, _) when
      UserPathKey =:= User?RCS_USER.key_id;
      UserPathKey =:= <<>> ->
    %% User is accessing own account
    AccessRD = riak_cs_access_log_handler:set_user(User, RD),
    {false, AccessRD, Ctx};
forbidden(_Method, RD, Ctx, User, UserPathKey, _) ->
    AdminCheckResult = admin_check(riak_cs_user:is_admin(User), RD, Ctx),
    get_user(AdminCheckResult, UserPathKey).

get_user({false, RD, Ctx}, UserPathKey) ->
    handle_get_user_result(
      riak_cs_user:get_user(UserPathKey, Ctx#rcs_web_context.riak_client),
      RD,
      Ctx);
get_user(AdminCheckResult, _) ->
    AdminCheckResult.

handle_get_user_result({ok, {User, UserObj}}, RD, Ctx) ->
    {false, RD, Ctx#rcs_web_context{user = User,
                                    user_object = UserObj}};
handle_get_user_result({error, Reason}, RD, Ctx) ->
    logger:warning("Failed to fetch user record. KeyId: ~s"
                   " Reason: ~p", [user_key(RD), Reason]),
    riak_cs_aws_response:api_error(invalid_access_key_id, RD, Ctx).

update_user(UpdateItems, User) ->
    UpdateUserResult = update_user_record(User, UpdateItems, false),
    handle_update_result(UpdateUserResult).

update_user_record(User, [], U1) ->
    {U1, User};
update_user_record(User, [{status, Status} | RestUpdates], _) ->
    update_user_record(User?RCS_USER{status = str_to_status(Status)}, RestUpdates, true);
update_user_record(User, [{name, Name} | RestUpdates], _) ->
    update_user_record(User?RCS_USER{name = Name}, RestUpdates, true);
update_user_record(User, [{email, Email} | RestUpdates], _) ->
    DisplayName = riak_cs_user:display_name(Email),
    update_user_record(User?RCS_USER{email = Email,
                                     display_name = DisplayName},
                       RestUpdates,
                       true);
update_user_record(User = ?RCS_USER{}, [{new_key_secret, true} | RestUpdates], _) ->
    update_user_record(riak_cs_user:update_key_secret(User), RestUpdates, true);
update_user_record(User, [_ | RestUpdates], U1) ->
    update_user_record(User, RestUpdates, U1).

str_to_status(<<"enabled">>) -> enabled;
str_to_status(<<"disabled">>) -> disabled.


handle_update_result({false, _User}) ->
    {halt, 200};
handle_update_result({true, User}) ->
    riak_cs_iam:update_user(User).

set_resp_data(ContentType, RD, #rcs_web_context{user = User}) ->
    UserDoc = format_user_record(User, ContentType),
    wrq:set_resp_body(UserDoc, RD).

user_key(RD) ->
    case wrq:path_tokens(RD) of
        [KeyId|_] ->
            list_to_binary(mochiweb_util:unquote(KeyId));
        _ -> <<>>
    end.

user_xml_filter(#xmlText{}, Acc) ->
    Acc;
user_xml_filter(Element, Acc) ->
    case Element#xmlElement.name of
        'Email' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                   Acc#{email => list_to_binary(Content#xmlText.value)};
                false ->
                    Acc
            end;
        'Name' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    Acc#{name => list_to_binary(Content#xmlText.value)};
                false ->
                    Acc
            end;
        'Path' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    Acc#{path => list_to_binary(Content#xmlText.value)};
                false ->
                    Acc
            end;
        'Status' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    Acc#{status => list_to_binary(Content#xmlText.value)};
                false ->
                    Acc
            end;
        'NewKeySecret' ->
            [Content | _] = Element#xmlElement.content,
            case is_record(Content, xmlText) of
                true ->
                    case Content#xmlText.value of
                        "true" ->
                            Acc#{new_key_secret => true};
                        "false" ->
                            Acc#{new_key_secret => false};
                        _ ->
                            Acc
                    end;
                false ->
                    Acc
            end;
        _ ->
            Acc
    end.

user_response({ok, User}, ContentType, RD, Ctx) ->
    UserDoc = format_user_record(User, ContentType),
    WrittenRD =
        wrq:set_resp_body(UserDoc,
                          wrq:set_resp_header("Content-Type", ContentType, RD)),
    {true, WrittenRD, Ctx};
user_response({halt, 200}, ContentType, RD, Ctx) ->
    {{halt, 200}, set_resp_data(ContentType, RD, Ctx), Ctx};
user_response({error, Reason}, _, RD, Ctx) ->
    riak_cs_aws_response:api_error(Reason, RD, Ctx).

format_user_record(User, ?JSON_TYPE) ->
    riak_cs_json:to_json(User);
format_user_record(User, ?XML_TYPE) ->
    riak_cs_xml:to_xml(User).
