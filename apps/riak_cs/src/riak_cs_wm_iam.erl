%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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

%% @doc WM resource for IAM requests.

-module(riak_cs_wm_iam).

-export([init/1,
         service_available/2,
         malformed_request/2,
         forbidden/2,
         authorize/2,
         content_types_provided/2,
         content_types_accepted/2,
         generate_etag/2,
         last_modified/2,
         valid_entity_length/2,
         multiple_choices/2,
         accept_wwwform/2,
         allowed_methods/2,
         post_is_create/2,
         create_path/2,
         finish_request/2
        ]).

-export([finish_tags/1,
         add_tag/3]).
-ignore_xref([init/1,
              service_available/2,
              malformed_request/2,
              forbidden/2,
              authorize/2,
              content_types_provided/2,
              content_types_accepted/2,
              generate_etag/2,
              last_modified/2,
              multiple_choices/2,
              authorize/2,
              accept_wwwform/2,
              allowed_methods/2,
              valid_entity_length/2,
              post_is_create/2,
              create_path/2,
              finish_request/2
             ]).

-include("riak_cs_web.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

-spec init([proplists:proplist()]) -> {ok, #rcs_iam_context{}}.
init(Config) ->
    %% Check if authentication is disabled and set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    AuthModule = proplists:get_value(auth_module, Config),
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    StatsPrefix = no_stats,
    Ctx = #rcs_iam_context{auth_bypass = AuthBypass,
                           auth_module = AuthModule,
                           response_module = RespModule,
                           stats_prefix = StatsPrefix,
                           api = Api},
    {ok, Ctx}.


-spec service_available(#wm_reqdata{}, #rcs_iam_context{}) -> {boolean(), #wm_reqdata{}, #rcs_iam_context{}}.
service_available(RD, Ctx = #rcs_iam_context{rc_pool = undefined}) ->
    service_available(RD, Ctx#rcs_iam_context{rc_pool = request_pool});
service_available(RD, Ctx = #rcs_iam_context{rc_pool = Pool}) ->
    case riak_cs_riak_client:checkout(Pool) of
        {ok, RcPid} ->
            {true, RD, Ctx#rcs_iam_context{riak_client = RcPid}};
        {error, _Reason} ->
            {false, RD, Ctx}
    end.

-spec malformed_request(#wm_reqdata{}, #rcs_iam_context{}) -> {boolean(), #wm_reqdata{}, #rcs_iam_context{}}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.


-spec valid_entity_length(#wm_reqdata{}, #rcs_iam_context{}) -> {boolean(), #wm_reqdata{}, #rcs_iam_context{}}.
valid_entity_length(RD, Ctx) ->
    {true, RD, Ctx}.


-spec forbidden(#wm_reqdata{}, #rcs_iam_context{}) ->
          {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #rcs_iam_context{}}.
forbidden(RD, Ctx=#rcs_iam_context{auth_module = AuthMod,
                                   riak_client = RcPid}) ->
    AuthResult =
        case AuthMod:identify(RD, Ctx) of
            failed ->
                %% Identification failed, deny access
                {error, no_such_key};
            {failed, Reason} ->
                {error, Reason};
            {UserKey, AuthData} ->
                case riak_cs_user:get_user(UserKey, RcPid) of
                    {ok, {User, Obj}} = _LookupResult ->
                        authenticate(User, Obj, RD, Ctx, AuthData);
                    Error ->
                        Error
                end;
            Role ->
                riak_cs_wm_utils:eval_role_for_action(RD, Role)
        end,
    post_authentication(AuthResult, RD, Ctx, fun authorize/2).

post_authentication({ok, User, _UserObj}, RD, Ctx, Authorize) ->
    %% given keyid and signature matched, proceed
    Authorize(RD, Ctx#rcs_iam_context{user = User});
post_authentication({error, no_user_key}, RD, Ctx, Authorize) ->
    %% no keyid was given, proceed anonymously
    ?LOG_DEBUG("No user key"),
    Authorize(RD, Ctx);
post_authentication({error, bad_auth}, RD, #rcs_iam_context{response_module = ResponseMod} = Ctx, _) ->
    %% given keyid was found, but signature didn't match
    ?LOG_DEBUG("bad_auth"),
    ResponseMod:api_error(access_denied, RD, Ctx);
post_authentication({error, reqtime_tooskewed} = Error, RD,
                    #rcs_iam_context{response_module = ResponseMod} = Ctx, _) ->
    ?LOG_DEBUG("reqtime_tooskewed"),
    ResponseMod:api_error(Error, RD, Ctx);
post_authentication({error, {auth_not_supported, AuthType}}, RD,
                    #rcs_iam_context{response_module = ResponseMod} = Ctx, _) ->
    ?LOG_DEBUG("auth_not_supported: ~s", [AuthType]),
    ResponseMod:api_error({auth_not_supported, AuthType}, RD, Ctx);
post_authentication({error, notfound}, RD, #rcs_iam_context{response_module = ResponseMod} = Ctx, _) ->
    ?LOG_DEBUG("User does not exist"),
    ResponseMod:api_error(invalid_access_key_id, RD, Ctx);
post_authentication({error, Reason}, RD,
                    #rcs_iam_context{response_module = ResponseMod} = Ctx, _) ->
    %% Lookup failed, basically due to disconnected stuff
    ?LOG_DEBUG("Authentication error: ~p", [Reason]),
    ResponseMod:api_error(Reason, RD, Ctx).

authenticate(User, UserObj, RD, Ctx = #rcs_iam_context{auth_module = AuthMod}, AuthData)
  when User?RCS_USER.status =:= enabled ->
    case AuthMod:authenticate(User, AuthData, RD, Ctx) of
        ok ->
            {ok, User, UserObj};
        {error, reqtime_tooskewed} ->
            {error, reqtime_tooskewed};
        {error, _Reason} ->
            {error, bad_auth}
    end;
authenticate(User, _UserObj, _RD, _Ctx, _AuthData)
  when User?RCS_USER.status =/= enabled ->
    %% {ok, _} -> %% disabled account, we are going to 403
    {error, bad_auth}.

-spec allowed_methods(#wm_reqdata{}, #rcs_iam_context{}) -> {[atom()], #wm_reqdata{}, #rcs_iam_context{}}.
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.


-spec content_types_accepted(#wm_reqdata{}, #rcs_iam_context{}) ->
          {[{string(), module()}], #wm_reqdata{}, #rcs_iam_context{}}.
content_types_accepted(RD, Ctx) ->
    {[{?WWWFORM_TYPE, accept_wwwform}], RD, Ctx}.


-spec content_types_provided(#wm_reqdata{}, #rcs_iam_context{}) ->
          {[{string(), module()}], #wm_reqdata{}, #rcs_iam_context{}}.
content_types_provided(RD, Ctx) ->
    {[{?XML_TYPE, produce_xml}], RD, Ctx}.


-spec authorize(#wm_reqdata{}, #rcs_iam_context{}) ->
          {boolean() | {halt, term()}, #wm_reqdata{}, #rcs_iam_context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:role_access_authorize_helper(RD, Ctx).


-spec generate_etag(#wm_reqdata{}, #rcs_iam_context{}) -> {undefined|string(), #wm_reqdata{}, #rcs_iam_context{}}.
generate_etag(RD, Ctx) ->
    {undefined, RD, Ctx}.


-spec last_modified(#wm_reqdata{}, #rcs_iam_context{}) -> {undefined|string(), #wm_reqdata{}, #rcs_iam_context{}}.
last_modified(RD, Ctx) ->
    {undefined, RD, Ctx}.

-spec post_is_create(#wm_reqdata{}, #rcs_iam_context{}) ->
          {true, #wm_reqdata{}, #rcs_iam_context{}}.
post_is_create(RD, Ctx) ->
    {true, RD, Ctx}.


-spec create_path(#wm_reqdata{}, #rcs_iam_context{}) ->
          {string(), #wm_reqdata{}, #rcs_iam_context{}}.
create_path(RD, Ctx) ->
    {wrq:disp_path(RD), RD, Ctx}.


-spec multiple_choices(#wm_reqdata{}, #rcs_iam_context{}) ->
          {boolean(), #wm_reqdata{}, #rcs_iam_context{}}.
multiple_choices(RD, Ctx) ->
    {false, RD, Ctx}.


-spec accept_wwwform(#wm_reqdata{}, #rcs_iam_context{}) ->
          {boolean() | {halt, term()}, term(), term()}.
accept_wwwform(RD, Ctx) ->
    Form = mochiweb_util:parse_qs(wrq:req_body(RD)),
    Action = proplists:get_value("Action", Form),
    do_action(Action, Form, RD, Ctx).

-spec finish_request(#wm_reqdata{}, #rcs_iam_context{}) ->
          {boolean() | {halt, term()}, term(), term()}.
finish_request(RD, Ctx=#rcs_iam_context{riak_client = undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx=#rcs_iam_context{riak_client=RcPid}) ->
    riak_cs_riak_client:checkin(RcPid),
    {true, RD, Ctx#rcs_iam_context{riak_client = undefined}}.


%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

do_action("CreateRole",
          Form, RD, Ctx = #rcs_iam_context{response_module = ResponseMod}) ->
    Specs = finish_tags(
              lists:foldl(fun role_fields_filter/2, #{}, Form)),
    case riak_cs_iam:create_role(Specs) of
        {ok, RoleId} ->
            Role_ = ?IAM_ROLE{assume_role_policy_document = A} =
                riak_cs_iam:exprec_role(
                  riak_cs_iam:fix_permissions_boundary(Specs)),
            Role = Role_?IAM_ROLE{assume_role_policy_document = binary_to_list(base64:decode(A)),
                                  role_id = RoleId},
            RequestId = make_request_id(),
            logger:info("Created role \"~s\" on request_id ~s", [Role?IAM_ROLE.role_id, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #create_role_response{role = Role,
                                          request_id = RequestId}),
            {true, make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("GetRole",
          Form, RD, Ctx = #rcs_iam_context{riak_client = RcPid,
                                           response_module = ResponseMod}) ->
    RoleName = proplists:get_value("RoleName", Form),
    case riak_cs_iam:get_role(RoleName, RcPid) of
        {ok, Role} ->
            RequestId = make_request_id(),
            Doc = riak_cs_xml:to_xml(
                    #get_role_response{role = Role,
                                       request_id = RequestId}),
            {true, make_final_rd(Doc, RD), Ctx};
        {error, not_found} ->
            ResponseMod:api_error(no_such_role, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DeleteRole",
          Form, RD, Ctx = #rcs_iam_context{response_module = ResponseMod}) ->
    RoleName = proplists:get_value("RoleName", Form),
    case riak_cs_iam:delete_role(RoleName) of
        ok ->
            RequestId = make_request_id(),
            logger:info("Deleted role \"~s\" on request_id ~s", [RoleName, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #delete_role_response{request_id = RequestId}),
            {true, make_final_rd(Doc, RD), Ctx};
        {error, not_found} ->
            ResponseMod:api_error(no_such_role, RD, Ctx);
        {error, Reason} ->
            ?LOG_DEBUG("deal with me ~p", [Reason]),
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("ListRoles",
          Form, RD, Ctx = #rcs_iam_context{riak_client = RcPid,
                                           response_module = ResponseMod}) ->
    PathPrefix = proplists:get_value("PathPrefix", Form),
    MaxItems = proplists:get_value("MaxItems", Form),
    Marker = proplists:get_value("Marker", Form),
    case riak_cs_api:list_roles(
           RcPid, ?LRREQ{path_prefix = PathPrefix,
                         max_items = MaxItems,
                         marker = Marker}) of
        {ok, #{roles := Roles,
               marker := NewMarker,
               is_truncated := IsTruncated}} ->
            RequestId = make_request_id(),
            Doc = riak_cs_xml:to_xml(
                    #list_roles_response{roles = Roles,
                                         request_id = RequestId,
                                         marker = NewMarker,
                                         is_truncated = IsTruncated}),
            {true, make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("CreateSAMLProvider",
          Form, RD, Ctx = #rcs_iam_context{response_module = ResponseMod}) ->
    Specs = finish_tags(
              lists:foldl(fun create_saml_provider_fields_filter/2, #{}, Form)),

    case riak_cs_iam:create_saml_provider(Specs) of
        {ok, {Arn, Tags}} ->
            RequestId = make_request_id(),
            logger:info("Created SAML provider \"~s\" on request_id ~s", [maps:get(name, Specs), RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #create_saml_provider_response{saml_provider_arn = Arn,
                                                   tags = Tags,
                                                   request_id = RequestId}),
            {true, make_final_rd(Doc, RD), Ctx};
        {error, not_found} ->
            ResponseMod:api_error(no_such_role, RD, Ctx);
        {error, Reason} ->
            ?LOG_DEBUG("deal with me ~p", [Reason]),
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action(Unsupported, _Form, RD, Ctx = #rcs_iam_context{response_module = ResponseMod}) ->
    logger:warning("IAM action ~s not supported yet; ignoring request", [Unsupported]),
    ResponseMod:api_error(unsupported_iam_action, RD, Ctx).


role_fields_filter({ItemKey, ItemValue}, Acc) ->
    case ItemKey of
        "AssumeRolePolicyDocument" ->
            maps:put(assume_role_policy_document, base64:encode(ItemValue), Acc);
        "Description" ->
            maps:put(description, ItemValue, Acc);
        "MaxSessionDuration" ->
            maps:put(max_session_duration, ItemValue, Acc);
        "Path" ->
            maps:put(path, ItemValue, Acc);
        "PermissionsBoundary" ->
            maps:put(permissions_boundary, ItemValue, Acc);
        "RoleName" ->
            maps:put(role_name, ItemValue, Acc);
        "Tags.member." ++ TagMember ->
            add_tag(TagMember, ItemValue, Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter for CreateRole: ~s", [Unrecognized]),
            Acc
    end.

create_saml_provider_fields_filter({ItemKey, ItemValue}, Acc) ->
    case ItemKey of
        "Name" ->
            maps:put(name, ItemValue, Acc);
        "SAMLMetadataDocument" ->
            maps:put(saml_metadata_documet, ItemValue, Acc);
        "Tags.member." ++ TagMember ->
            add_tag(TagMember, ItemValue, Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter for CreateSAMLProvider: ~s", [Unrecognized]),
            Acc
    end.

add_tag(A, V, Acc) ->
    Tags0 = maps:get(tags, Acc, []),
    Tags =
        case string:tokens(A, ".") of
            [N, "Key"] ->
                lists:keystore({k, N}, 1, Tags0, {{k, N}, V});
            [N, "Value"] ->
                lists:keystore({v, N}, 1, Tags0, {{v, N}, V});
            _ ->
                logger:warning("Malformed Tags item", [])
        end,
    maps:put(tags, Tags, Acc).

finish_tags(Acc) ->
    Tags0 = lists:sort(maps:get(tags, Acc, [])),
    Tags1 = lists:foldl(
              fun({{k, N}, A}, Q) ->
                      lists:keystore(A, 1, Q, {{swap_me, N}, A});
                 ({{v, N}, A}, Q) ->
                      {_, K} = lists:keyfind({swap_me, N}, 1, Q),
                      lists:keyreplace({swap_me, N}, 1, Q, {K, A})
              end,
              [], Tags0),
    Tags2 = [#{key => K, value => V} || {K, V} <- Tags1],
    maps:put(tags, Tags2, Acc).

make_final_rd(Body, RD) ->
    wrq:set_resp_body(
      Body, wrq:set_resp_header(
              "ETag", etag(Body),
              wrq:set_resp_header(
                "Content-Type", ?XML_TYPE, RD))).

make_request_id() ->
    uuid:uuid_to_string(uuid:get_v4()).

etag(Body) ->
        riak_cs_utils:etag_from_binary(riak_cs_utils:md5(Body)).

