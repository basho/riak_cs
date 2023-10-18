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
         forbidden/2,
         options/2,
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
              forbidden/2,
              options/2,
              content_types_accepted/2,
              generate_etag/2,
              last_modified/2,
              multiple_choices/2,
              accept_wwwform/2,
              allowed_methods/2,
              valid_entity_length/2,
              post_is_create/2,
              create_path/2,
              finish_request/2
             ]).

-include("riak_cs_web.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

-spec init([proplists:proplist()]) -> {ok, #rcs_web_context{}}.
init(Config) ->
    %% Check if authentication is disabled and set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    AuthModule = proplists:get_value(auth_module, Config),
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    StatsPrefix = no_stats,
    Ctx = #rcs_web_context{auth_bypass = AuthBypass,
                           auth_module = AuthModule,
                           response_module = RespModule,
                           stats_prefix = StatsPrefix,
                           api = Api},
    {ok, Ctx}.


-spec options(#wm_reqdata{}, #rcs_web_context{}) -> {[{string(), string()}], #wm_reqdata{}, #rcs_web_context{}}.
options(RD, Ctx) ->
    {riak_cs_wm_utils:cors_headers(),
     RD, Ctx}.

-spec service_available(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
service_available(RD, Ctx) ->
    riak_cs_wm_utils:service_available(
      wrq:set_resp_headers(riak_cs_wm_utils:cors_headers(), RD), Ctx).


-spec valid_entity_length(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
valid_entity_length(RD, Ctx) ->
    {true, RD, Ctx}.


-spec forbidden(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #rcs_web_context{}}.
forbidden(RD, Ctx) ->
    case wrq:method(RD) of
        'OPTIONS' ->
            {false, RD, Ctx};
        'POST' ->
            riak_cs_wm_utils:forbidden(RD, Ctx, iam_entity)
    end.

-spec allowed_methods(#wm_reqdata{}, #rcs_web_context{}) -> {[atom()], #wm_reqdata{}, #rcs_web_context{}}.
allowed_methods(RD, Ctx) ->
    {['POST', 'OPTIONS'], RD, Ctx}.


-spec content_types_accepted(#wm_reqdata{}, #rcs_web_context{}) ->
          {[{string(), module()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_accepted(RD, Ctx) ->
    {[{?WWWFORM_TYPE, accept_wwwform}], RD, Ctx}.


-spec generate_etag(#wm_reqdata{}, #rcs_web_context{}) ->
          {undefined|string(), #wm_reqdata{}, #rcs_web_context{}}.
generate_etag(RD, Ctx) ->
    {undefined, RD, Ctx}.


-spec last_modified(#wm_reqdata{}, #rcs_web_context{}) ->
          {undefined|string(), #wm_reqdata{}, #rcs_web_context{}}.
last_modified(RD, Ctx) ->
    {undefined, RD, Ctx}.

-spec post_is_create(#wm_reqdata{}, #rcs_web_context{}) ->
          {true, #wm_reqdata{}, #rcs_web_context{}}.
post_is_create(RD, Ctx) ->
    {true, RD, Ctx}.


-spec create_path(#wm_reqdata{}, #rcs_web_context{}) ->
          {string(), #wm_reqdata{}, #rcs_web_context{}}.
create_path(RD, Ctx) ->
    {wrq:disp_path(RD), RD, Ctx}.


-spec multiple_choices(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
multiple_choices(RD, Ctx) ->
    {false, RD, Ctx}.


-spec accept_wwwform(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, term()}, term(), term()}.
accept_wwwform(RD, Ctx) ->
    Form = mochiweb_util:parse_qs(wrq:req_body(RD)),
    Action = proplists:get_value("Action", Form),
    do_action(Action, Form, RD, Ctx).

-spec finish_request(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, term()}, term(), term()}.
finish_request(RD, Ctx=#rcs_web_context{riak_client = undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx=#rcs_web_context{riak_client=RcPid}) ->
    riak_cs_riak_client:checkin(RcPid),
    {true, RD, Ctx#rcs_web_context{riak_client = undefined}}.


%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

do_action("CreateUser",
          Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Specs = finish_tags(
              lists:foldl(fun create_user_fields_filter/2, #{}, Form)),
    case create_user_require_fields(Specs) of
        false ->
            ResponseMod:api_error(missing_parameter, RD, Ctx);
        true ->
            case riak_cs_iam:create_user(Specs) of
                {ok, User} ->
                    logger:info("Created user ~s \"~s\" (~s) on request_id ~s",
                                [User?IAM_USER.id, User?IAM_USER.name, User?IAM_USER.arn, RequestId]),
                    Doc = riak_cs_xml:to_xml(
                            #create_user_response{user = User,
                                                  request_id = RequestId}),
                    {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx)
            end
    end;

do_action("GetUser",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    UserName = proplists:get_value("UserName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_user(#{name => UserName}, Pbc) of
        {ok, {User, _}} ->
            Doc = riak_cs_xml:to_xml(
                    #get_user_response{user = User,
                                       request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, notfound} ->
            ResponseMod:api_error(no_such_user, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DeleteUser",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Name = proplists:get_value("UserName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_user(#{name => list_to_binary(Name)}, Pbc) of
        {ok, {?IAM_USER{arn = Arn} = User, _}} ->
            case riak_cs_iam:delete_user(User) of
                ok ->
                    logger:info("Deleted user \"~s\" (~s) on request_id ~s", [Name, Arn, RequestId]),
                    Doc = riak_cs_xml:to_xml(
                            #delete_user_response{request_id = RequestId}),
                    {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx)
            end;
        {error, notfound} ->
            ResponseMod:api_error(no_such_user, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("ListUsers",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PathPrefix = proplists:get_value("PathPrefix", Form, "/"),
    MaxItems = proplists:get_value("MaxItems", Form),
    Marker = proplists:get_value("Marker", Form),
    case riak_cs_api:list_users(
           RcPid, #list_users_request{request_id = RequestId,
                                      path_prefix = list_to_binary(PathPrefix),
                                      max_items = MaxItems,
                                      marker = Marker}) of
        {ok, #{users := Users,
               marker := NewMarker,
               is_truncated := IsTruncated}} ->
            Doc = riak_cs_xml:to_xml(
                    #list_users_response{users = Users,
                                         request_id = RequestId,
                                         marker = NewMarker,
                                         is_truncated = IsTruncated}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;


do_action("CreateRole",
          Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Specs = finish_tags(
              lists:foldl(fun create_role_fields_filter/2, #{}, Form)),
    case create_role_require_fields(Specs) of
        false ->
            ResponseMod:api_error(missing_parameter, RD, Ctx);
        true ->
            case riak_cs_iam:create_role(Specs) of
                {ok, Role} ->
                    logger:info("Created role ~s \"~s\" (~s) on request_id ~s",
                                [Role?IAM_ROLE.role_id, Role?IAM_ROLE.role_name, Role?IAM_ROLE.arn, RequestId]),
                    Doc = riak_cs_xml:to_xml(
                            #create_role_response{role = Role,
                                                  request_id = RequestId}),
                    {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx)
            end
    end;

do_action("GetRole",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    RoleName = proplists:get_value("RoleName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_role(#{name => RoleName}, Pbc) of
        {ok, Role} ->
            Doc = riak_cs_xml:to_xml(
                    #get_role_response{role = Role,
                                       request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, notfound} ->
            ResponseMod:api_error(no_such_role, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DeleteRole",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Name = proplists:get_value("RoleName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:find_role(#{name => list_to_binary(Name)}, Pbc) of
        {ok, ?IAM_ROLE{arn = Arn}} ->
            case riak_cs_iam:delete_role(Arn) of
                ok ->
                    logger:info("Deleted role \"~s\" (~s) on request_id ~s", [Name, Arn, RequestId]),
                    Doc = riak_cs_xml:to_xml(
                            #delete_role_response{request_id = RequestId}),
                    {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx)
            end;
        {error, notfound} ->
            ResponseMod:api_error(no_such_role, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("ListRoles",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PathPrefix = proplists:get_value("PathPrefix", Form, "/"),
    MaxItems = proplists:get_value("MaxItems", Form),
    Marker = proplists:get_value("Marker", Form),
    case riak_cs_api:list_roles(
           RcPid, #list_roles_request{request_id = RequestId,
                                      path_prefix = list_to_binary(PathPrefix),
                                      max_items = MaxItems,
                                      marker = Marker}) of
        {ok, #{roles := Roles,
               marker := NewMarker,
               is_truncated := IsTruncated}} ->
            Doc = riak_cs_xml:to_xml(
                    #list_roles_response{roles = Roles,
                                         request_id = RequestId,
                                         marker = NewMarker,
                                         is_truncated = IsTruncated}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;


do_action("CreatePolicy",
          Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Specs = finish_tags(
              lists:foldl(fun create_policy_fields_filter/2, #{}, Form)),
    case create_policy_require_fields(Specs) of
        false ->
            ResponseMod:api_error(missing_parameter, RD, Ctx);
        true ->
            case riak_cs_iam:create_policy(Specs) of
                {ok, Policy} ->
                    logger:info("Created managed policy \"~s\" (~s) on request_id ~s",
                                [Policy?IAM_POLICY.policy_id, Policy?IAM_POLICY.arn, RequestId]),
                    Doc = riak_cs_xml:to_xml(
                            #create_policy_response{policy = Policy,
                                                    request_id = RequestId}),
                    {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx)
            end
    end;

do_action("GetPolicy",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Arn = proplists:get_value("PolicyArn", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:get_policy(list_to_binary(Arn), Pbc) of
        {ok, Policy} ->
            Doc = riak_cs_xml:to_xml(
                    #get_policy_response{policy = Policy,
                                         request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, notfound} ->
            ResponseMod:api_error(no_such_policy, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DeletePolicy",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Arn = proplists:get_value("PolicyArn", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:delete_policy(list_to_binary(Arn), Pbc) of
        ok ->
            logger:info("Deleted policy with arn ~s on request_id ~s", [Arn, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #delete_policy_response{request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, notfound} ->
            ResponseMod:api_error(no_such_policy, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("ListPolicies",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PathPrefix = proplists:get_value("PathPrefix", Form, "/"),
    OnlyAttached = proplists:get_value("OnlyAttached", Form, "false"),
    PolicyUsageFilter = proplists:get_value("PolicyUsageFilter", Form, "All"),
    Scope = proplists:get_value("Scope", Form, "All"),
    MaxItems = proplists:get_value("MaxItems", Form),
    Marker = proplists:get_value("Marker", Form),
    case riak_cs_api:list_policies(
           RcPid, #list_policies_request{request_id = RequestId,
                                         path_prefix = list_to_binary(PathPrefix),
                                         only_attached = list_to_atom(OnlyAttached),
                                         scope = list_to_atom(Scope),
                                         policy_usage_filter = list_to_atom(PolicyUsageFilter),
                                         max_items = MaxItems,
                                         marker = Marker}) of
        {ok, #{policies := Policies,
               marker := NewMarker,
               is_truncated := IsTruncated}} ->
            Doc = riak_cs_xml:to_xml(
                    #list_policies_response{policies = Policies,
                                            request_id = RequestId,
                                            marker = NewMarker,
                                            is_truncated = IsTruncated}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("ListAttachedUserPolicies",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PathPrefix = proplists:get_value("PathPrefix", Form, "/"),
    UserName = proplists:get_value("UserName", Form),
    %% _MaxItems = proplists:get_value("MaxItems", Form),
    %% _Marker = proplists:get_value("Marker", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:list_attached_user_policies(
           list_to_binary(UserName),
           list_to_binary(PathPrefix), Pbc) of
        {ok, AANN} ->
            NewMarker = undefined,
            IsTruncated = false,
            Doc = riak_cs_xml:to_xml(
                    #list_attached_user_policies_response{policies = AANN,
                                                          request_id = RequestId,
                                                          marker = NewMarker,
                                                          is_truncated = IsTruncated}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("AttachRolePolicy",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PolicyArn = proplists:get_value("PolicyArn", Form),
    RoleName = proplists:get_value("RoleName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:attach_role_policy(list_to_binary(PolicyArn),
                                        list_to_binary(RoleName), Pbc) of
        ok ->
            logger:info("Attached policy ~s to role ~s on request_id ~s",
                        [PolicyArn, RoleName, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #attach_role_policy_response{request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("AttachUserPolicy",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PolicyArn = proplists:get_value("PolicyArn", Form),
    UserName = proplists:get_value("UserName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:attach_user_policy(list_to_binary(PolicyArn),
                                        list_to_binary(UserName), Pbc) of
        ok ->
            logger:info("Attached policy ~s to user ~s on request_id ~s",
                        [PolicyArn, UserName, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #attach_user_policy_response{request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DetachRolePolicy",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PolicyArn = proplists:get_value("PolicyArn", Form),
    RoleName = proplists:get_value("RoleName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:detach_role_policy(list_to_binary(PolicyArn),
                                        list_to_binary(RoleName), Pbc) of
        ok ->
            logger:info("Detached policy ~s from role ~s on request_id ~s",
                        [PolicyArn, RoleName, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #detach_role_policy_response{request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DetachUserPolicy",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    PolicyArn = proplists:get_value("PolicyArn", Form),
    UserName = proplists:get_value("UserName", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:detach_user_policy(list_to_binary(PolicyArn),
                                        list_to_binary(UserName), Pbc) of
        ok ->
            logger:info("Detached policy ~s from user ~s on request_id ~s",
                        [PolicyArn, UserName, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #detach_role_policy_response{request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;


do_action("CreateSAMLProvider",
          Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Specs = finish_tags(
              lists:foldl(fun create_saml_provider_fields_filter/2, #{}, Form)),
    case create_saml_provider_require_fields(Specs) of
        false ->
            ResponseMod:api_error(missing_parameter, RD, Ctx);
        true ->
            case riak_cs_iam:create_saml_provider(Specs) of
                {ok, {Arn, Tags}} ->
                    logger:info("Created SAML Provider \"~s\" (~s) on request_id ~s",
                                [maps:get(name, Specs), Arn, RequestId]),
                    Doc = riak_cs_xml:to_xml(
                            #create_saml_provider_response{saml_provider_arn = Arn,
                                                           tags = Tags,
                                                           request_id = RequestId}),
                    {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx)
            end
    end;

do_action("GetSAMLProvider",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Arn = proplists:get_value("SAMLProviderArn", Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_iam:get_saml_provider(list_to_binary(Arn), Pbc) of
        {ok, ?IAM_SAML_PROVIDER{create_date = CreateDate,
                                valid_until = ValidUntil,
                                tags = Tags}} ->
            Doc = riak_cs_xml:to_xml(
                    #get_saml_provider_response{create_date = CreateDate,
                                                valid_until = ValidUntil,
                                                tags = Tags,
                                                request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, notfound} ->
            ResponseMod:api_error(no_such_saml_provider, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("DeleteSAMLProvider",
          Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Arn = proplists:get_value("SAMLProviderArn", Form),
    case riak_cs_iam:delete_saml_provider(list_to_binary(Arn)) of
        ok ->
            logger:info("Deleted SAML Provider with arn ~s on request_id ~s", [Arn, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #delete_saml_provider_response{request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, notfound} ->
            ResponseMod:api_error(no_such_saml_provider, RD, Ctx);
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action("ListSAMLProviders",
          _Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                            response_module = ResponseMod,
                                            request_id = RequestId}) ->
    case riak_cs_api:list_saml_providers(
           RcPid, #list_saml_providers_request{request_id = RequestId}) of
        {ok, #{saml_providers := PP}} ->
            PPEE = [#saml_provider_list_entry{arn = Arn,
                                              create_date = CreateDate,
                                              valid_until = ValidUntil}
                    || ?IAM_SAML_PROVIDER{arn = Arn,
                                          create_date = CreateDate,
                                          valid_until = ValidUntil} <- PP],
            Doc = riak_cs_xml:to_xml(
                    #list_saml_providers_response{saml_provider_list = PPEE,
                                                  request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action(Unsupported, _Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod}) ->
    logger:warning("IAM action ~s not supported yet; ignoring request", [Unsupported]),
    ResponseMod:api_error(invalid_action, RD, Ctx).


create_user_fields_filter({K, V}, Acc) ->
    case K of
        "Path" ->
            maps:put(path, list_to_binary(V), Acc);
        "PermissionsBoundary" ->
            maps:put(permissions_boundary, list_to_binary(V), Acc);
        "UserName" ->
            maps:put(user_name, list_to_binary(V), Acc);
        "Tags.member." ++ TagMember ->
            add_tag(TagMember, V, Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter for CreateUser: ~s", [Unrecognized]),
            Acc
    end.
create_user_require_fields(FF) ->
    lists:all(fun(A) -> lists:member(A, maps:keys(FF)) end,
              [user_name]).

create_role_fields_filter({K, V}, Acc) ->
    case K of
        "AssumeRolePolicyDocument" ->
            maps:put(assume_role_policy_document, list_to_binary(V), Acc);
        "Description" ->
            maps:put(description, list_to_binary(V), Acc);
        "MaxSessionDuration" ->
            maps:put(max_session_duration, list_to_integer(V), Acc);
        "Path" ->
            maps:put(path, list_to_binary(V), Acc);
        "PermissionsBoundary" ->
            maps:put(permissions_boundary, list_to_binary(V), Acc);
        "RoleName" ->
            maps:put(role_name, list_to_binary(V), Acc);
        "Tags.member." ++ TagMember ->
            add_tag(TagMember, V, Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter for CreateRole: ~s", [Unrecognized]),
            Acc
    end.
create_role_require_fields(FF) ->
    lists:all(fun(A) -> lists:member(A, maps:keys(FF)) end,
             [assume_role_policy_document,
              role_name]).

create_policy_fields_filter({K, V}, Acc) ->
    case K of
        "Description" ->
            maps:put(description, list_to_binary(V), Acc);
        "Path" ->
            maps:put(path, list_to_binary(V), Acc);
        "PolicyDocument" ->
            maps:put(policy_document, list_to_binary(V), Acc);
        "PolicyName" ->
            maps:put(policy_name, list_to_binary(V), Acc);
        "Tags.member." ++ TagMember ->
            add_tag(TagMember, V, Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter for CreatePolicy: ~s", [Unrecognized]),
            Acc
    end.
create_policy_require_fields(FF) ->
    lists:all(fun(A) -> lists:member(A, maps:keys(FF)) end,
              [policy_name,
               policy_document]).

create_saml_provider_fields_filter({K, V}, Acc) ->
    case K of
        "Name" ->
            maps:put(name, list_to_binary(V), Acc);
        "SAMLMetadataDocument" ->
            maps:put(saml_metadata_document, list_to_binary(V), Acc);
        "Tags.member." ++ TagMember ->
            add_tag(TagMember, V, Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter in call to CreateSAMLProvider: ~s", [Unrecognized]),
            Acc
    end.
create_saml_provider_require_fields(FF) ->
    lists:all(fun(A) -> lists:member(A, maps:keys(FF)) end,
              [name,
               saml_metadata_document]).

add_tag(A, V, Acc) ->
    Tags0 = maps:get(tags, Acc, []),
    Tags =
        case string:tokens(A, ".") of
            [N, "Key"] ->
                lists:keystore({k, N}, 1, Tags0, {{k, N}, list_to_binary(V)});
            [N, "Value"] ->
                lists:keystore({v, N}, 1, Tags0, {{v, N}, list_to_binary(V)});
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
