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

-module(riak_cs_wm_sts).

-export([init/1,
         service_available/2,
         malformed_request/2,
         forbidden/2,
         authorize/2,
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

-ignore_xref([init/1,
              service_available/2,
              malformed_request/2,
              forbidden/2,
              authorize/2,
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
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/logger.hrl").

-define(UNSIGNED_API_CALLS, ["AssumeRoleWithSAML"]).

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


-spec service_available(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
service_available(RD, Ctx = #rcs_web_context{rc_pool = undefined}) ->
    service_available(RD, Ctx#rcs_web_context{rc_pool = request_pool});
service_available(RD, Ctx = #rcs_web_context{rc_pool = RcPool}) ->
    case riak_cs_riak_client:checkout(RcPool) of
        {ok, RcPid} ->
            {true, RD, Ctx#rcs_web_context{riak_client = RcPid}};
        {error, _Reason} ->
            {false, RD, Ctx}
    end.

-spec malformed_request(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.


-spec valid_entity_length(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
valid_entity_length(RD, Ctx) ->
    {true, RD, Ctx}.


-spec forbidden(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #rcs_web_context{}}.
forbidden(RD, Ctx=#rcs_web_context{auth_module = AuthMod,
                                   riak_client = RcPid,
                                   request_id = RequestId}) ->
    case unsigned_call_allowed(RD) of
        true ->
            {false, RD, Ctx};
        false ->
            AuthResult =
                case AuthMod:identify(RD, Ctx) of
                    failed ->
                        %% Identification failed, deny access
                        {error, no_such_key};
                    {failed, Reason} ->
                        {error, Reason};
                    {UserKey, AuthData} ->
                        case riak_cs_config:admin_creds() of
                            {ok, {UserKey, _}} ->
                                logger:notice("Granting admin access to request ~s", [RequestId]),
                                {ok, admin_access};
                            _NoAdmiAccess ->
                                case riak_cs_user:get_user(UserKey, RcPid) of
                                    {ok, {User, Obj}} = _LookupResult ->
                                        authenticate(User, Obj, RD, Ctx, AuthData);
                                    Error ->
                                        Error
                                end
                        end
                end,
            post_authentication(AuthResult, RD, Ctx, fun authorize/2)
    end.

unsigned_call_allowed(RD) ->
    Form = mochiweb_util:parse_qs(wrq:req_body(RD)),
    lists:member(proplists:get_value("Action", Form),
                 ?UNSIGNED_API_CALLS).


post_authentication({ok, admin_access}, RD, Ctx, _Authorize) ->
    {false, RD, Ctx#rcs_web_context{admin_access = true}};
post_authentication({ok, User, UserObj}, RD, Ctx, Authorize) ->
    Authorize(RD, Ctx#rcs_web_context{user = User,
                                      user_object = UserObj});
post_authentication({error, no_user_key}, RD, Ctx, Authorize) ->
    %% no keyid was given, proceed anonymously
    ?LOG_DEBUG("No user key"),
    Authorize(RD, Ctx);
post_authentication({error, bad_auth}, RD, #rcs_web_context{response_module = ResponseMod} = Ctx, _) ->
    %% given keyid was found, but signature didn't match
    ?LOG_DEBUG("bad_auth"),
    ResponseMod:api_error(access_denied, RD, Ctx);
post_authentication({error, reqtime_tooskewed} = Error, RD,
                    #rcs_web_context{response_module = ResponseMod} = Ctx, _) ->
    ?LOG_DEBUG("reqtime_tooskewed"),
    ResponseMod:api_error(Error, RD, Ctx);
post_authentication({error, {auth_not_supported, AuthType}}, RD,
                    #rcs_web_context{response_module = ResponseMod} = Ctx, _) ->
    ?LOG_DEBUG("auth_not_supported: ~s", [AuthType]),
    ResponseMod:api_error({auth_not_supported, AuthType}, RD, Ctx);
post_authentication({error, notfound}, RD, #rcs_web_context{response_module = ResponseMod} = Ctx, _) ->
    ?LOG_DEBUG("User does not exist"),
    ResponseMod:api_error(invalid_access_key_id, RD, Ctx);
post_authentication({error, Reason}, RD,
                    #rcs_web_context{response_module = ResponseMod} = Ctx, _) ->
    %% Lookup failed, basically due to disconnected stuff
    ?LOG_DEBUG("Authentication error: ~p", [Reason]),
    ResponseMod:api_error(Reason, RD, Ctx).

authenticate(User, UserObj, RD, Ctx = #rcs_web_context{auth_module = AuthMod}, AuthData)
  when User?RCS_USER.status =:= enabled ->
    case AuthMod:authenticate(User, AuthData, RD, Ctx) of
        ok ->
            {ok, User, UserObj};
        {error, reqtime_tooskewed} ->
            {error, reqtime_tooskewed};
        {error, _Reason} ->
            {error, bad_auth}
    end;
authenticate(_User, _UserObj, _RD, _Ctx, _AuthData) ->
    %% {ok, _} -> %% disabled account, we are going to 403
    {error, bad_auth}.

-spec allowed_methods(#wm_reqdata{}, #rcs_web_context{}) -> {[atom()], #wm_reqdata{}, #rcs_web_context{}}.
allowed_methods(RD, Ctx) ->
    {['POST'], RD, Ctx}.


-spec content_types_accepted(#wm_reqdata{}, #rcs_web_context{}) ->
          {[{string(), module()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_accepted(RD, Ctx) ->
    {[{?WWWFORM_TYPE, accept_wwwform}], RD, Ctx}.


-spec authorize(#wm_reqdata{}, #rcs_web_context{}) ->
          {boolean() | {halt, term()}, #wm_reqdata{}, #rcs_web_context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:role_access_authorize_helper(sts_entity, RD, Ctx).


-spec generate_etag(#wm_reqdata{}, #rcs_web_context{}) ->
          {undefined, #wm_reqdata{}, #rcs_web_context{}}.
generate_etag(RD, Ctx) ->
    {undefined, RD, Ctx}.


-spec last_modified(#wm_reqdata{}, #rcs_web_context{}) ->
          {undefined, #wm_reqdata{}, #rcs_web_context{}}.
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
finish_request(RD, Ctx=#rcs_web_context{riak_client = RcPid}) ->
    riak_cs_riak_client:checkin(RcPid),
    {true, RD, Ctx#rcs_web_context{riak_client = undefined}}.


%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

do_action("AssumeRoleWithSAML",
          Form, RD, Ctx = #rcs_web_context{riak_client = RcPid,
                                           response_module = ResponseMod,
                                           request_id = RequestId}) ->
    Specs = lists:foldl(fun assume_role_with_saml_fields_filter/2,
                        #{request_id => RequestId}, Form),
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_sts:assume_role_with_saml(Specs, Pbc) of
        {ok, #{assumed_role_user := #assumed_role_user{assumed_role_id = AssumedRoleId} = AssumedRoleUser,
               audience := Audience,
               credentials := Credentials,
               issuer := Issuer,
               name_qualifier := NameQualifier,
               packed_policy_size := PackedPolicySize,
               source_identity := SourceIdentity,
               subject := Subject,
               subject_type := SubjectType}} ->
            logger:info("AssumeRoleWithSAML completed for user ~s (key_id: ~s) on request_id ~s",
                        [AssumedRoleId, Credentials#credentials.access_key_id, RequestId]),
            Doc = riak_cs_xml:to_xml(
                    #assume_role_with_saml_response{assumed_role_user = AssumedRoleUser,
                                                    audience = Audience,
                                                    credentials = Credentials,
                                                    issuer = Issuer,
                                                    name_qualifier = NameQualifier,
                                                    packed_policy_size = PackedPolicySize,
                                                    source_identity = SourceIdentity,
                                                    subject = Subject,
                                                    subject_type = SubjectType,
                                                    request_id = RequestId}),
            {true, riak_cs_wm_utils:make_final_rd(Doc, RD), Ctx};
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end;

do_action(Unsupported, _Form, RD, Ctx = #rcs_web_context{response_module = ResponseMod}) ->
    logger:warning("STS action ~s not supported yet; ignoring request", [Unsupported]),
    ResponseMod:api_error(invalid_action, RD, Ctx).


assume_role_with_saml_fields_filter({K, V}, Acc) ->
    case K of
        "DurationSeconds" ->
            maps:put(duration_seconds, list_to_integer(V), Acc);
        "Policy" ->
            maps:put(policy, base64:encode(V), Acc);
        "PrincipalArn" ->
            maps:put(principal_arn, list_to_binary(V), Acc);
        "RoleArn" ->
            maps:put(role_arn, list_to_binary(V), Acc);
        "SAMLAssertion" ->
            maps:put(saml_assertion, list_to_binary(V), Acc);
        "PolicyArns.member." ++ _MemberNo ->
            AA = maps:get(policy_arns, Acc, []),
            maps:put(policy_arns, AA ++ [list_to_binary(V)], Acc);
        CommonParameter when CommonParameter == "Action";
                             CommonParameter == "Version" ->
            Acc;
        Unrecognized ->
            logger:warning("Unrecognized parameter in call to AssumeRoleWithSAML: ~s", [Unrecognized]),
            Acc
    end.
