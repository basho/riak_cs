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

-module(riak_cs_wm_common).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_accepted/2,
         content_types_provided/2,
         generate_etag/2,
         last_modified/2,
         valid_entity_length/2,
         validate_content_checksum/2,
         malformed_request/2,
         to_xml/2,
         to_json/2,
         post_is_create/2,
         create_path/2,
         process_post/2,
         resp_body/2,
         multiple_choices/2,
         accept_body/2,
         produce_body/2,
         allowed_methods/2,
         delete_resource/2,
         finish_request/2]).

-export([default_allowed_methods/0,
         default_content_types_accepted/2,
         default_content_types_provided/2,
         default_generate_etag/2,
         default_last_modified/2,
         default_finish_request/2,
         default_init/1,
         default_authorize/2,
         default_malformed_request/2,
         default_valid_entity_length/2,
         default_validate_content_checksum/2,
         default_delete_resource/2,
         default_anon_ok/0,
         default_produce_body/2]).

-include("riak_cs.hrl").
-include("oos_api.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% ===================================================================
%% Webmachine callbacks
%% ===================================================================

-spec init([{atom(),term()}]) -> {ok, #context{}}.
init(Config) ->
    _ = dyntrace:put_tag(pid_to_list(self())),
    Mod = proplists:get_value(submodule, Config),
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"init">>),
    %% Check if authentication is disabled and set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    AuthModule = proplists:get_value(auth_module, Config),
    Api = riak_cs_config:api(),
    RespModule = riak_cs_config:response_module(Api),
    PolicyModule = proplists:get_value(policy_module, Config),
    Exports = orddict:from_list(Mod:module_info(exports)),
    ExportsFun = exports_fun(Exports),
    Ctx = #context{auth_bypass=AuthBypass,
                   auth_module=AuthModule,
                   response_module=RespModule,
                   policy_module=PolicyModule,
                   exports_fun=ExportsFun,
                   start_time=os:timestamp(),
                   submodule=Mod,
                   api=Api},
    resource_call(Mod, init, [Ctx], ExportsFun).

-spec service_available(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
service_available(RD, Ctx=#context{submodule=Mod,riakc_pool=undefined}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"service_available">>),
    case riak_cs_utils:riak_connection() of
        {ok, Pid} ->
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"service_available">>, [1], []),
            {true, RD, Ctx#context{riakc_pid=Pid}};
        {error, _Reason} ->
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"service_available">>, [0], []),
            {false, RD, Ctx}
    end;
service_available(RD, Ctx=#context{submodule=Mod,riakc_pool=Pool}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"service_available">>),
    case riak_cs_utils:riak_connection(Pool) of
        {ok, Pid} ->
            {true, RD, Ctx#context{riakc_pid=Pid}};
        {error, _Reason} ->
            {false, RD, Ctx}
    end.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
malformed_request(RD, Ctx=#context{submodule=Mod,
                                   exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"malformed_request">>),
    {Malformed, _, _} = R = resource_call(Mod,
                                          malformed_request,
                                          [RD, Ctx],
                                          ExportsFun),
    riak_cs_dtrace:dt_wm_return_bool_with_default({?MODULE, Mod},
                                                  <<"malformed_request">>,
                                                  Malformed,
                                                  false),
    R.


-spec valid_entity_length(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
valid_entity_length(RD, Ctx=#context{submodule=Mod, exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"valid_entity_length">>),
    {Valid, _, _} = R = resource_call(Mod,
                                      valid_entity_length,
                                      [RD, Ctx],
                                      ExportsFun),
    riak_cs_dtrace:dt_wm_return_bool_with_default({?MODULE, Mod},
                                                  <<"valid_entity_length">>,
                                                  Valid,
                                                  true),
    R.

-type validate_checksum_response() :: {error, term()} |
                                      {halt, pos_integer()} |
                                      boolean().
-spec validate_content_checksum(#wm_reqdata{}, #context{}) ->
                                       {validate_checksum_response(), #wm_reqdata{}, #context{}}.
validate_content_checksum(RD, Ctx=#context{submodule=Mod, exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"validate_content_checksum">>),
    {Valid, _, _} = R = resource_call(Mod,
                                      validate_content_checksum,
                                      [RD, Ctx],
                                      ExportsFun),
    riak_cs_dtrace:dt_wm_return_bool_with_default({?MODULE, Mod},
                                                  <<"validate_content_checksum">>,
                                                  Valid,
                                                  true),
    R.

-spec forbidden(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
forbidden(RD, Ctx=#context{auth_module=AuthMod,
                           submodule=Mod,
                           riakc_pid=RiakPid,
                           exports_fun=ExportsFun}) ->
    {AuthResult, AnonOk} =
        case AuthMod:identify(RD, Ctx) of
            {UserKey, AuthData} ->
                riak_cs_dtrace:dt_wm_entry({?MODULE, Mod},
                                           <<"forbidden">>,
                                           [],
                                           [riak_cs_wm_utils:extract_name(UserKey)]),
                UserLookupResult = maybe_create_user(
                                     riak_cs_utils:get_user(UserKey, RiakPid),
                                     UserKey,
                                     Ctx#context.api,
                                     Ctx#context.auth_module,
                                     AuthData,
                                     RiakPid),
                {authenticate(UserLookupResult, RD, Ctx, AuthData),
                 resource_call(Mod, anon_ok, [], ExportsFun)};
            failed ->
                %% Identification failed, deny access
                {{error, no_such_key}, false}
        end,
    case post_authentication(AuthResult, RD, Ctx, fun authorize/2, AnonOk) of
        {false, _RD2, Ctx2} = FalseRet ->
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod},
                                        <<"forbidden">>, [],
                                        [riak_cs_wm_utils:extract_name(Ctx2#context.user),
                                         <<"false">>]),
            FalseRet;
        {Rsn, _RD2, Ctx2} = Ret ->
            Reason =
                case Rsn of
                    {halt, Code} -> Code;
                    _            -> -1
                end,
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod},
                                        <<"forbidden">>, [Reason],
                                        [riak_cs_wm_utils:extract_name(Ctx2#context.user),
                                        <<"true">>]),
            Ret
    end.

maybe_create_user({ok, {_, _}}=UserResult, _, _, _, _, _) ->
    UserResult;
maybe_create_user({error, NE}, KeyId, oos, _, {UserData, _}, RiakPid)
  when NE =:= not_found;
       NE =:= notfound;
       NE =:= no_user_key ->
    {Name, Email, UserId} = UserData,
    {_, Secret} = riak_cs_oos_utils:user_ec2_creds(UserId, KeyId),
    %% Attempt to create a Riak CS user to represent the OS tenant
    _ = riak_cs_utils:create_user(Name, Email, KeyId, Secret),
    riak_cs_utils:get_user(KeyId, RiakPid);
maybe_create_user({error, NE}, KeyId, s3, riak_cs_keystone_auth, {UserData, _}, RiakPid)
  when NE =:= not_found;
       NE =:= notfound;
       NE =:= no_user_key ->
    {Name, Email, UserId} = UserData,
    {_, Secret} = riak_cs_oos_utils:user_ec2_creds(UserId, KeyId),
    %% Attempt to create a Riak CS user to represent the OS tenant
    _ = riak_cs_utils:create_user(Name, Email, KeyId, Secret),
    riak_cs_utils:get_user(KeyId, RiakPid);
maybe_create_user({error, Reason}=Error, _, Api, _, _, _) ->
    _ = lager:error("Retrieval of user record for ~p failed. Reason: ~p",
                    [Api, Reason]),
    Error.

%% @doc Get the list of methods a resource supports.
-spec allowed_methods(#wm_reqdata{}, #context{}) -> {[atom()], #wm_reqdata{}, #context{}}.
allowed_methods(RD, Ctx=#context{submodule=Mod,
                                 exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"allowed_methods">>),
    Methods = resource_call(Mod,
                            allowed_methods,
                            [],
                            ExportsFun),
    {Methods, RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx=#context{submodule=Mod,
                                        exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"content_types_accepted">>),
    resource_call(Mod,
                  content_types_accepted,
                  [RD,Ctx],
                  ExportsFun).

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx=#context{submodule=Mod,
                                        exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"content_types_provided">>),
    resource_call(Mod,
                  content_types_provided,
                  [RD,Ctx],
                  ExportsFun).

-spec generate_etag(#wm_reqdata{}, #context{}) -> {string(), #wm_reqdata{}, #context{}}.
generate_etag(RD, Ctx=#context{submodule=Mod,
                               exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"generate_etag">>),
    resource_call(Mod,
                  generate_etag,
                  [RD,Ctx],
                  ExportsFun).

-spec last_modified(#wm_reqdata{}, #context{}) -> {calendar:datetime(), #wm_reqdata{}, #context{}}.
last_modified(RD, Ctx=#context{submodule=Mod,
                               exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"last_modified">>),
    resource_call(Mod,
                  last_modified,
                  [RD,Ctx],
                  ExportsFun).

-spec delete_resource(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{submodule=Mod,exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"delete_resource">>),
    %% TODO: add dt_wm_return from subresource?
    resource_call(Mod,
                  delete_resource,
                  [RD,Ctx],
                  ExportsFun).

-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{user=User,
                        submodule=Mod,
                        exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"to_xml">>),
    Res = resource_call(Mod,
                        to_xml,
                        [RD, Ctx],
                        ExportsFun),
    riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"to_xml">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res.

-spec to_json(#wm_reqdata{}, #context{}) ->
                     {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_json(RD, Ctx=#context{user=User,
                         submodule=Mod,
                         exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"to_json">>),
    Res = resource_call(Mod,
                        to_json,
                        [RD, Ctx],
                        ExportsFun(to_json)),
    riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"to_json">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res.

post_is_create(RD, Ctx=#context{submodule=Mod,
                                exports_fun=ExportsFun}) ->
    resource_call(Mod, post_is_create, [RD, Ctx], ExportsFun).

create_path(RD, Ctx=#context{submodule=Mod,
                             exports_fun=ExportsFun}) ->
    resource_call(Mod, create_path, [RD, Ctx], ExportsFun).

process_post(RD, Ctx=#context{submodule=Mod,
                              exports_fun=ExportsFun}) ->
    resource_call(Mod, process_post, [RD, Ctx], ExportsFun).

resp_body(RD, Ctx=#context{submodule=Mod,
                           exports_fun=ExportsFun}) ->
    resource_call(Mod, resp_body, [RD, Ctx], ExportsFun).

multiple_choices(RD, Ctx=#context{submodule=Mod,
                                  exports_fun=ExportsFun}) ->
    try
        resource_call(Mod, multiple_choices, [RD, Ctx], ExportsFun)
    catch _:_ ->
            {false, RD, Ctx}
    end.

-spec accept_body(#wm_reqdata{}, #context{}) ->
                         {boolean() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{submodule=Mod,exports_fun=ExportsFun,user=User}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"accept_body">>),
    Res = resource_call(Mod,
                        accept_body,
                        [RD, Ctx],
                        ExportsFun),
    %% TODO: extract response code and add to ints field
    riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"accept_body">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res.

-spec produce_body(#wm_reqdata{}, #context{}) ->
                          {iolist()|binary(), #wm_reqdata{}, #context{}} |
                          {{known_length_stream, non_neg_integer(), {<<>>, function()}}, #wm_reqdata{}, #context{}}.
produce_body(RD, Ctx=#context{user=User,
                              submodule=Mod,
                              exports_fun=ExportsFun}) ->
    %% TODO: add dt_wm_return w/ content length
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"produce_body">>),
    Res = resource_call(Mod,
                        produce_body,
                        [RD, Ctx],
                        ExportsFun),
    riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"produce_body">>, [], [riak_cs_wm_utils:extract_name(User)]),
    Res.

-spec finish_request(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
finish_request(RD, Ctx=#context{riakc_pid=undefined,
                                submodule=Mod,
                                exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"finish_request">>, [0], []),
    Res = resource_call(Mod,
                        finish_request,
                        [RD, Ctx],
                        ExportsFun),
    riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"finish_request">>, [0], []),
    Res;
finish_request(RD, Ctx0=#context{riakc_pid=RiakcPid,
                                 submodule=Mod,
                                 riakc_pool=Pool,
                                 exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"finish_request">>, [1], []),
    case Pool of
        undefined -> riak_cs_utils:close_riak_connection(RiakcPid);
        _ -> riak_cs_utils:close_riak_connection(Pool, RiakcPid)
    end,
    riak_cs_utils:close_riak_connection(RiakcPid),
    Ctx = Ctx0#context{riakc_pid=undefined},
    Res = resource_call(Mod,
                        finish_request,
                        [RD, Ctx],
                        ExportsFun),
    riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"finish_request">>, [1], []),
    Res.

%% ===================================================================
%% Helper functions
%% ===================================================================

-spec authorize(#wm_reqdata{}, #context{}) -> {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize(RD,Ctx=#context{submodule=Mod, exports_fun=ExportsFun}) ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"authorize">>),
    {Success, _, _} = R = resource_call(Mod, authorize, [RD,Ctx], ExportsFun),
    case Success of
        {halt, Code} ->
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"authorize">>, [Code], []);
        false -> %% not forbidden, e.g. success
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"authorize">>);
        true -> %% forbidden
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"authorize">>, [403], [])
    end,
    R.

-type user_lookup_result() :: {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, term()}.
-spec authenticate(user_lookup_result(), term(), term(), term()) ->
                          {ok, rcs_user(), riakc_obj:riakc_obj()} | {error, term()}.
authenticate({ok, {User, UserObj}}, RD, Ctx=#context{auth_module=AuthMod, submodule=Mod}, AuthData)
  when User?RCS_USER.status =:= enabled ->
    riak_cs_dtrace:dt_wm_entry({?MODULE, Mod}, <<"authenticate">>, [], [atom_to_binary(AuthMod, latin1)]),
    case AuthMod:authenticate(User, AuthData, RD, Ctx) of
        ok ->
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"authenticate">>, [1], [atom_to_binary(AuthMod, latin1)]),
            {ok, User, UserObj};
        {error, _Reason} ->
            riak_cs_dtrace:dt_wm_return({?MODULE, Mod}, <<"authenticate">>, [0], [atom_to_binary(AuthMod, latin1)]),
            {error, bad_auth}
    end;
authenticate({ok, {User, _UserObj}}, _RD, _Ctx, _AuthData)
  when User?RCS_USER.status =/= enabled ->
    %% {ok, _} -> %% disabled account, we are going to 403
    {error, bad_auth};
authenticate({error, _}=Error, _RD, _Ctx, _AuthData) ->
    Error.

-spec exports_fun(orddict:orddict()) -> function().
exports_fun(Exports) ->
    fun(Function) ->
            orddict:is_key(Function, Exports)
    end.


resource_call(Mod, Fun, Args, true) ->
    erlang:apply(Mod, Fun, Args);
resource_call(_Mod, Fun, Args, false) ->
    erlang:apply(?MODULE, default(Fun), Args);
resource_call(Mod, Fun, Args, ExportsFun) ->
    resource_call(Mod, Fun, Args, ExportsFun(Fun)).

%% ===================================================================
%% Helper Functions Copied from riak_cs_wm_utils that should be removed from that module
%% ===================================================================

post_authentication({ok, User, UserObj}, RD, Ctx, Authorize, _) ->
    %% given keyid and signature matched, proceed
    Authorize(RD, Ctx#context{user=User,
                              user_object=UserObj});
post_authentication({error, no_user_key}, RD, Ctx, Authorize, true) ->
    %% no keyid was given, proceed anonymously
    _ = lager:debug("No user key"),
    Authorize(RD, Ctx);
post_authentication({error, no_user_key}, RD, Ctx, _, false) ->
    %% no keyid was given, deny access
    _ = lager:debug("No user key, deny"),
    riak_cs_wm_utils:deny_access(RD, Ctx);
post_authentication({error, bad_auth}, RD, Ctx, _, _) ->
    %% given keyid was found, but signature didn't match
    _ = lager:debug("bad_auth"),
    riak_cs_wm_utils:deny_access(RD, Ctx);
post_authentication({error, Reason}, RD, Ctx, _, _) ->
    %% no matching keyid was found, or lookup failed
    _ = lager:debug("Authentication error: ~p", [Reason]),
    riak_cs_wm_utils:deny_invalid_key(RD, Ctx).


%% ===================================================================
%% Resource function defaults
%% ===================================================================

default(init) ->
    default_init;
default(allowed_methods) ->
    default_allowed_methods;
default(content_types_accepted) ->
    default_content_types_accepted;
default(content_types_provided) ->
    default_content_types_provided;
default(generate_etag) ->
    default_generate_etag;
default(last_modified) ->
    default_last_modified;
default(malformed_request) ->
    default_malformed_request;
default(valid_entity_length) ->
    default_valid_entity_length;
default(validate_content_checksum) ->
    default_validate_content_checksum;
default(delete_resource) ->
    default_delete_resource;
default(authorize) ->
    default_authorize;
default(finish_request) ->
    default_finish_request;
default(anon_ok) ->
    default_anon_ok;
default(produce_body) ->
    default_produce_body;
default(_) ->
    undefined.

default_init(Ctx) ->
    {ok, Ctx}.

default_malformed_request(RD, Ctx) ->
    {false, RD, Ctx}.

default_valid_entity_length(RD, Ctx) ->
    {true, RD, Ctx}.

default_validate_content_checksum(RD, Ctx) ->
    {true, RD, Ctx}.

default_content_types_accepted(RD, Ctx) ->
    {[], RD, Ctx}.

-spec default_content_types_provided(#wm_reqdata{}, #context{}) ->
                                            {[{string(), atom()}],
                                             #wm_reqdata{},
                                             #context{}}.
default_content_types_provided(RD, Ctx=#context{api=oos}) ->
    {[{"text/plain", produce_body}], RD, Ctx};
default_content_types_provided(RD, Ctx) ->
    {[{"application/xml", produce_body}], RD, Ctx}.

default_generate_etag(RD, Ctx) ->
    {undefined, RD, Ctx}.

default_last_modified(RD, Ctx) ->
    {undefined, RD, Ctx}.

default_delete_resource(RD, Ctx) ->
    {false, RD, Ctx}.

default_allowed_methods() ->
    [].

default_finish_request(RD, Ctx) ->
    {true, RD, Ctx}.

default_anon_ok() ->
    true.

default_produce_body(RD, Ctx=#context{submodule=Mod,
                                       response_module=ResponseMod,
                                       exports_fun=ExportsFun}) ->
    try
        ResponseMod:respond(
          resource_call(Mod, api_request, [RD, Ctx], ExportsFun),
          RD,
          Ctx)
    catch error:{badmatch, {error, Reason}} ->
            ResponseMod:api_error(Reason, RD, Ctx)
    end.

%% @doc this function will be called by `post_authenticate/2` if the user successfully
%% authenticates and the submodule does not provide an implementation
%% of authorize/2. The default implementation does not perform any authorization
%% and simply returns false to signify the request is not fobidden
-spec default_authorize(term(), term()) -> {false, term(), term()}.
default_authorize(RD, Ctx) ->
    {false, RD, Ctx}.
