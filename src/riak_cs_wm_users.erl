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

-module(riak_cs_wm_users).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         allowed_methods/2,
         produce_json/2,
         produce_xml/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%% -------------------------------------------------------------------
%% Webmachine callbacks
%% -------------------------------------------------------------------

init(Config) ->
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
    riak_cs_wm_utils:service_available(RD, Ctx).

-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    {['GET', 'HEAD'], RD, Ctx}.

forbidden(RD, Ctx=#context{auth_bypass=AuthBypass}) ->
    Next = fun(NewRD, NewCtx=#context{user=User}) ->
                   forbidden(NewRD, NewCtx, User, AuthBypass)
           end,
    riak_cs_wm_utils:find_and_auth_user(RD, Ctx, Next, AuthBypass).

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
    Response = {<<>>, fun() -> riak_cs_users:stream_users(json, RiakPid, Boundary, Status) end},
    {{stream, Response}, UpdRD, Ctx}.

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
    Response = {<<>>, fun() -> riak_cs_users:stream_users(xml, RiakPid, Boundary, Status) end},
    {{stream, Response}, UpdRD, Ctx}.

finish_request(RD, Ctx=#context{riakc_pid=undefined}) ->
    {true, RD, Ctx};
finish_request(RD, Ctx=#context{riakc_pid=RiakPid}) ->
    riak_cs_utils:close_riak_connection(RiakPid),
    {true, RD, Ctx#context{riakc_pid=undefined}}.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

forbidden(RD, Ctx, undefined, true) ->
    {false, RD, Ctx};
forbidden(RD, Ctx, undefined, false) ->
    %% anonymous access disallowed
    riak_cs_wm_utils:deny_access(RD, Ctx);
forbidden(RD, Ctx, User, false) ->
    UserKeyId = User?RCS_USER.key_id,
    case riak_cs_config:admin_creds() of
        {ok, {Admin, _}} when Admin == UserKeyId ->
            %% admin account is allowed
            {false, RD, Ctx};
        _ ->
            %% no one else is allowed
            riak_cs_wm_utils:deny_access(RD, Ctx)
    end.


unique_id() ->
    Rand = riak_cs_utils:sha(term_to_binary({make_ref(), now()})),
    <<I:160/integer>> = Rand,
    integer_to_list(I, 36).
