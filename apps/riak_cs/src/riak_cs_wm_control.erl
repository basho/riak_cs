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

-module(riak_cs_wm_control).

%% webmachine resource exports
-export([init/1,
         allowed_methods/2,
         content_types_provided/2,
         service_available/2,
         produce_html/2
        ]).
-ignore_xref([init/1,
              encodings_provided/2,
              content_types_provided/2,
              service_available/2,
              produce_html/2
             ]).

-include("riak_cs.hrl").

init(Props) ->
    AuthBypass = not proplists:get_value(admin_auth_enabled, Props),
    {ok, #rcs_web_context{auth_bypass = AuthBypass}}.

-spec allowed_methods(#wm_reqdata{}, #rcs_web_context{}) -> {[atom()], #wm_reqdata{}, #rcs_web_context{}}.
allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.

-spec content_types_provided(#wm_reqdata{}, #rcs_web_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_provided(_RD, _Context) ->
    [{"text/html", produce_html}].

service_available(RD, Ctx) ->
    {riak_cs_config:control_ui_enabled(), RD, Ctx}.

produce_html(RD, Ctx) ->
    SpecificPath = specific_path(wrq:path(RD)),
    {ok, Contents} = file:read_file(code:priv_dir(riak_cs) ++ "/control_ui/" ++ SpecificPath),
    {unicode:characters_to_list(
       subst_env_vars(Contents, SpecificPath), unicode), RD, Ctx}.

specific_path("/riak-cs/ui" ++ MaybeSingleSlash) when MaybeSingleSlash == "/";
                                                      MaybeSingleSlash == [] ->
    "index.js";
specific_path("/riak-cs/ui/" ++ OtherAsset) ->
    OtherAsset.


subst_env_vars(Body, "index.js") ->
    %% these should be collected from user; passing these as flags pending development
    Proto = proto(),
    {ok, {Host, Port}} = application:get_env(riak_cs, listener),
    {ok, {AdminKeyId, AdminKeySecret}} = riak_cs_config:admin_creds(),
    lists:foldl(
      fun({P, R}, S) -> re:replace(S, P, R) end,
      Body,
      [{<<"process\.env\.CS_URL">>, iolist_to_binary([Proto, "://", Host, $:, integer_to_binary(Port)])},
       {<<"process\.env\.CS_ADMIN_KEY">>, AdminKeyId},
       {<<"process\.env\.CS_ADMIN_SECRET">>, AdminKeySecret}]);
subst_env_vars(A, _) ->
    A.


proto() ->
    case application:get_value(ssl) of
        undefined ->
            <<"http">>;
        _ ->
            <<"https">>
    end.
