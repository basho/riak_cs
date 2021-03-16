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

%% @doc A collection helper functions for interacting with OpenStack
%% web services.

-module(riak_cs_oos_utils).

-include("oos_api.hrl").

-export([get_os_user_data/1,
         extract_user_data/1,
         user_ec2_creds/2]).

%% ===================================================================
%% Public API
%% ===================================================================

get_os_user_data(undefined) ->
    undefined;
get_os_user_data(UserId) ->
    %% GET /v2.0/users/<user-id>
    RequestURI = riak_cs_config:os_users_url() ++ UserId,
    RequestHeaders = [{"X-Auth-Token", riak_cs_config:os_admin_token()}],
    Response = httpc:request(get, {RequestURI, RequestHeaders}, [], []),
    handle_user_data_response(Response).

-type undef_or_string() :: undefined | string().
-spec extract_user_data(undefined | term()) -> {undef_or_string(),
                                                undef_or_string(),
                                                undef_or_string()}.
extract_user_data(undefined) ->
    {undefined, undefined, []};
extract_user_data(UserData) ->
    %% GET /v2.0/users/<user-id>/credentials/OS-EC2
    %% If the user does not have ec2 credentials for the tenant, then set `key_secret'
    %% to `undefined'.
    Path = [<<"user">>, {<<"name">>, <<"email">>, <<"tenantId">>}],
    user_binaries_to_lists(
      riak_cs_json:value_or_default(
        riak_cs_json:get(UserData, Path),
        {undefined, undefined, []})).

user_ec2_creds(undefined, _) ->
    {undefined, []};
user_ec2_creds(UserId, TenantId) ->
    %% GET /v2.0/users/<user-id>
    RequestURI = riak_cs_config:os_users_url() ++
        UserId ++
        "/credentials/OS-EC2",
    RequestHeaders = [{"X-Auth-Token", riak_cs_config:os_admin_token()}],
    Response = httpc:request(get, {RequestURI, RequestHeaders}, [], []),
    handle_ec2_creds_response(Response, TenantId).

%% ===================================================================
%% Internal functions
%% ===================================================================

user_binaries_to_lists({X, Y, Z}) ->
    {binary_to_list(X), binary_to_list(Y), binary_to_list(Z)}.

handle_user_data_response({ok, {{_HTTPVer, _Status, _StatusLine}, _, UserInfo}})
  when _Status >= 200, _Status =< 299 ->
    riak_cs_json:from_json(UserInfo);
handle_user_data_response({ok, {{_HTTPVer, _Status, _StatusLine}, _, _}}) ->
    %% @TODO Log error
    undefined;
handle_user_data_response({error, Reason}) ->
    _ = lager:warning("Error occurred requesting user data from keystone. Reason: ~p",
                  [Reason]),
    undefined.

handle_ec2_creds_response({ok, {{_HTTPVer, _Status, _StatusLine}, _, CredsInfo}}, TenantId)
  when _Status >= 200, _Status =< 299 ->
    CredsResult = riak_cs_json:from_json(CredsInfo),
    ec2_creds_for_tenant(CredsResult, TenantId);
handle_ec2_creds_response({ok, {{_HTTPVer, _Status, _StatusLine}, _, _}}, _) ->
    %% @TODO Log error
    {undefined, []};
handle_ec2_creds_response({error, Reason}, _) ->
    _ = lager:warning("Error occurred requesting user EC2 credentials from keystone. Reason: ~p",
                  [Reason]),
    {undefined, []}.

ec2_creds_for_tenant({error, decode_failed}, _) ->
    {undefined, []};
ec2_creds_for_tenant(Creds, TenantId) ->
    TenantQuery = {find, {key, <<"tenant_id">>, list_to_binary(TenantId)}},
    Path = [<<"credentials">>, TenantQuery, {<<"access">>, <<"secret">>}],
    case Res=riak_cs_json:value_or_default(
               riak_cs_json:get(Creds, Path), {undefined, []}) of
        {undefined, []} ->
            Res;
        {KeyBin, SecretBin} ->
            {binary_to_list(KeyBin), binary_to_list(SecretBin)}
    end.

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

-endif.
