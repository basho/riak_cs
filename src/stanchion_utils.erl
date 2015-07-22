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

%% @doc stanchion utility functions

-module(stanchion_utils).

%% Public API
-export([get_admin_creds/0, sha_mac/2, md5/1]).

%% @doc Return the credentials of the admin user
-spec get_admin_creds() -> {ok, {string(), string()}} | {error, term()}.
get_admin_creds() ->
    case application:get_env(stanchion, admin_key) of
        {ok, KeyId} ->
            case application:get_env(stanchion, admin_secret) of
                {ok, Secret} ->
                    {ok, {KeyId, Secret}};
                undefined ->
                    _ = lager:warning("The admin user's secret has not been defined."),
                    {error, secret_undefined}
            end;
        undefined ->
            _ = lager:warning("The admin user's key id has not been defined."),
            {error, key_id_undefined}
    end.

-ifdef(new_hash).
sha_mac(KeyData, STS) ->
    crypto:hmac(sha, KeyData, STS).
md5(Bin) -> crypto:hash(md5, Bin).
-else.
sha_mac(KeyData, STS) ->
    crypto:sha_mac(KeyData, STS).
md5(Bin) -> crypto:md5(Bin).
-endif.
