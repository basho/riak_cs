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

-module(riak_cs_aws_utils).

-export([make_id/1,
         make_id/2,
         make_role_arn/2,
         make_provider_arn/1,
         make_assumed_role_user_arn/2,
         generate_access_creds/1,
         generate_secret/2,
         generate_canonical_id/1
        ]).

-include("aws_api.hrl").
-include_lib("kernel/include/logger.hrl").

-define(AWS_ID_CHARSET, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").
-define(AWS_ID_EXT_CHARSET, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_").
-define(ACCESS_KEY_LENGTH, 20).

-spec make_id(non_neg_integer(), string()) -> binary().
make_id(Length) ->
    make_id(Length, []).
make_id(Length, Prefix) ->
    make_id(Length, Prefix, ?AWS_ID_CHARSET).
make_id(Length, Prefix, CharSet) ->
    iolist_to_binary([Prefix, fill(Length - length(Prefix), [], CharSet)]).

make_role_arn(Name, Path) ->
    iolist_to_binary(["arn:aws:iam::", fill(12, [], "0123456789"), ":role", Path, $/, Name]).

make_provider_arn(Name) ->
    iolist_to_binary(["arn:aws:iam::", fill(12, "", "0123456789"), ":saml-provider/", Name]).

make_assumed_role_user_arn(RoleName, SessionName) ->
    iolist_to_binary(["arn:aws:sts::", fill(12, "", "0123456789"), ":assumed-role/", RoleName, $/, SessionName]).


%% @doc Generate a new set of access credentials for user.
-spec generate_access_creds(binary()) -> {iodata(), iodata()}.
generate_access_creds(UserId) ->
    KeyId = generate_key(UserId),
    Secret = generate_secret(UserId, KeyId),
    {KeyId, Secret}.

%% @doc Generate the canonical id for a user.
-spec generate_canonical_id(string()) -> string().
generate_canonical_id(KeyID) ->
    Bytes = 16,
    Id1 = riak_cs_utils:md5(KeyID),
    Id2 = riak_cs_utils:md5(uuid:get_v4()),
    riak_cs_utils:binary_to_hexlist(
      iolist_to_binary(<< Id1:Bytes/binary,
                          Id2:Bytes/binary >>)).

%% @doc Generate an access key for a user
-spec generate_key(binary()) -> [byte()].
generate_key(UserName) ->
    Ctx = crypto:mac_init(hmac, sha, UserName),
    Ctx1 = crypto:mac_update(Ctx, uuid:get_v4()),
    Key = crypto:mac_finalN(Ctx1, 15),
    string:to_upper(base64url:encode_to_string(Key)).

%% @doc Generate a secret access token for a user
-spec generate_secret(binary(), string()) -> iodata().
generate_secret(UserName, Key) ->
    Bytes = 14,
    Ctx = crypto:mac_init(hmac, sha, UserName),
    Ctx1 = crypto:mac_update(Ctx, list_to_binary(Key)),
    SecretPart1 = crypto:mac_finalN(Ctx1, Bytes),
    Ctx2 = crypto:mac_init(hmac, sha, UserName),
    Ctx3 = crypto:mac_update(Ctx2, uuid:get_v4()),
    SecretPart2 = crypto:mac_finalN(Ctx3, Bytes),
    base64url:encode_to_string(
      iolist_to_binary(<< SecretPart1:Bytes/binary,
                          SecretPart2:Bytes/binary >>)).



fill(0, Q, _) ->
    Q;
fill(N, Q, CharSet) ->
    fill(N-1, [lists:nth(rand:uniform(length(CharSet)), CharSet) | Q], CharSet).

