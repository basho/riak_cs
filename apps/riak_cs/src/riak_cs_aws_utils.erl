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

-export([aws_service_from_url/1,
         make_id/1,
         make_id/2,
         make_unique_index_id/2,
         make_user_arn/2,
         make_role_arn/2,
         make_policy_arn/2,
         make_provider_arn/1,
         make_assumed_role_user_arn/2,
         generate_access_creds/1,
         generate_secret/2,
         generate_canonical_id/1
        ]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("kernel/include/logger.hrl").

-define(AWS_ID_CHARSET, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").
-define(AWS_ID_EXT_CHARSET, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_").
-define(ACCESS_KEY_LENGTH, 20).

-spec make_unique_index_id(iam_entity(), pid()) -> binary().
make_unique_index_id(role, Pbc) ->
    case try_unique_index_id(
           make_id(?IAM_ENTITY_ID_LENGTH, ?ROLE_ID_PREFIX),
           ?IAM_ROLE_BUCKET, ?ROLE_ID_INDEX, Pbc) of
        retry ->
            make_unique_index_id(role, Pbc);
        Id ->
            Id
    end;
make_unique_index_id(user, Pbc) ->
    case try_unique_index_id(
           make_id(?IAM_ENTITY_ID_LENGTH, ?USER_ID_PREFIX),
           ?IAM_USER_BUCKET, ?USER_ID_INDEX, Pbc) of
        retry ->
            make_unique_index_id(user, Pbc);
        Id ->
            Id
    end;
make_unique_index_id(policy, Pbc) ->
    case try_unique_index_id(
           make_id(?IAM_ENTITY_ID_LENGTH, ?POLICY_ID_PREFIX),
           ?IAM_POLICY_BUCKET, ?POLICY_ID_INDEX, Pbc) of
        retry ->
            make_unique_index_id(policy, Pbc);
        Id ->
            Id
    end.

try_unique_index_id(Id, Bucket, Index, Pbc) ->
    case riakc_pb_socket:get_index_eq(Pbc, Bucket, Index, Id) of
        {ok, ?INDEX_RESULTS{keys=[]}} ->
            Id;
        _ ->
            retry
    end.


-spec aws_service_from_url(string()) -> aws_service() | unsupported.
aws_service_from_url(Host) ->
    {Third, Fourth} =
        case lists:reverse(
               string:split(string:to_lower(Host), ".", all)) of
            ["com", "amazonaws", A] ->
                {A, ""};
            ["com", "amazonaws", A, B|_] ->
                {A, B};
            _ ->
                {"", ""}
        end,
    case aws_service(Third) of
        unsupported ->
            %% third item is a region, then fourth must be it
            aws_service(Fourth);
        Service ->
            Service
    end.

aws_service("s3") -> s3;
aws_service("iam") -> iam;
aws_service("sts") -> sts;
aws_service(_) ->  unsupported.



-spec make_id(non_neg_integer(), string()) -> binary().
make_id(Length) ->
    make_id(Length, []).
make_id(Length, Prefix) ->
    make_id(Length, Prefix, ?AWS_ID_CHARSET).
make_id(Length, Prefix, CharSet) ->
    iolist_to_binary([Prefix, fill(Length - length(Prefix), [], CharSet)]).

make_user_arn(Name, Path) ->
    single_slash(
      iolist_to_binary(["arn:aws:iam::", fill(12, [], "0123456789"), ":user", Path, $/, Name])).

make_role_arn(Name, Path) ->
    single_slash(
      iolist_to_binary(["arn:aws:iam::", fill(12, [], "0123456789"), ":role", Path, $/, Name])).

make_policy_arn(Name, Path) ->
    single_slash(
      iolist_to_binary(["arn:aws:iam::", fill(12, [], "0123456789"), ":policy", Path, $/, Name])).

make_provider_arn(Name) ->
    single_slash(
      iolist_to_binary(["arn:aws:iam::", fill(12, "", "0123456789"), ":saml-provider/", Name])).

make_assumed_role_user_arn(RoleName, SessionName) ->
    single_slash(
      iolist_to_binary(["arn:aws:sts::", fill(12, "", "0123456789"), ":assumed-role/", RoleName, $/, SessionName])).
single_slash(A) ->
    iolist_to_binary(re:replace(A, <<"/+">>, <<"/">>)).

%% @doc Generate a new set of access credentials for user.
-spec generate_access_creds(binary()) -> {iodata(), iodata()}.
generate_access_creds(UserId) ->
    KeyId = generate_key(UserId),
    Secret = generate_secret(UserId, KeyId),
    {KeyId, Secret}.

%% @doc Generate the canonical id for a user.
-spec generate_canonical_id(binary()) -> binary().
generate_canonical_id(KeyID) ->
    Bytes = 16,
    Id1 = riak_cs_utils:md5(KeyID),
    Id2 = riak_cs_utils:md5(uuid:get_v4()),
    list_to_binary(
      riak_cs_utils:binary_to_hexlist(
        iolist_to_binary(<< Id1:Bytes/binary,
                            Id2:Bytes/binary >>))).

%% @doc Generate an access key for a user
-spec generate_key(binary()) -> binary().
generate_key(UserName) ->
    Ctx = crypto:mac_init(hmac, sha, UserName),
    Ctx1 = crypto:mac_update(Ctx, uuid:get_v4()),
    Key = crypto:mac_finalN(Ctx1, 15),
    list_to_binary(string:to_upper(base64url:encode_to_string(Key))).

%% @doc Generate a secret access token for a user
-spec generate_secret(binary(), binary()) -> binary().
generate_secret(UserName, Key) ->
    Bytes = 14,
    Ctx = crypto:mac_init(hmac, sha, UserName),
    Ctx1 = crypto:mac_update(Ctx, Key),
    SecretPart1 = crypto:mac_finalN(Ctx1, Bytes),
    Ctx2 = crypto:mac_init(hmac, sha, UserName),
    Ctx3 = crypto:mac_update(Ctx2, uuid:get_v4()),
    SecretPart2 = crypto:mac_finalN(Ctx3, Bytes),
    base64url:encode(
      << SecretPart1:Bytes/binary,
         SecretPart2:Bytes/binary >>).


fill(0, Q, _) ->
    Q;
fill(N, Q, CharSet) ->
    fill(N-1, [lists:nth(rand:uniform(length(CharSet)), CharSet) | Q], CharSet).

