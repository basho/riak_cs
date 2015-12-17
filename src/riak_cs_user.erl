%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_cs user related functions

-module(riak_cs_user).

%% Public API
-export([
         create_user/2,
         create_user/4,
         display_name/1,
         is_admin/1,
         get_user/2,
         get_user_by_index/3,
         from_riakc_obj/2,
         to_3tuple/1,
         save_user/3,
         update_key_secret/1,
         update_user/3,
         key_id/1,
         fetch_user_keys/1
        ]).

-include("riak_cs.hrl").
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a new Riak CS user
-spec create_user(string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email) ->
    {KeyId, Secret} = generate_access_creds(Email),
    create_user(Name, Email, KeyId, Secret).

%% @doc Create a new Riak CS user
-spec create_user(string(), string(), string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email, KeyId, Secret) ->
    case validate_email(Email) of
        ok ->
            CanonicalId = generate_canonical_id(KeyId),
            User = user_record(Name, Email, KeyId, Secret, CanonicalId),
            create_credentialed_user(riak_cs_config:admin_creds(), User);
        {error, _Reason}=Error ->
            Error
    end.

-spec create_credentialed_user({ok, {term(), term()}}, rcs_user()) ->
                                      {ok, rcs_user()} | {error, term()}.
create_credentialed_user({ok, AdminCreds}, User) ->
    {StIp, StPort, StSSL} = riak_cs_utils:stanchion_data(),
    %% Make a call to the user request serialization service.
    StatsKey = [velvet, create_user],
    _ = riak_cs_stats:inflow(StatsKey),
    StartTime = os:timestamp(),
    Result = velvet:create_user(StIp,
                                StPort,
                                "application/json",
                                binary_to_list(riak_cs_json:to_json(User)),
                                [{ssl, StSSL}, {auth_creds, AdminCreds}]),
    _ = riak_cs_stats:update_with_start(StatsKey, StartTime, Result),
    handle_create_user(Result, User).

handle_create_user(ok, User) ->
    {ok, User};
handle_create_user({error, {error_status, _, _, ErrorDoc}}, _User) ->
    case riak_cs_config:api() of
        s3 ->
            riak_cs_s3_response:error_response(ErrorDoc);
        oos ->
            {error, ErrorDoc}
    end;
handle_create_user({error, _}=Error, _User) ->
    Error.

handle_update_user(ok, User, UserObj, RcPid) ->
    _ = save_user(User, UserObj, RcPid),
    {ok, User};
handle_update_user({error, {error_status, _, _, ErrorDoc}}, _User, _, _) ->
    case riak_cs_config:api() of
        s3 ->
            riak_cs_s3_response:error_response(ErrorDoc);
        oos ->
            {error, ErrorDoc}
    end;
handle_update_user({error, _}=Error, _User, _, _) ->
    Error.

%% @doc Update a Riak CS user record
-spec update_user(rcs_user(), riakc_obj:riakc_obj(), riak_client()) ->
                         {ok, rcs_user()} | {error, term()}.
update_user(User, UserObj, RcPid) ->
    {StIp, StPort, StSSL} = riak_cs_utils:stanchion_data(),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Options = [{ssl, StSSL}, {auth_creds, AdminCreds}],
    StatsKey = [velvet, update_user],
    _ = riak_cs_stats:inflow(StatsKey),
    StartTime = os:timestamp(),
    %% Make a call to the user request serialization service.
    Result = velvet:update_user(StIp,
                                StPort,
                                "application/json",
                                User?RCS_USER.key_id,
                                binary_to_list(riak_cs_json:to_json(User)),
                                Options),
    _ = riak_cs_stats:update_with_start(StatsKey, StartTime, Result),
    handle_update_user(Result, User, UserObj, RcPid).

%% @doc Retrieve a Riak CS user's information based on their id string.
-spec get_user('undefined' | list(), riak_client()) -> {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, term()}.
get_user(undefined, _RcPid) ->
    {error, no_user_key};
get_user(KeyId, RcPid) ->
    %% Check for and resolve siblings to get a
    %% coherent view of the bucket ownership.
    BinKey = list_to_binary(KeyId),
    case riak_cs_riak_client:get_user(RcPid, BinKey) of
        {ok, {Obj, KeepDeletedBuckets}} ->
            {ok, {from_riakc_obj(Obj, KeepDeletedBuckets), Obj}};
        Error ->
            Error
    end.

-spec from_riakc_obj(riakc_obj:riakc_obj(), boolean()) -> rcs_user().
from_riakc_obj(Obj, KeepDeletedBuckets) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            Value = binary_to_term(riakc_obj:get_value(Obj)),
            User = update_user_record(Value),
            Buckets = riak_cs_bucket:resolve_buckets([Value], [], KeepDeletedBuckets),
            User?RCS_USER{buckets=Buckets};
        0 ->
            error(no_value);
        N ->
            Values = [binary_to_term(Value) ||
                         Value <- riakc_obj:get_values(Obj),
                         Value /= <<>>  % tombstone
                     ],
            User = update_user_record(hd(Values)),

            KeyId = User?RCS_USER.key_id,
            _ = lager:warning("User object of '~s' has ~p siblings", [KeyId, N]),

            Buckets = riak_cs_bucket:resolve_buckets(Values, [], KeepDeletedBuckets),
            User?RCS_USER{buckets=Buckets}
    end.

%% @doc Retrieve a Riak CS user's information based on their
%% canonical id string or email.
%% @TODO May want to use mapreduce job for this.
-spec get_user_by_index(binary(), binary(), riak_client()) ->
                               {ok, {rcs_user(), term()}} |
                               {error, term()}.
get_user_by_index(Index, Value, RcPid) ->
    case get_user_index(Index, Value, RcPid) of
        {ok, KeyId} ->
            get_user(KeyId, RcPid);
        {error, _}=Error1 ->
            Error1
    end.

%% @doc Query `Index' for `Value' in the users bucket.
-spec get_user_index(binary(), binary(), riak_client()) -> {ok, string()} | {error, term()}.
get_user_index(Index, Value, RcPid) ->
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    %% TODO: Does adding max_results=1 help latency or load to riak cluster?
    case riak_cs_pbc:get_index_eq(MasterPbc, ?USER_BUCKET, Index, Value,
                                  [riakc, get_user_by_index]) of
        {ok, ?INDEX_RESULTS{keys=[]}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys=[Key | _]}} ->
            {ok, binary_to_list(Key)};
        {error, Reason}=Error ->
            _ = lager:warning("Error occurred trying to query ~p in user"
                              "index ~p. Reason: ~p",
                              [Value, Index, Reason]),
            Error
    end.

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user()) -> boolean().
is_admin(User) ->
    is_admin(User, riak_cs_config:admin_creds()).

-spec to_3tuple(rcs_user()) -> acl_owner().
to_3tuple(U) ->
    %% acl_owner3: {display name, canonical id, key id}
    {U?RCS_USER.display_name, U?RCS_USER.canonical_id,
     U?RCS_USER.key_id}.

%% @doc Save information about a Riak CS user
-spec save_user(rcs_user(), riakc_obj:riakc_obj(), riak_client()) -> ok | {error, term()}.
save_user(User, UserObj, RcPid) ->
    riak_cs_riak_client:save_user(RcPid, User, UserObj).


%% @doc Generate a new `key_secret' for a user record.
-spec update_key_secret(rcs_user()) -> rcs_user().
update_key_secret(User=?RCS_USER{email=Email,
                                 key_id=KeyId}) ->
    EmailBin = list_to_binary(Email),
    User?RCS_USER{key_secret=generate_secret(EmailBin, KeyId)}.

%% @doc Strip off the user name portion of an email address
-spec display_name(string()) -> string().
display_name(Email) ->
    Index = string:chr(Email, $@),
    string:sub_string(Email, 1, Index-1).

%% @doc Grab the whole list of Riak CS user keys.
-spec fetch_user_keys(riak_client()) -> {ok, [binary()]} | {error, term()}.
fetch_user_keys(RcPid) ->
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    Timeout = riak_cs_config:list_keys_list_users_timeout(),
    riak_cs_pbc:list_keys(MasterPbc, ?USER_BUCKET, Timeout,
                          [riakc, list_all_user_keys]).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Generate a new set of access credentials for user.
-spec generate_access_creds(string()) -> {iodata(), iodata()}.
generate_access_creds(UserId) ->
    UserBin = list_to_binary(UserId),
    KeyId = generate_key(UserBin),
    Secret = generate_secret(UserBin, KeyId),
    {KeyId, Secret}.

%% @doc Generate the canonical id for a user.
-spec generate_canonical_id(string()) -> string().
generate_canonical_id(KeyID) ->
    Bytes = 16,
    Id1 = riak_cs_utils:md5(KeyID),
    Id2 = riak_cs_utils:md5(druuid:v4()),
    riak_cs_utils:binary_to_hexlist(
      iolist_to_binary(<< Id1:Bytes/binary,
                          Id2:Bytes/binary >>)).

%% @doc Generate an access key for a user
-spec generate_key(binary()) -> [byte()].
generate_key(UserName) ->
    Ctx = crypto:hmac_init(sha, UserName),
    Ctx1 = crypto:hmac_update(Ctx, druuid:v4()),
    Key = crypto:hmac_final_n(Ctx1, 15),
    string:to_upper(base64url:encode_to_string(Key)).

%% @doc Generate a secret access token for a user
-spec generate_secret(binary(), string()) -> iodata().
generate_secret(UserName, Key) ->
    Bytes = 14,
    Ctx = crypto:hmac_init(sha, UserName),
    Ctx1 = crypto:hmac_update(Ctx, list_to_binary(Key)),
    SecretPart1 = crypto:hmac_final_n(Ctx1, Bytes),
    Ctx2 = crypto:hmac_init(sha, UserName),
    Ctx3 = crypto:hmac_update(Ctx2, druuid:v4()),
    SecretPart2 = crypto:hmac_final_n(Ctx3, Bytes),
    base64url:encode_to_string(
      iolist_to_binary(<< SecretPart1:Bytes/binary,
                          SecretPart2:Bytes/binary >>)).

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user(), {ok, {string(), string()}} |
               {error, term()}) -> boolean().
is_admin(?RCS_USER{key_id=KeyId, key_secret=KeySecret},
         {ok, {KeyId, KeySecret}}) ->
    true;
is_admin(_, _) ->
    false.

%% @doc Validate an email address.
-spec validate_email(string()) -> ok | {error, term()}.
validate_email(EmailAddr) ->
    %% @TODO More robust email address validation
    case string:chr(EmailAddr, $@) of
        0 ->
            {error, invalid_email_address};
        _ ->
            ok
    end.

%% @doc Update a user record from a previous version if necessary.
-spec update_user_record(rcs_user()) -> rcs_user().
update_user_record(User=?RCS_USER{buckets=Buckets}) ->
    User?RCS_USER{buckets=[riak_cs_bucket:update_bucket_record(Bucket) ||
                              Bucket <- Buckets]};
update_user_record(User=#moss_user_v1{}) ->
    ?RCS_USER{name=User#moss_user_v1.name,
              display_name=User#moss_user_v1.display_name,
              email=User#moss_user_v1.email,
              key_id=User#moss_user_v1.key_id,
              key_secret=User#moss_user_v1.key_secret,
              canonical_id=User#moss_user_v1.canonical_id,
              buckets=[riak_cs_bucket:update_bucket_record(Bucket) ||
                          Bucket <- User#moss_user_v1.buckets]}.

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string(), string(), string(), string()) -> rcs_user().
user_record(Name, Email, KeyId, Secret, CanonicalId) ->
    user_record(Name, Email, KeyId, Secret, CanonicalId, []).

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string(), string(), string(), string(), [cs_bucket()]) ->
                         rcs_user().
user_record(Name, Email, KeyId, Secret, CanonicalId, Buckets) ->
    DisplayName = display_name(Email),
    ?RCS_USER{name=Name,
              display_name=DisplayName,
              email=Email,
              key_id=KeyId,
              key_secret=Secret,
              canonical_id=CanonicalId,
              buckets=Buckets}.

key_id(?RCS_USER{key_id=KeyId}) ->
    KeyId.
