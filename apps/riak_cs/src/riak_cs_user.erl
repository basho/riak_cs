%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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
-export([create_user/2,
         create_user/4,
         display_name/1,
         is_admin/1,
         get_user/2,
         get_user_by_index/3,
         from_riakc_obj/2,
         to_3tuple/1,
         update_key_secret/1,
         update_user/3,
         key_id/1,
         fetch_user_keys/1
        ]).

-include("riak_cs.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a new Riak CS user
-spec create_user(string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email) ->
    {KeyId, Secret} = riak_cs_aws_utils:generate_access_creds(Email),
    create_user(Name, Email, KeyId, Secret).

%% @doc Create a new Riak CS user
-spec create_user(string(), string(), string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email, KeyId, Secret) ->
    case validate_email(Email) of
        ok ->
            CanonicalId = riak_cs_aws_utils:generate_canonical_id(KeyId),
            User = user_record(Name, Email, KeyId, Secret, CanonicalId),
            create_credentialed_user(riak_cs_config:admin_creds(), User);
        {error, _Reason}=Error ->
            Error
    end.

create_credentialed_user({ok, AdminCreds}, User) ->
    %% Make a call to the user request serialization service.
    StatsKey = [velvet, create_user],
    _ = riak_cs_stats:inflow(StatsKey),
    StartTime = os:timestamp(),
    Result = velvet:create_user("application/json",
                                riak_cs_json:to_json(User),
                                [{auth_creds, AdminCreds}]),
    _ = riak_cs_stats:update_with_start(StatsKey, StartTime, Result),
    handle_create_user(Result, User).

handle_create_user(ok, User) ->
    {ok, User};
handle_create_user({error, {error_status, _, _, ErrorDoc}}, _User) ->
    case riak_cs_config:api() of
        s3 ->
            riak_cs_aws_response:error_response(ErrorDoc);
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
            riak_cs_aws_response:error_response(ErrorDoc);
        oos ->
            {error, ErrorDoc}
    end;
handle_update_user({error, _}=Error, _User, _, _) ->
    Error.

%% @doc Update a Riak CS user record
-spec update_user(rcs_user(), riakc_obj:riakc_obj(), riak_client()) ->
                         {ok, rcs_user()} | {error, term()}.
update_user(User, UserObj, RcPid) ->
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    Options = [{auth_creds, AdminCreds}],
    StatsKey = [velvet, update_user],
    _ = riak_cs_stats:inflow(StatsKey),
    StartTime = os:timestamp(),
    %% Make a call to the user request serialization service.
    Result = velvet:update_user("application/json",
                                User?RCS_USER.key_id,
                                binary_to_list(riak_cs_json:to_json(User)),
                                Options),
    _ = riak_cs_stats:update_with_start(StatsKey, StartTime, Result),
    handle_update_user(Result, User, UserObj, RcPid).

%% @doc Retrieve a Riak CS user's information based on their id string.
-spec get_user(undefined | iodata(), riak_client()) ->
          {ok, {rcs_user(), no_object | riakc_obj:riakc_obj()}} | {error, term()}.
get_user(undefined, _RcPid) ->
    {error, no_user_key};
get_user(KeyIdS, RcPid) ->
    KeyId = iolist_to_binary([KeyIdS]),
    case riak_cs_temp_sessions:get(KeyId) of
        {ok, #temp_session{credentials = #credentials{secret_access_key = SecretKey},
                           effective_policy = Policy,
                           canonical_id = CanonicalId,
                           subject = Subject} = _Session} ->
            {ok, {?RCS_USER{name = binary_to_list(Subject),
                            display_name = binary_to_list(Subject),
                            email = lists:flatten(io_lib:format("~s@some.idp", [Subject])),
                            canonical_id = binary_to_list(CanonicalId),
                            key_id = KeyId,
                            key_secret = binary_to_list(SecretKey),
                            buckets = extract_buckets_from_policy(Policy)}, no_object}};
        _ ->
            get_cs_user(KeyId, RcPid)
    end.

get_cs_user(KeyId, RcPid) ->
    %% Check for and resolve siblings to get a
    %% coherent view of the bucket ownership.
    case catch riak_cs_riak_client:get_user(RcPid, KeyId) of
        {ok, {Obj, KeepDeletedBuckets}} ->
            ?LOG_DEBUG("PPPPPPPPPPP Obj ~p", [Obj]),
            {ok, {from_riakc_obj(Obj, KeepDeletedBuckets), Obj}};
        Error ->
            Error
    end.

extract_buckets_from_policy(P) ->
    ?LOG_DEBUG("STUB ~p", [P]),
    
    [].

-spec from_riakc_obj(riakc_obj:riakc_obj(), boolean()) -> rcs_user().
from_riakc_obj(Obj, KeepDeletedBuckets) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            Value = binary_to_term(riakc_obj:get_value(Obj)),
            User = update_user_record(Value),
            Buckets = riak_cs_bucket:resolve_buckets([Value], [], KeepDeletedBuckets),
            User?RCS_USER{buckets = Buckets};
        0 ->
            error(no_value);
        N ->
            Values = [binary_to_term(Value) ||
                         Value <- riakc_obj:get_values(Obj),
                         Value /= <<>>  % tombstone
                     ],
            User = update_user_record(hd(Values)),

            KeyId = User?RCS_USER.key_id,
            logger:warning("User object of '~s' has ~p siblings", [KeyId, N]),

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
            logger:warning("Error occurred trying to query ~p in user index ~p. Reason: ~p",
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

save_user(User, UserObj, RcPid) ->
    riak_cs_riak_client:save_user(RcPid, User, UserObj).


%% @doc Generate a new `key_secret' for a user record.
-spec update_key_secret(rcs_user()) -> rcs_user().
update_key_secret(User=?RCS_USER{email=Email,
                                 key_id=KeyId}) ->
    EmailBin = list_to_binary(Email),
    User?RCS_USER{key_secret = riak_cs_aws_utils:generate_secret(EmailBin, KeyId)}.

%% @doc Strip off the user name portion of an email address
-spec display_name(binary()) -> string().
display_name(Email) ->
    hd(binary:split(Email, <<"@">>)).

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

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user(), {ok, {string(), string()}} |
               {error, term()}) -> boolean().
is_admin(?RCS_USER{key_id=KeyId, key_secret=KeySecret},
         {ok, {KeyId, KeySecret}}) ->
    true;
is_admin(_, _) ->
    false.

validate_email(EmailAddr) ->
    case re:run(EmailAddr, "^[a-z0-9]+[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,17}$", [caseless]) of
        nomatch ->
            {error, invalid_email_address};
        _ ->
            ok
    end.

update_user_record(User = ?RCS_USER{buckets = Buckets}) ->
    User?RCS_USER{buckets = [riak_cs_bucket:update_bucket_record(Bucket) || Bucket <- Buckets]};
update_user_record(User = #moss_user_v1{}) ->
    ?RCS_USER{name=User#moss_user_v1.name,
              display_name=User#moss_user_v1.display_name,
              email=User#moss_user_v1.email,
              key_id=User#moss_user_v1.key_id,
              key_secret=User#moss_user_v1.key_secret,
              canonical_id=User#moss_user_v1.canonical_id,
              buckets=[riak_cs_bucket:update_bucket_record(Bucket) ||
                          Bucket <- User#moss_user_v1.buckets]}.

user_record(Name, Email, KeyId, Secret, CanonicalId) ->
    user_record(Name, Email, KeyId, Secret, CanonicalId, []).

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
