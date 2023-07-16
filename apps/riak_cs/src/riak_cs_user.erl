%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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
         create_user/3,
         create_user/5,
         display_name/1,
         is_admin/1,
         get_user/2,
         from_riakc_obj/2,
         to_3tuple/1,
         update_key_secret/1,
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
-spec create_user(binary(), binary()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email) ->
    create_user(Name, Email, #{}).

-spec create_user(binary(), binary(), maps:map()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email, IAMExtra) ->
    {KeyId, Secret} = riak_cs_aws_utils:generate_access_creds(Email),
    create_user(Name, Email, KeyId, Secret, IAMExtra).

%% @doc Create a new Riak CS/IAM user
-spec create_user(binary(), binary(), binary(), binary(), maps:map()) ->
          {ok, rcs_user()} | {error, term()}.
create_user(Name, Email, KeyId, Secret, IAMExtra) ->
    case validate_email(Email) of
        ok ->
            CanonicalId = riak_cs_aws_utils:generate_canonical_id(KeyId),
            DisplayName = display_name(Email),
            Path = maps:get(path, IAMExtra, <<"/">>),
            Arn = riak_cs_aws_utils:make_user_arn(Name, Path),
            User = ?RCS_USER{arn = Arn,
                             name = Name,
                             path = Path,
                             permissions_boundary = maps:get(permissions_boundary, IAMExtra, undefined),
                             tags = [exprec:frommap_tag(T) || T <- maps:get(tags, IAMExtra, [])],
                             display_name = DisplayName,
                             email = Email,
                             key_id = KeyId,
                             key_secret = Secret,
                             canonical_id = CanonicalId},
            create_credentialed_user(User);
        {error, _Reason} = Error ->
            Error
    end.

create_credentialed_user(User) ->
    %% Make a call to the user request serialization service.
    {ok, AdminCreds} = riak_cs_config:admin_creds(),
    StatsKey = [velvet, create_user],
    _ = riak_cs_stats:inflow(StatsKey),
    StartTime = os:system_time(millisecond),
    Result = velvet:create_user("application/json",
                                riak_cs_json:to_json(User),
                                [{auth_creds, AdminCreds}]),
    _ = riak_cs_stats:update_with_start(StatsKey, StartTime, Result),
    handle_create_user(Result, User).

handle_create_user(ok, User) ->
    {ok, User};
handle_create_user({error, _} = Error, _User) ->
    Error.


%% @doc Retrieve a Riak CS user's information based on their id string.
-spec get_user(undefined | binary(), riak_client()) ->
          {ok, {rcs_user(), undefined | riakc_obj:riakc_obj()}} | {error, no_user_key | riak_obj_error()}.
get_user(undefined, _) ->
    {error, no_user_key};
get_user(KeyId, RcPid) ->
    case riak_cs_temp_sessions:get(KeyId) of
        {ok, #temp_session{assumed_role_user = #assumed_role_user{arn = AssumedRoleUserArn},
                           credentials = #credentials{secret_access_key = SecretKey},
                           canonical_id = CanonicalId,
                           subject = Subject,
                           source_identity = SourceIdentity,
                           email = Email}} ->
            {ok, {?RCS_USER{arn = AssumedRoleUserArn,
                            attached_policies = [],
                            name = Subject,
                            email = select_email([SourceIdentity, Email]),
                            display_name = Subject,
                            canonical_id = CanonicalId,
                            key_id = KeyId,
                            key_secret = SecretKey,
                            buckets = []},
                  _UserObject = undefined}};
        {error, notfound} ->
            get_cs_user(KeyId, RcPid)
    end.


get_cs_user(KeyId, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),
    riak_cs_iam:find_user(#{key_id => KeyId}, Pbc).


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

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user()) -> boolean().
is_admin(User) ->
    is_admin(User, riak_cs_config:admin_creds()).
is_admin(?RCS_USER{key_id = KeyId, key_secret = KeySecret},
         {ok, {KeyId, KeySecret}}) ->
    true;
is_admin(_, _) ->
    false.

-spec to_3tuple(rcs_user()) -> acl_owner().
to_3tuple(?RCS_USER{display_name = DisplayName,
                    canonical_id = CanonicalId,
                    key_id = KeyId}) ->
    %% acl_owner3: {display name, canonical id, key id}
    #{display_name => DisplayName,
      canonical_id => CanonicalId,
      key_id => KeyId}.


%% @doc Generate a new `key_secret' for a user record.
-spec update_key_secret(rcs_user()) -> rcs_user().
update_key_secret(User = ?RCS_USER{email = Email,
                                   key_id = KeyId}) ->
    User?RCS_USER{key_secret = riak_cs_aws_utils:generate_secret(Email, KeyId)}.

%% @doc Strip off the user name portion of an email address
-spec display_name(binary()) -> binary().
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

validate_email(EmailAddr) ->
    case re:run(EmailAddr, "^[a-z0-9]+[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,17}$", [caseless]) of
        nomatch ->
            {error, invalid_email_address};
        _ ->
            ok
    end.

select_email([]) ->
    <<"unspecified">>;
select_email([A|T]) ->
    case validate_email(A) of
        ok ->
            A;
        _ ->
            select_email(T)
    end.

update_user_record(User = ?RCS_USER{}) ->
    User;
update_user_record(#moss_user_v1{name = Name,
                                 display_name = DisplayName,
                                 email = Email,
                                 key_id = KeyId,
                                 key_secret = KeySecret,
                                 canonical_id = CanonicalId,
                                 buckets = Buckets}) ->
    ?RCS_USER{arn = riak_cs_aws_utils:make_user_arn(Name, <<"/">>),
              name = Name,
              display_name = DisplayName,
              email = Email,
              key_id = KeyId,
              key_secret = KeySecret,
              canonical_id = CanonicalId,
              buckets = Buckets}.
