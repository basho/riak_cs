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

-module(riak_cs_temp_sessions).

-export([create/8,
         get/1, get/2,
         effective_policies/2,
         close_session/1
        ]).

-include("riak_cs.hrl").
-include_lib("kernel/include/logger.hrl").

-define(USER_ID_LENGTH, 16).  %% length("ARO456EXAMPLE789").


-spec create(role(), binary(), binary(), binary(), non_neg_integer(), binary(), [binary()], pid()) ->
          {ok, temp_session()} | {error, term()}.
create(?IAM_ROLE{role_id = RoleId,
                 role_name = RoleName} = Role,
       Subject, SourceIdentity, Email, DurationSeconds, InlinePolicy_, PolicyArns, Pbc) ->

    UserId = riak_cs_aws_utils:make_id(?USER_ID_LENGTH, ?USER_ID_PREFIX),
    {KeyId, AccessKey} = riak_cs_aws_utils:generate_access_creds(UserId),
    CanonicalId = riak_cs_aws_utils:generate_canonical_id(KeyId),

    SessionName = riak_cs_aws_utils:make_id(16),
    Arn = riak_cs_aws_utils:make_assumed_role_user_arn(RoleName, SessionName),
    AssumedRoleUser = #assumed_role_user{arn = Arn,
                                         assumed_role_id = <<RoleId/binary, $:, SessionName/binary>>},

    InlinePolicy = case InlinePolicy_ of
                       undefined ->
                           undefined;
                       _ ->
                           base64:decode(InlinePolicy_)
                   end,
    Session = #temp_session{assumed_role_user = AssumedRoleUser,
                            role = Role,
                            credentials = #credentials{access_key_id = KeyId,
                                                       secret_access_key = AccessKey,
                                                       expiration = os:system_time(second) + DurationSeconds,
                                                       session_token = make_session_token()},
                            duration_seconds = DurationSeconds,
                            subject = Subject,
                            source_identity = SourceIdentity,
                            email = Email,
                            user_id = UserId,
                            canonical_id = CanonicalId,
                            inline_policy = InlinePolicy,
                            session_policies = PolicyArns},

    %% rather than using an arn for key, consistently with all other
    %% things in IAM, let's use access_key_id here, to make it easier
    %% to check for existing assumed role sessions from
    %% riak_cs_user:get_user/2 where we only have the key_id.
    Obj = riakc_obj:new(?TEMP_SESSIONS_BUCKET, KeyId, term_to_binary(Session)),
    case riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS) of
        ok ->
            logger:info("Opened new temp session for user ~s with key_id ~s", [UserId, KeyId]),
            {ok, _Tref} = timer:apply_after(DurationSeconds * 1000,
                                            ?MODULE, close_session, [KeyId]),
            {ok, Session};
        {error, Reason} = ER ->
            logger:error("Failed to save temp session: ~p", [Reason]),
            ER
    end.


-spec effective_policies(#temp_session{}, pid()) -> {[amz_policy()], PermissionsBoundary::amz_policy()}.
effective_policies(#temp_session{inline_policy = InlinePolicy,
                                 session_policies = SessionPolicies,
                                 role = ?IAM_ROLE{assume_role_policy_document = AssumeRolePolicyDocument,
                                                  permissions_boundary = PermissionsBoundary,
                                                  attached_policies = RoleAttachedPolicies}},
                   Pbc) ->
    Policies = lists:flatten(
                 [maybe_include(AssumeRolePolicyDocument)
                 | [maybe_include(InlinePolicy)
                   | riak_cs_iam:express_policies(SessionPolicies ++ RoleAttachedPolicies, Pbc)]]),
    ?LOG_DEBUG("Effective policies: ~p", [Policies]),
    {Policies, PermissionsBoundary}.

maybe_include(undefined) -> [];
maybe_include(A) -> A.


-spec get(binary(), pid()) -> {ok, temp_session()} | {error, term()}.
get(KeyId) ->
    {ok, Pbc} = riak_cs_utils:riak_connection(),
    try
        get(KeyId, Pbc)
    after
        riak_cs_utils:close_riak_connection(Pbc)
    end.

get(KeyId, Pbc) ->
    case riakc_pb_socket:get(Pbc, ?TEMP_SESSIONS_BUCKET, KeyId) of
        {ok, Obj} ->
            session_from_riakc_obj(Obj);
        ER ->
            ER
    end.
session_from_riakc_obj(Obj) ->
    case [binary_to_term(Value) || Value <- riakc_obj:get_values(Obj),
                                   Value /= <<>>] of
        [] ->
            {error, notfound};
        [S] ->
            {ok, S};
        [S|_] = VV ->
            logger:warning("Temp session object for user ~s has ~b siblings",
                           [S#temp_session.user_id, length(VV)]),
            {ok, S}
    end.

make_session_token() ->
    ?LOG_DEBUG("STUB"),
    
    riak_cs_aws_utils:make_id(80).

close_session(Id) ->
    {ok, Pbc} = riak_cs_utils:riak_connection(),
    try
        case riakc_pb_socket:delete(Pbc, ?TEMP_SESSIONS_BUCKET, Id, ?CONSISTENT_DELETE_OPTIONS) of
            ok ->
                logger:info("Deleted temp session for user with key_id ~s", [Id]),
                ok;
            {error, Reason} ->
                logger:warning("Failed to delete temp session with key_id ~s: ~p", [Id, Reason]),
                still_ok
        end
    after
        riak_cs_utils:close_riak_connection(Pbc)
    end.

