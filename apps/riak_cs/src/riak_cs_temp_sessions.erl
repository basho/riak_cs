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

-export([create/6,
         get/1, get/2,
         effective_policies/1,
         close_session/1
        ]).

-include("riak_cs.hrl").
-include_lib("kernel/include/logger.hrl").

-define(USER_ID_LENGTH, 16).  %% length("ARO456EXAMPLE789").


-spec create(role(), binary(), non_neg_integer(), binary(), [binary()], pid()) ->
          {ok, temp_session()} | {error, term()}.
create(?IAM_ROLE{role_id = RoleId,
                 role_name = RoleName},
       Subject, DurationSeconds, InlinePolicy, PolicyArns, RcPid) ->
    {ok, Pbc} = riak_cs_riak_client:master_pbc(RcPid),

    UserId = riak_cs_aws_utils:make_id(?USER_ID_LENGTH, "AIDA"),
    {KeyIdS, AccessKeyS} = riak_cs_aws_utils:generate_access_creds(UserId),
    CanonicalIdS = riak_cs_aws_utils:generate_canonical_id(KeyIdS),
    {KeyId, AccessKey, CanonicalId} =
        {list_to_binary(KeyIdS),
         list_to_binary(AccessKeyS),
         list_to_binary(CanonicalIdS)},

    SessionName = riak_cs_aws_utils:make_id(16),
    AssumedRoleUser = #assumed_role_user{arn = riak_cs_aws_utils:make_assumed_role_user_arn(RoleName, SessionName),
                                         assumed_role_id = <<RoleId/binary, $:, SessionName/binary>>},

    Session = #temp_session{assumed_role_user = AssumedRoleUser,
                            credentials = #credentials{access_key_id = KeyId,
                                                       secret_access_key = AccessKey,
                                                       expiration = os:system_time(second) + DurationSeconds,
                                                       session_token = make_session_token()},
                            duration_seconds = DurationSeconds,
                            subject = Subject,
                            user_id = UserId,
                            canonical_id = CanonicalId,
                            inline_policy = InlinePolicy,
                            session_policies = PolicyArns},

    Obj = riakc_obj:new(?TEMP_SESSIONS_BUCKET, KeyId, term_to_binary(Session)),
    case riakc_pb_socket:put(Pbc, Obj, [{w, all}, {pw, all}]) of
        ok ->
            logger:info("Opened new temp session for user ~s with key_id ~s", [UserId, KeyId]),
            {ok, _Tref} = timer:apply_after(DurationSeconds * 1000,
                                            ?MODULE, close_session, [KeyId]),
            {ok, Session};
        {error, Reason} = ER ->
            logger:error("Failed to save temp session: ~p", [Reason]),
            ER
    end.


-spec effective_policies(#temp_session{}) -> [policy()].
effective_policies(#temp_session{inline_policy = InlinePolicy,
                                 session_policies = SessionPolicies,
                                 role_arn = RoleArn}) ->
    RoleAttachedPolicies =
        case riak_cs_iam:get_role(RoleArn) of
            {ok, ?IAM_ROLE{attached_policies = PP}} ->
                PP;
            _ ->
                logger:notice("Assumed role ~s deleted while temp session is still open", [RoleArn]),
                []
        end,
    lists:flatten(
      [InlinePolicy |
       [begin
            case riak_cs_iam:get_policy(Arn) of
                {ok, Policy} ->
                    Policy;
                _ ->
                    logger:notice("Managed policy ~p previously attached to role "
                                  "or referenced as session policy, is not available", [Arn]),
                    []
            end
        end || Arn <- SessionPolicies ++ RoleAttachedPolicies]]
     ).


-spec get(binary(), pid()) -> {ok, temp_session()} | {error, term()}.
get(Id) ->
    {ok, Pbc} = riak_cs_utils:riak_connection(),
    Res = get(Id, Pbc),
    riak_cs_utils:close_riak_connection(Pbc),
    Res.

get(Id, Pbc) ->
    case riakc_pb_socket:get(Pbc, ?TEMP_SESSIONS_BUCKET, Id) of
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
    _ = case riakc_pb_socket:get(Pbc, ?TEMP_SESSIONS_BUCKET, Id) of
            {ok, Obj0} ->
                Obj1 = riakc_obj:update_value(Obj0, ?DELETED_MARKER),
                case riakc_pb_socket:put(Pbc, Obj1, [{dw, all}]) of
                    ok ->
                        logger:info("Deleted temp session for user with key_id ~s", [Id]),
                        ok;
                    {error, Reason} ->
                        logger:warning("Failed to delete temp session with key_id ~s: ~p", [Id, Reason]),
                        still_ok
                end;
            {error, _r} ->
                nevermind
        end,
    riak_cs_utils:close_riak_connection(Pbc).

