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

-module(riak_cs_sts).

-export([assume_role_with_saml/1
        ]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("kernel/include/logger.hrl").


-type assume_role_with_saml_error() :: expired_token
                                     | idp_rejected_claim
                                     | invalid_identity_token
                                     | malformed_policy_document
                                     | packed_policy_too_large
                                     | region_disabled.

-spec assume_role_with_saml(proplist:proplist()) -> {ok, maps:map()} | {error, assume_role_with_saml_error()}.
assume_role_with_saml(Specs) ->
    ?LOG_DEBUG("STUB assume_role_with_saml(~p)", [Specs]),
    lists:foldl(
      fun(StepF, {Args, State}) -> StepF(Args, State) end,
      {Specs, #{}},
      [fun validate_args/2,
       fun check_with_saml_provider/2,
       fun create_session_and_issue_temp_creds/2]).

validate_args(Specs, #{}) ->
    ?LOG_DEBUG("STUB"),
    ValidatedSpecs = Specs,
    {ValidatedSpecs, #{}}.

check_with_saml_provider({error, _} = PreviousStepFailed, #{}) ->
    PreviousStepFailed;
check_with_saml_provider(Specs, #{principal_arn := PrincipalArn,
                                  saml_assertion := SAMLAssertion,
                                  role_arn := RoleArn}) ->
    ?LOG_DEBUG("STUB PrincipalArn ~p, SAMLAssertion ~p, RoleArn ~p", [PrincipalArn, SAMLAssertion, RoleArn]),
    {Specs, #{}}.

create_session_and_issue_temp_creds({error, _} = PreviousStepFailed, #{}) ->
    PreviousStepFailed;
create_session_and_issue_temp_creds(Specs, #{duration_seconds := DurationSeconds}) ->
    ?LOG_DEBUG("STUB DurationSeconds ~p", [DurationSeconds]),
    
    {Specs, #{assumed_role_user => "AssumedRoleUser",
              audience => "Audience",
              credentials => "Credentials",
              issuer => "Issuer",
              name_qualifier => "NameQualifier",
              packed_policy_size => "PackedPolicySize",
              source_identity => "SourceIdentity",
              subject => "Subject",
              subject_type => "SubjectType"}}.


-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
