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

-export([assume_role_with_saml/2
        ]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("esaml/include/esaml.hrl").
-include_lib("kernel/include/logger.hrl").


-type assume_role_with_saml_error() :: expired_token
                                     | idp_rejected_claim
                                     | invalid_identity_token
                                     | malformed_policy_document
                                     | packed_policy_too_large
                                     | region_disabled.

-spec assume_role_with_saml(maps:map(), pid()) -> {ok, maps:map()} | {error, assume_role_with_saml_error()}.
assume_role_with_saml(Specs, RcPid) ->
    Res = lists:foldl(
            fun(StepF, State) -> StepF(State) end,
            #{riak_client => RcPid,
              specs => Specs},
            [fun validate_args/1,
             fun check_role/1,
             fun parse_saml_assertion_claims/1,
             fun check_with_saml_provider/1,
             fun create_session_and_issue_temp_creds/1]),
    case Res of
        #{status := ok} ->
            {ok, Res};
        #{status := NotOk} ->
            NotOk
    end.

validate_args(#{specs := Specs} = State) ->
    case lists:foldl(fun(_Fun, {error, _} = E) -> E;
                        (Fun, ok) -> Fun(Specs) end,
                     ok,
                     [fun validate_duration_seconds/1,
                      fun validate_policy/1,
                      fun validate_policy_arns/1,
                      fun validate_principal_arn/1,
                      fun validate_role_arn/1,
                      fun validate_saml_assertion/1]) of
        ok ->
            State#{status => ok};
        {error, Reason} ->
            State#{status => {error, Reason}}
    end.
validate_duration_seconds(#{duration_seconds := A}) ->
    case A >= 900 andalso A =< 43200 of
        true ->
            ok;
        false ->
            logger:warning("Unacceptable value for DurationSeconds: ~p", [A]),
            {error, invalid_parameter_value}
    end;
validate_duration_seconds(#{}) ->
   ok.

validate_policy(#{policy := A}) ->
    case nomatch /= re:run(A, "[\u0009\u000A\u000D\u0020-\u00FF]+")
        andalso size(A) >= 1
        andalso size(A) =< 2048 of
        true ->
            ok;
        false ->
            logger:warning("Unacceptable value for Policy: ~p", [A]),
            {error, invalid_parameter_value}
    end;
validate_policy(#{}) ->
    ok.

validate_policy_arns(#{policy_arns := AA}) ->
    case length(AA) =< 10 andalso
        lists:all(fun is_valid_arn/1, AA) of
        true ->
            ok;
        false ->
            logger:warning("Invalid or too many PolicyArn members", []),
            {error, invalid_parameter_value}
    end;
validate_policy_arns(#{}) ->
    ok.

validate_principal_arn(#{principal_arn := A}) ->
    case is_valid_arn(A)
        andalso size(A) >= 20
        andalso size(A) =< 2048 of
        true ->
            ok;
        false ->
            logger:warning("Unacceptable value for PrincipalArn: ~p", [A]),
            {error, invalid_parameter_value}
    end;
validate_principal_arn(#{}) ->
    logger:warning("Missing PrincipalArn parameter"),
    {error, missing_parameter}.

validate_role_arn(#{role_arn := A}) ->
    case is_valid_arn(A)
        andalso size(A) >= 20
        andalso size(A) =< 2048 of
        true ->
            ok;
        false ->
            logger:warning("Unacceptable value for RoleArn: ~p", [A]),
            {error, invalid_parameter_value}
    end.
validate_saml_assertion(#{saml_assertion := A}) ->
    case size(A) >= 4 andalso size(A) =< 100000 of
        true ->
            ok;
        false ->
            logger:warning("Unacceptable value for SAMLAssertion: ~p", [A]),
            {error, invalid_parameter_value}
    end;
validate_saml_assertion(#{}) ->
    logger:warning("Missing SAMLAssertion parameter"),
    {error, missing_parameter}.



check_role(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
check_role(#{riak_client := RcPid,
             specs := #{role_arn := RoleArn}} = State) ->
    case riak_cs_iam:get_role(RoleArn, RcPid) of
        {ok, Role} ->
            State#{status => ok,
                   role => Role};
        ER ->
            State#{status => ER}
    end.


%% Since we have IdP metadata (from previous calls to
%% CreateSAMLProvider), we can, and will, use esaml decoding and
%% validation facilities to emulate a SP without actuallay talking to
%% the IdP described in the SAMLMetadataDocument.  This appears to be
%% sufficient except for the case of encrypted SAML assertions, for
%% which we need a private key which we currently have no way to
%% obtain via an IAM or STS call.

parse_saml_assertion_claims(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
parse_saml_assertion_claims(#{specs := #{request_id := RequestId,
                                         saml_assertion := SAMLAssertion_}} = State0) ->
    SAMLAssertion = base64:decode(SAMLAssertion_),
    {Doc, _} = xmerl_scan:string(binary_to_list(SAMLAssertion)),
    case esaml:decode_response(Doc) of
        {ok, #esaml_response{} = SAMLResponse} ->
            parse_saml_assertion_claims(
              SAMLResponse, State0#{response_doc => Doc});
        {error, Reason} ->
            logger:warning("Failed to parse Response document in request ~s: ~p", [RequestId, Reason]),
            State0#{status => {error, idp_rejected_claim}}
    end.
parse_saml_assertion_claims(#esaml_response{issuer = Issuer,
                                            assertion = Assertion,
                                            version = _Version}, State0) ->
    #esaml_assertion{subject = #esaml_subject{name = SubjectName,
                                              name_format = SubjectNameFormat},
                     attributes = Attributes} = Assertion,

    %% optional fields
    RoleSessionName =
        proplists:get_value("https://aws.amazon.com/SAML/Attributes/RoleSessionName",
                            Attributes),
    SessionDuration =
        proplists:get_value("https://aws.amazon.com/SAML/Attributes/SessionDuration",
                            Attributes),
    SourceIdentity =
        proplists:get_value("https://aws.amazon.com/SAML/Attributes/SourceIdentity",
                            Attributes),
    Role =
        proplists:get_value("https://aws.amazon.com/SAML/Attributes/Role",
                            Attributes, []),

    State1 = State0#{status => ok,
                     issuer => list_to_binary(Issuer),
                     subject => list_to_binary(SubjectName),
                     subject_type => list_to_binary(SubjectNameFormat)},
    maybe_update_state_with([{role_session_name, maybe_list_to_binary(RoleSessionName)},
                             {source_identity, maybe_list_to_binary(SourceIdentity)},
                             {session_duration, maybe_list_to_integer(SessionDuration)},
                             {claims_role, [maybe_list_to_binary(A) || A <- Role]}], State1).

maybe_list_to_integer(undefined) -> undefined;
maybe_list_to_integer(A) -> list_to_integer(A).
maybe_list_to_binary(undefined) -> undefined;
maybe_list_to_binary(A) -> list_to_binary(A).


check_with_saml_provider(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
check_with_saml_provider(#{riak_client := RcPid,
                           response_doc := ResponseDoc,
                           specs := #{request_id := RequestId,
                                      principal_arn := PrincipalArn}
                          } = State) ->
    case riak_cs_iam:get_saml_provider(PrincipalArn, RcPid) of
        {ok, SP} ->
            State#{status => validate_assertion(ResponseDoc, SP, RequestId)};
        {error, not_found} ->
            State#{status => {error, no_such_saml_provider}}
    end.

validate_assertion(ResponseDoc, ?IAM_SAML_PROVIDER{certificates = Certs,
                                                   consume_uri = ConsumeUri,
                                                   entity_id = EntityId},
                   RequestId) ->
    FPs = lists:flatten([FP || {signing, _, FP} <- Certs]),
    SP = #esaml_sp{trusted_fingerprints = FPs,
                   entity_id = binary_to_list(EntityId),
                   idp_signs_assertions = false,
                   idp_signs_envelopes = false,
                   consume_uri = ConsumeUri},
    case esaml_sp:validate_assertion(ResponseDoc, SP) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:warning("Failed to validate SAML Assertion for AssumeRoleWithSAML call on request ~s: ~p", [RequestId, Reason]),
            {error, idp_rejected_claim}
    end.

create_session_and_issue_temp_creds(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
create_session_and_issue_temp_creds(#{specs := #{duration_seconds := DurationSeconds} = Specs,
                                      role := Role,
                                      subject := Subject,
                                      subject_type := SubjectType,
                                      riak_client := RcPid} = State) ->
    SourceIdentity = maps:get(source_identity, State, <<>>),
    InlinePolicy = maps:get(policy, Specs, undefined),
    PolicyArns = maps:get(policy_arns, Specs, []),

    case riak_cs_temp_sessions:create(
           Role, Subject, DurationSeconds, InlinePolicy, PolicyArns, RcPid) of
        {ok, #temp_session{assumed_role_user = AssumedRoleUser,
                           credentials = Credentials}} ->
            State#{status => ok,
                   assumed_role_user => AssumedRoleUser,
                   audience => <<"https://signin.aws.amazon.com/saml">>,
                   credentials => Credentials,
                   name_qualifier => <<"Base64 ( SHA1 ( \"https://example.com/saml\" + \"123456789012\" + \"/MySAMLIdP\" ) )">>,
                   packed_policy_size => 6,
                   subject => Subject,
                   subject_type => SubjectType,
                   source_identity => SourceIdentity};
        ER ->
            State#{status => ER}
    end.


maybe_update_state_with([], State) ->
    State;
maybe_update_state_with([{_, undefined}|Rest], State) ->
    maybe_update_state_with(Rest, State);
maybe_update_state_with([{P, V}|Rest], State) ->
    maybe_update_state_with(Rest, maps:put(P, V, State)).




is_valid_arn(A) ->
    nomatch /= re:run(A, "[\u0009\u000A\u000D\u0020-\u007E\u0085\u00A0-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]+").


-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
