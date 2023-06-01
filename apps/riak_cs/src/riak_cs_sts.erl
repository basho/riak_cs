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

parse_saml_assertion_claims(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
parse_saml_assertion_claims(#{specs := #{principal_arn := _PrincipalArn,
                                         saml_assertion := SAMLAssertion_}} = State0) ->
    SAMLAssertion = base64:decode(SAMLAssertion_),
    {#xmlElement{content = RootContent}, _} =
        xmerl_scan:string(binary_to_list(SAMLAssertion)),

    [#xmlElement{content = AssertionContent,
                 attributes = _AssertionAttrs}|_] =
        riak_cs_xml:find_elements('saml:Assertion', RootContent),
    [#xmlElement{content = SubjectContent}|_] =
        riak_cs_xml:find_elements('saml:Subject', AssertionContent),
    [#xmlElement{content = AttributeStatementContent}|_] =
        riak_cs_xml:find_elements('saml:AttributeStatement', AssertionContent),
    [#xmlElement{content = NameIDContent,
                 attributes = NameIDAttrs}|_] =
        riak_cs_xml:find_elements('saml:NameID', SubjectContent),
    [#xmlText{value = NameID}|_] = NameIDContent,

    [#xmlElement{content = IssuerContent}|_] =
        riak_cs_xml:find_elements('saml:Issuer', AssertionContent),
    [#xmlText{value = Issuer}|_] = IssuerContent,

    RoleSessionName =
        first_or_none(
          find_AttributeValues_of_Attribute("https://aws.amazon.com/SAML/Attributes/RoleSessionName",
                                            AttributeStatementContent)),
    SourceIdentity =
        first_or_none(
          find_AttributeValues_of_Attribute("https://aws.amazon.com/SAML/Attributes/SourceIdentity",
                                            AttributeStatementContent)),
    SessionDuration =
        first_or_none(
          find_AttributeValues_of_Attribute("https://aws.amazon.com/SAML/Attributes/SessionDuration",
                                            AttributeStatementContent)),
    Role =
        find_AttributeValues_of_Attribute("https://aws.amazon.com/SAML/Attributes/Role",
                                          AttributeStatementContent),

    SubjectType = attr_value('Format', NameIDAttrs),

    State1 = State0#{status => ok,
                     issuer => list_to_binary(Issuer),
                     certificate => <<"Certificate">>,
                     subject => list_to_binary(NameID),
                     subject_type => list_to_binary(SubjectType)},
    maybe_update_state_with([{role_session_name, maybe_list_to_binary(RoleSessionName)},
                             {source_identity, maybe_list_to_binary(SourceIdentity)},
                             {session_duration, maybe_list_to_integer(SessionDuration)},
                             {claims_role, [maybe_list_to_binary(A) || A <- Role]}], State1).

attr_value(A, AA) ->
    case [V || #xmlAttribute{name = Name, value = V} <- AA, Name == A] of
        [] ->
            [];
        [V] ->
            V
    end.

find_AttributeValues_of_Attribute(AttrName, AA) ->
    [extract_attribute_value(C) || #xmlElement{name = 'saml:Attribute',
                                               attributes = EA,
                                               content = C} <- AA,
                                   attr_value('Name', EA) == AttrName].
extract_attribute_value(AA) ->
    hd([V || #xmlElement{name = 'saml:AttributeValue', content = [#xmlText{value = V}]} <- AA]).

first_or_none(A) ->
    case A of
        [V|_] ->
            V;
        [] ->
            none
    end.

maybe_list_to_integer(none) -> none;
maybe_list_to_integer(A) -> list_to_integer(A).
maybe_list_to_binary(none) -> none;
maybe_list_to_binary(A) -> list_to_binary(A).



check_with_saml_provider(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
check_with_saml_provider(#{riak_client := RcPid,
                           certificate := Certificate,
                           issuer := Issuer} = State) ->
    {_, IssuerHostS, _, _, _} = mochiweb_util:urlsplit(binary_to_list(Issuer)),
    case riak_cs_iam:find_saml_provider(#{entity_id => list_to_binary(IssuerHostS)}, RcPid) of
        {ok, SP} ->
            State#{status => check_assertion_certificate(Certificate, SP)};
        {error, notfound} ->
            State#{status => {error, no_such_saml_provider}}
    end.

check_assertion_certificate(ClaimsCert, ?IAM_SAML_PROVIDER{certificates = ProviderCerts}) ->
    ?LOG_DEBUG("STUB ~p ~p", [ClaimsCert, ProviderCerts]),
    
    ok.

create_session_and_issue_temp_creds(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
create_session_and_issue_temp_creds(#{specs := #{policy := InlinePolicy,
                                                 policy_arns := PolicyArns,
                                                 duration_seconds := DurationSeconds},
                                      role := Role,
                                      subject := Subject,
                                      subject_type := SubjectType,
                                      riak_client := RcPid} = State) ->
    SourceIdentity = maps:get(source_identity, State, <<>>),

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
maybe_update_state_with([{_, none}|Rest], State) ->
    maybe_update_state_with(Rest, State);
maybe_update_state_with([{P, V}|Rest], State) ->
    maybe_update_state_with(Rest, maps:put(P, V, State)).




is_valid_arn(A) ->
    nomatch /= re:run(A, "[\u0009\u000A\u000D\u0020-\u007E\u0085\u00A0-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]+").


-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
