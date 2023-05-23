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
    Res = lists:foldl(
            fun(StepF, State) -> StepF(State) end,
            #{specs => Specs},
            [fun validate_args/1,
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



check_with_saml_provider(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
check_with_saml_provider(#{specs := #{principal_arn := PrincipalArn,
                                      saml_assertion := SAMLAssertion,
                                      role_arn := RoleArn}} = State) ->
    State.

create_session_and_issue_temp_creds(#{status := {error, _}} = PreviousStepFailed) ->
    PreviousStepFailed;
create_session_and_issue_temp_creds(#{specs := #{duration_seconds := DurationSeconds}} = State) ->
    ?LOG_DEBUG("STUB DurationSeconds ~p", [DurationSeconds]),
    
    Tomorrow = calendar:system_time_to_local_time(os:system_time(second) + 3600*24, second),
    State#{assumed_role_user => #{arn => <<"arn:aws:sts::123456789012:assumed-role/TestSaml">>,
                                  assumed_role_id => <<"ARO456EXAMPLE789:TestSaml">>},
           audience => <<"https://signin.aws.amazon.com/saml">>,
           credentials => #{access_key_id => <<"ASIAV3ZUEFP6EXAMPLE">>,
                            secret_access_key => <<"8P+SQvWIuLnKhh8d++jpw0nNmQRBZvNEXAMPLEKEY">>,
                            session_token => <<"IQoJb3JpZ2luX2VjEOz////////////////////wEXAMPLEtMSJHMEUCIDoKK3JH9uG"
                            "QE1z0sINr5M4jk+Na8KHDcCYRVjJCZEvOAiEA3OvJGtw1EcViOleS2vhs8VdCKFJQWP"
                            "QrmGdeehM4IC1NtBmUpp2wUE8phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU"
                            "9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD0u0iPPkUL64lIZbqBAz"
                            "+scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA==">>,
                            expiration => Tomorrow},
           issuer => <<"https://samltest.id/saml/idp">>,
           name_qualifier => <<"SbdGOnUkh1i4+EXAMPLExL/jEvs=">>,
           packed_policy_size => 6,
           source_identity => <<"SomeSourceIdentity">>,
           subject => <<"ThatUsersNameID">>,
           subject_type => <<"transient">>}.


is_valid_arn(A) ->
    nomatch /= re:run(A, "[\u0009\u000A\u000D\u0020-\u007E\u0085\u00A0-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]+").


-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.
