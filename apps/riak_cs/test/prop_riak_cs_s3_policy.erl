%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

-module(prop_riak_cs_s3_policy).

-export([prop_ip_filter/0,
         prop_secure_transport/0,
         prop_eval/0,
         prop_policy/0]).

-export([string_condition/0,
         numeric_condition/0,
         date_condition/0]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_ITERATIONS, 500).
-define(QC_OUT(P),
        on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(TIMEOUT, 60).

proper_test_() ->
    {inparallel,
     [
      {timeout, ?TIMEOUT,
       ?_assertEqual(true,
                     proper:quickcheck(numtests(?TEST_ITERATIONS,
                                                ?QC_OUT(prop_ip_filter()))))},
      {timeout, ?TIMEOUT,
       ?_assertEqual(true,
                     proper:quickcheck(numtests(?TEST_ITERATIONS,
                                                ?QC_OUT(prop_secure_transport()))))},
      {timeout, ?TIMEOUT,
       ?_assertEqual(true,
                     proper:quickcheck(numtests(?TEST_ITERATIONS,
                                                ?QC_OUT(prop_eval()))))},
      {timeout, ?TIMEOUT,
       ?_assertEqual(true,
                     proper:quickcheck(numtests(?TEST_ITERATIONS,
                                                ?QC_OUT(prop_policy()))))}
     ]}.

%% accept case of ip filtering
%% TODO: reject case of ip filtering
prop_ip_filter() ->
    ?FORALL({Policy0, Access0, IP, PrefixDigit},
            {policy(), access_v1(), inet_ip_address_v4(), choose(0,32)},
            begin
                application:set_env(riak_cs, trust_x_forwarded_for, true),
                %% replace IP in the policy with prefix mask
                Statement0 = hd(Policy0?POLICY.statement),
                IPStr0 = lists:flatten(io_lib:format("~s/~p",
                                                     [inet_parse:ntoa(IP), PrefixDigit])),
                IPTuple = riak_cs_aws_policy:parse_ip(IPStr0),
                Cond = {'IpAddress', [{'aws:SourceIp', IPTuple}]},
                Statement = Statement0#statement{condition_block = [Cond]},
                Policy = Policy0?POLICY{statement = [Statement]},

                %% replace IP in the wm_reqdata to match the policy
                Peer = lists:flatten(io_lib:format("~s", [inet_parse:ntoa(IP)])),
                ReqData0 = Access0#access_v1.req,
                Access = Access0#access_v1{req = ReqData0#wm_reqdata{peer=Peer}},

                %% eval
                JsonPolicy = riak_cs_aws_policy:policy_to_json_term(Policy),
                Result = riak_cs_aws_policy:eval(Access, JsonPolicy),
                Effect = Statement#statement.effect,

                case {Result, Effect} of
                    %% there are some cases that may be undefined outside the IPaddr thing
                    {undefined, _} -> true;

                    %% the IPaddr thing matched
                    {true, allow} -> true;
                    {false, deny} -> true
                end
            end).


prop_secure_transport() ->
    %% needs better name instead of Bool
    ?FORALL({Policy0, Access, Bool}, {policy(), access_v1(), bool()},
            begin
                %% inject SecureTransport policy
                Statement0 = hd(Policy0?POLICY.statement),
                Cond = {'Bool', [{'aws:SecureTransport', Bool}]},
                Statement = Statement0#statement{condition_block=[Cond]},
                Policy = Policy0?POLICY{statement=[Statement]},

                %% get scheme from generated wm_reqdata
                ReqData = Access#access_v1.req,
                Scheme = ReqData#wm_reqdata.scheme,

                %% eval
                JsonPolicy = riak_cs_aws_policy:policy_to_json_term(Policy),
                Result = riak_cs_aws_policy:eval(Access, JsonPolicy),
                Effect = Statement#statement.effect,

                case {Result, {Scheme, Bool}} of
                    %% SecureTransport policy is not concerned
                    %% some cases, due to unmatch of scheme and SecureTransport
                    %% other cases are due to mismatch of ARN or so
                    {undefined, _} -> true;

                    %% SecureTransport policy is concerned
                    {_, {http, false}} -> (Effect =:= allow) =:= Result;
                    {_, {https, true}} -> (Effect =:= allow) =:= Result
                end
            end).


%% checking not to throw or return unexpected result
prop_eval() ->
    ?FORALL({Policy, Access}, {policy(), access_v1()},
            begin
                application:set_env(riak_cs, trust_x_forwarded_for, true),
                JsonPolicy = riak_cs_aws_policy:policy_to_json_term(Policy),
                case riak_cs_aws_policy:eval(Access, JsonPolicy) of
                    true -> true;
                    false -> true;
                    undefined  -> true
                end
            end).

%% policy conversion between JSON <==> record
prop_policy() ->
    ?FORALL(Policy, policy(),
            begin
                %% ?debugVal(Policy),
                application:set_env(riak_cs, trust_x_forwarded_for, true),
                JsonPolicy =
                    riak_cs_aws_policy:policy_to_json_term(Policy),
                {ok, PolicyFromJson} =
                    riak_cs_aws_policy:policy_from_json(JsonPolicy),
                %%?debugVal({Policy?POLICY.id, PolicyFromJson?POLICY.id}),
                (Policy?POLICY.id =:= PolicyFromJson?POLICY.id)
                    andalso
                      (Policy?POLICY.version =:= PolicyFromJson?POLICY.version)
                    andalso
                    lists:all(fun({LHS, RHS}) ->
                                      riak_cs_aws_policy:statement_eq(LHS, RHS)
                              end,
                              lists:zip(lists:sort(Policy?POLICY.statement),
                                        lists:sort(PolicyFromJson?POLICY.statement)))
            end).


%% Generators
object_action() -> oneof(?SUPPORTED_OBJECT_ACTIONS).
bucket_action() -> oneof(?SUPPORTED_BUCKET_ACTIONS).
iam_action() -> oneof(?SUPPORTED_IAM_ACTIONS).

string_condition()  -> oneof(?STRING_CONDITION_ATOMS).
numeric_condition() -> oneof(?NUMERIC_CONDITION_ATOMS).
date_condition()    -> oneof(?DATE_CONDITION_ATOMS).
ip_addr_condition() -> oneof(?IP_ADDR_CONDITION_ATOMS).

inet_ip_address_v4() ->
    {choose(0,16#FF), choose(0,16#FF), choose(0,16#FF), choose(0,16#FF)}.

ip_with_mask() ->
    ?LET({IP, PrefixDigit}, {inet_ip_address_v4(), choose(0, 32)},
         begin
             %% this code is to be moved to riak_cs_s3_policy
             String = lists:flatten(io_lib:format("~s/~p", [inet_parse:ntoa(IP), PrefixDigit])),
             riak_cs_aws_policy:parse_ip(String)
         end).

condition_pair() ->
    oneof([
%%           {date_condition(),    [{'aws:CurrentTime', binary_char_string()}]},
%%           {numeric_condition(), [{'aws:EpochTime', nat()}]},
           {'Bool',              [{'aws:SecureTransport', bool()}]},
           {ip_addr_condition(), [{'aws:SourceIp',  one_or_more_ip_with_mask()}]}
%%           {string_condition(),  [{'aws:UserAgent', binary_char_string()}]},
%%           {string_condition(),  [{'aws:Referer',   binary_char_string()}]}
          ]).

one_or_more_ip_with_mask() ->
    oneof([ip_with_mask(), non_empty(list(ip_with_mask()))]).

%% TODO: FIXME: add a more various form of path
path() ->
    <<"test/*">>.

arn_id() ->
    %% removing ":" which confuses parser
    nonempty_binary_char_string().

arn_v1() ->
    #arn_v1{
       provider = aws,
       service  = s3,
       region   = <<"cs-ap-e1">>,
       id =  arn_id(),
       path = path()
      }.


principal() -> oneof(['*', {aws, '*'}, {aws, nonempty_binary_char_string()}]).

effect() -> oneof([allow, deny]).

statement() ->
    #statement{
       sid = nonempty_binary_char_string(),
       effect = effect(),
       principal  = principal(),
       action     = [action()],
       not_action = [],
       resource   = oneof([arn_v1(), '*']),
       condition_block = [condition_pair()]
      }.

statements() ->
    non_empty(list(statement())).

ustring() -> list(choose($a, $z)).

binary_char_string() ->
    ?LET(String, ustring(), list_to_binary(String)).

nonempty_binary_char_string() ->
    ?LET({Char, BinString}, {choose($a,$z), binary_char_string()},
         <<Char, BinString/binary>>).

policy() ->
    #policy{
       id        = oneof([undefined, riak_cs_aws_utils:make_id(11)]),
       statement = statements()
      }.

method() ->
    oneof(['PUT', 'GET', 'POST', 'DELETE', 'HEAD']).

method_from_target(bucket) ->
    oneof(['PUT', 'GET', 'DELETE', 'HEAD']);
method_from_target(bucket_acl) ->
    oneof(['PUT', 'GET']);
method_from_target(bucket_location) -> 'GET';
method_from_target(bucket_policy) ->
    oneof(['PUT', 'GET', 'DELETE']);
method_from_target(bucket_versioning) -> 'GET';
method_from_target(bucket_uploads) -> 'GET';
method_from_target(object) ->
    oneof(['PUT', 'GET', 'POST', 'DELETE', 'HEAD']);
method_from_target(object_acl) ->
    oneof(['PUT', 'GET']).

action() ->
    oneof([ object_action(), bucket_action(), iam_action(), <<"s3:*">>, <<"iam:*">>, <<"sts:*">>, <<"*">> ]).

access_v1() ->
    ?LET(Target, oneof([bucket, bucket_acl, bucket_location,
                        bucket_policy, bucket_uploads, bucket_versioning,
                        bucket_uploads,
                        object, object_acl]),
         ?LET(Method, method_from_target(Target),
              #access_v1{
                 method = Method,
                 target = Target,
                 action = action(),
                 id     = nonempty_binary_char_string(),
                 bucket = nonempty_binary_char_string(),
                 key    = oneof([undefined, nonempty_binary_char_string()]),
                 req    = wm_reqdata()
                })).

http_response_code() ->
    oneof([200]).

wm_reqdata() ->
    ?LET(IP, inet_ip_address_v4(),
         #wm_reqdata{
            method = method(),
            scheme = oneof([http, https]),
            peer   = inet_parse:ntoa(IP),
            wm_state  = undefined,
            disp_path = "/",
            path      = "/",
            raw_path  = "/",
            path_info = dict:new(),
            path_tokens = ["/"],
            app_root  = "/",
            response_code = oneof([undefined, http_response_code()]),
            max_recv_body = nat(),
            max_recv_hunk = nat(),
            req_cookie    = ustring(),
            req_qs        = ustring(),
            req_headers   = undefined,
            req_body      = binary_char_string(),
            resp_redirect = bool(),
            resp_headers  = undefined,
            resp_body     = undefined,
            resp_range    = "range=0-",
            host_tokens   = list(binary_char_string()),
            port          = choose(1,65535),
            notes         = list(nonempty_binary_char_string()) %% any..?
           }).
