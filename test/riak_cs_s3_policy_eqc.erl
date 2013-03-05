%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_s3_policy_eqc).

-compile(export_all).

-ifdef(TEST).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("webmachine/include/wm_reqdata.hrl").

-define(TEST_ITERATIONS, 500).
-define(SET_MODULE, twop_set).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

eqc_test_()->
    {spawn,
     [
      {timeout, 20, ?_assertEqual(true,
                                  quickcheck(numtests(?TEST_ITERATIONS,
                                                      ?QC_OUT(prop_policy_v1()))))},
      {timeout, 20, ?_assertEqual(true,
                                  quickcheck(numtests(?TEST_ITERATIONS,
                                                      ?QC_OUT(prop_eval()))))}
      ]}.


prop_eval() ->
    ?FORALL({Policy, Access}, {policy_v1(), access_v1()},
            begin
                JsonPolicy = riak_cs_s3_policy:policy_to_json_term(Policy),
                case eval(Access, JsonPolicy) of
                    true -> ok;
                    false -> ok;
                    undefined  -> ok
                end,
                true
            end).

prop_policy_v1()->
    ?FORALL(Policy, policy_v1(),
            begin
                JsonPolicy =
                    riak_cs_s3_policy:policy_to_json_term(Policy),
                
                {ok, PolicyFromJson} =
                    riak_cs_s3_policy:policy_from_json(JsonPolicy),
                (Policy?POLICY.id =:= PolicyFromJson?POLICY.id)
                    andalso
                      (Policy?POLICY.version =:= PolicyFromJson?POLICY.version)
                    andalso
                    lists:all(fun({LHS, RHS}) ->
                                      riak_cs_s3_policy:statement_eq(LHS, RHS)
                              end,
                              lists:zip(Policy?POLICY.statement,
                                        PolicyFromJson?POLICY.statement))
            end).

%% Generators
object_action() -> oneof(?SUPPORTED_OBJECT_ACTION).
bucket_action() -> oneof(?SUPPORTED_BUCKET_ACTION).

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
             riak_cs_s3_policy:parse_ip(String)
         end).

condition_pair() ->
    oneof([{date_condition(),    [{'aws:CurrentTime', binary_char_string()}]},
           {numeric_condition(), [{'aws:EpochTime', nat()}]},
           {'Bool',              [{'aws:SecureTransport', bool()}]},
           {ip_addr_condition(), [{'aws:SourceIp',  ip_with_mask()}]},
           {string_condition(),  [{'aws:UserAgent', binary_char_string()}]},
           {string_condition(),  [{'aws:Referer',   binary_char_string()}]}
          ]).

%% TODO: FIXME:
path() ->
    "test/*".

arn_id() ->
    %% removing ":" which confuses parser
    ?LET(String, list(oneof([choose(33,57), choose(59,127)])),
         list_to_binary(String)).

arn_v1() ->
    #arn_v1{
       provider = aws,
       service  = s3,
       region   = "cs-ap-e1",
       id =  arn_id(), %% TODO, remove ':'
       path = path()
      }.


principal() -> oneof(['*', {aws, '*'}]).

effect() -> oneof([allow, deny]).

statement() ->
    #statement{
       sid = nonempty_binary_char_string(),
       effect = effect(),
       principal  = principal(),
       action     = oneof([ object_action(), bucket_action(), '*' ]),
       not_action = [],
       resource   = oneof([arn_v1(), '*']),
       condition_block = list(condition_pair())
      }.

creation_time() ->
    {nat(), choose(0, 1000000), choose(0, 1000000)}.

string() -> list(choose(33,127)).

binary_char_string() ->
    ?LET(String, string(), list_to_binary(String)).

nonempty_binary_char_string() ->
    ?LET({Char, BinString}, {choose(33,127), binary_char_string()},
         <<Char, BinString/binary>>).

policy_v1() ->
    #policy_v1{
       version   = <<"2008-10-17">>,
       id        = oneof([undefined, nonempty_binary_char_string()]),
       statement = list(statement()),
       creation_time = creation_time()
      }.

method() ->
    oneof(['PUT', 'GET', 'POST', 'DELETE', 'HEAD']).

access_v1() ->
    #access_v1{
       method = method(),
       target = oneof([bucket, bucket_acl, bucket_location,
                       bucket_policy, bucket_uploads, bucket_version,
                       object, object_acl]),
       id     = string(),
       bucket = nonempty_binary_char_string(),
       key    = oneof([undefined, binary_char_string()]),
       req    = wm_reqdata()
      }.

http_response_code() ->
    oneof([200]).

wm_reqdata() ->
    #wm_reqdata{
       method = method(),
       scheme = oneof([http, https]),
       peer   = inet_ip_address_v4(),
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
       req_cookie    = string(),
       req_qs        = string(),
       req_headers   = undefined,
       req_body      = binary_char_string(),
       resp_redirect = bool(),
       resp_headers  = undefined,
       resp_body     = undefined,
       resp_range    = "range=0-",
       host_tokens   = list(binary_char_string()),
       port          = choose(1,65535),
       notes         = list(nonempty_binary_char_string()) %% any..?
      }.

-endif.
