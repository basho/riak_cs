%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc ad-hoc policy tests

-module(riak_cs_s3_policy_test).

-compile(export_all).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

% TODO: eqc tests
parse_ip_test_()->
    [
     ?_assertEqual({{192,0,0,1}, {255,0,0,0}},
                   riak_cs_s3_policy:parse_ip(<<"192.0.0.1/8">>)),
     ?_assertEqual({{192,3,0,1}, {255,255,0,0}},
                   riak_cs_s3_policy:parse_ip(<<"192.3.1/16">>)),
     ?_assertEqual(<<"1.2.3.4">>,
                   riak_cs_s3_policy:print_ip(riak_cs_s3_policy:parse_ip(<<"1.2.3.4">>))),
     ?_assertEqual(<<"1.2.3.4/13">>,
                   riak_cs_s3_policy:print_ip(riak_cs_s3_policy:parse_ip(<<"1.2.3.4/13">>)))
    ].

empty_statement_conversion_test()->
    Policy = ?POLICY{id= <<"hello">>},
    JsonPolicy = "{\"Version\":\"2008-10-17\",\"Statement\":[],\"Id\":\"hello\"}",
    ?assertEqual(mochijson2:decode(JsonPolicy),
                 mochijson2:decode(riak_cs_s3_policy:policy_to_json_term(Policy))),
    {ok, PolicyFromJson} = riak_cs_s3_policy:policy_from_json(list_to_binary(JsonPolicy)),
    ?assertEqual(Policy?POLICY.id, PolicyFromJson?POLICY.id),
    ?assertEqual(Policy?POLICY.statement, PolicyFromJson?POLICY.statement),
    ?assertEqual(Policy?POLICY.version, PolicyFromJson?POLICY.version).

sample_plain_allow_policy()->
    <<"{"
      "\"Id\":\"Policy1354069963875\","
      "\"Statement\":["
      "{"
      "   \"Sid\":\"Stmt1354069958376\","
      "   \"Action\":["
      "     \"s3:CreateBucket\","
      "     \"s3:DeleteBucket\","
      "     \"s3:DeleteBucketPolicy\","
      "     \"s3:DeleteObject\","
      "     \"s3:GetBucketAcl\","
      "     \"s3:GetBucketPolicy\","
      "     \"s3:GetObject\","
      "     \"s3:GetObjectAcl\","
      "     \"s3:ListAllMyBuckets\","
      "     \"s3:ListBucket\","
      "     \"s3:PutBucketAcl\","
      "     \"s3:PutBucketPolicy\","
%      "     \"s3:PutObject\","
      "     \"s3:PutObjectAcl\""
      "   ],"
      "   \"Condition\":{"
      "     \"IpAddress\": { \"aws:SourceIp\":\"192.168.0.1/8\" }"
      "   },"
      "   \"Effect\": \"Allow\","
      "   \"Resource\": \"arn:aws:s3:::test\","
      "   \"Principal\": {"
      "     \"AWS\": \"*\""
      "   }"
      "  }"
      " ]"
      "}" >>.

sample_policy_check_test()->
    JsonPolicy0 = sample_plain_allow_policy(),
    {ok, Policy} = riak_cs_s3_policy:policy_from_json(JsonPolicy0),
    Access = #access_v1{method='GET', target=object, id="spam/ham/egg",
                        req = #wm_reqdata{peer="192.168.0.1"}, bucket= <<"test">>},
    ?assert(riak_cs_s3_policy:eval(Access, Policy)),
    % io:format(standard_error, "~w~n", [Policy]),
    Access2 = Access#access_v1{method='PUT', target=object},
    ?assertEqual(undefined, riak_cs_s3_policy:eval(Access2, Policy)),
    Access3 = Access#access_v1{req=#wm_reqdata{peer="1.1.1.1"}},
    ?assert(not riak_cs_s3_policy:eval(Access3, Policy)).

sample_conversion_test()->
    JsonPolicy0 = sample_plain_allow_policy(),
    {ok, Policy} = riak_cs_s3_policy:policy_from_json(JsonPolicy0),
    {ok, PolicyFromJson} = riak_cs_s3_policy:policy_from_json(riak_cs_s3_policy:policy_to_json_term(Policy)),
    ?assertEqual(Policy?POLICY.id, PolicyFromJson?POLICY.id),
    ?assert(lists:all(fun({LHS, RHS}) ->
                              riak_cs_s3_policy:statement_eq(LHS, RHS)
                      end,
                      lists:zip(Policy?POLICY.statement,
                                PolicyFromJson?POLICY.statement))),
    ?assertEqual(Policy?POLICY.version, PolicyFromJson?POLICY.version).


eval_all_ip_addr_test() ->
    ?assert(riak_cs_s3_policy:eval_all_ip_addr([{{192,168,0,1},{255,255,255,255}}], {192,168,0,1})),
    ?assert(not riak_cs_s3_policy:eval_all_ip_addr([{{192,168,0,1},{255,255,255,255}}], {192,168,25,1})),
    ?assert(riak_cs_s3_policy:eval_all_ip_addr([{{192,168,0,1},{255,255,255,0}}], {192,168,0,23})).

eval_ip_address_test()->
    ?assert(riak_cs_s3_policy:eval_ip_address(#wm_reqdata{peer = "23.23.23.23"},
                                              [garbage,{chiba, boo},"saitama",
                                               {'aws:SourceIp', {{23,23,0,0},{255,255,0,0}}}, hage])).

eval_ip_addresses_test()->
    ?assert(riak_cs_s3_policy:eval_ip_address(#wm_reqdata{peer = "23.23.23.23"},
                                              [{'aws:SourceIp', {{1,1,1,1}, {255,255,255,0}}},
                                               {'aws:SourceIp', {{23,23,0,0},{255,255,0,0}}}, hage])).

eval_condition_test()->
    ?assert(riak_cs_s3_policy:eval_condition(#wm_reqdata{peer = "23.23.23.23"},
                                             {'IpAddress', [garbage,{chiba, boo},"saitama",
                                                            {'aws:SourceIp', {{23,23,0,0},{255,255,0,0}}}, hage]})).

eval_statement_test()->
    Access = #access_v1{method='GET', target=object,
                        req=#wm_reqdata{peer="23.23.23.23"},
                        bucket= <<"testbokee">>},
    Statement = #statement{effect=allow,condition_block=
                               [{'IpAddress',
                                 [{'aws:SourceIp', {{23,23,0,0},{255,255,0,0}}}]}],
                           action=['s3:GetObject'],
                           resource='*'},
    ?assert(riak_cs_s3_policy:eval_statement(Access, Statement)).

my_split_test_()->
    [
     ?_assertEqual(["foo", "bar"], riak_cs_s3_policy:my_split($:, "foo:bar", [], [])),
     ?_assertEqual(["foo", "", "", "bar"], riak_cs_s3_policy:my_split($:, "foo:::bar", [], [])),
     ?_assertEqual(["arn", "aws", "s3", "", "", "hoge"],
                   riak_cs_s3_policy:my_split($:, "arn:aws:s3:::hoge", [], [])),
     ?_assertEqual(["arn", "aws", "s3", "", "", "hoge/*"],
                   riak_cs_s3_policy:my_split($:, "arn:aws:s3:::hoge/*", [], []))
    ].

parse_arn_test()->
    List0 = [<<"arn:aws:s3:::hoge">>, <<"arn:aws:s3:::hoge/*">>],
    {ok, ARNS0} = riak_cs_s3_policy:parse_arns(List0),
    ?assertEqual(List0, riak_cs_s3_policy:print_arns(ARNS0)),

    List1 = [<<"arn:aws:s3:ap-northeast-1:000000:hoge">>, <<"arn:aws:s3:::hoge/*">>],
    {ok, ARNS1} = riak_cs_s3_policy:parse_arns(List1),
    ?assertEqual(List1, riak_cs_s3_policy:print_arns(ARNS1)),

    ?assertEqual({error, bad_arn}, riak_cs_s3_policy:parse_arns([<<"asdfiua;sfkjsd">>])),

    List2 = <<"*">>,
    {ok, ARNS2} = riak_cs_s3_policy:parse_arns(List2),
    ?assertEqual(List2, riak_cs_s3_policy:print_arns(ARNS2)).

sample_securetransport_statement()->
    <<"{"
      "\"Id\":\"Policy135406996387500\","
      "\"Statement\":["
      "{"
      "   \"Sid\":\"Stmt135406995deadbeef\","
      "   \"Action\":["
      "     \"s3:GetObject\","
      "     \"s3:PutObject\","
      "     \"s3:DeleteObject\""
      "   ],"
      "   \"Condition\":{"
      "     \"Bool\": { \"aws:SecureTransport\":true }"
      "   },"
      "   \"Effect\": \"Allow\","
      "   \"Resource\": \"arn:aws:s3:::test\","
      "   \"Principal\": {"
      "     \"AWS\": \"*\""
      "   }"
      "  }"
      " ]"
      "}" >>.


secure_transport_test()->
    JsonPolicy0 = sample_securetransport_statement(),
    {ok, Policy} = riak_cs_s3_policy:policy_from_json(JsonPolicy0),
    Req = #wm_reqdata{peer="192.168.0.1", scheme=https},
    Access = #access_v1{method='GET', target=object, id="spam/ham/egg",
                        req = Req, bucket= <<"test">>},
    ?assert(riak_cs_s3_policy:eval(Access, Policy)),
    % io:format(standard_error, "~w~n", [Policy]),
    Access2 = Access#access_v1{req=Req#wm_reqdata{scheme=http}},
    ?assert(not riak_cs_s3_policy:eval(Access2, Policy)).

%% "Bool": { "aws:SecureTransport" : true,
%%           "aws:SecureTransport" : false } is recognized as false
%%
%% "Bool": { "aws:SecureTransport" : false,
%%           "aws:SecureTransport" : true } is recognized as true

malformed_json_statement()->
    <<"{"
      "\"Id\":\"Policy135406996387500\","
      "\"Statement\":["
      "{"
      "   \"Sid\":\"Stmt135406995deadbeef\","
      "   \"Action\":["
      "     \"s3:GetObject\","
      "     \"s3:PutObject\","
      "     \"s3:DeleteObject\""
      "   ],"
      "   \"Condition\":{"
      "     \"Bool\": { \"aws:SecureTransport\":tr }"
      "   },"
      "   \"Effect\": \"Allow\","
      "   \"Resource\": \"arn:aws:s3:::test\","
      "   \"Principal\": {"
      "     \"AWS\": \"*\""
      "   }"
      "  }"
      " ]"
      "}" >>.

malformed_policy_json_test()->
    JsonPolicy0 = malformed_json_statement(),
    {error, malformed_policy_json} = riak_cs_s3_policy:policy_from_json(JsonPolicy0).

-endif.
