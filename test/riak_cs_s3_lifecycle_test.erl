%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc ad-hoc policy tests

-module(riak_cs_s3_lifecycle_test).

-compile(export_all).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% parse_xml_test_()->
%%     [
%%      ?_assertEqual({{192,0,0,1}, {255,0,0,0}},
%%                    riak_cs_s3_policy:parse_ip(<<"192.0.0.1/8">>)),
%%      ?_assertEqual({error, einval},
%%                    riak_cs_s3_policy:parse_ip(<<"192.3.1/16">>)),
%%      ?_assertEqual(<<"1.2.3.4">>,
%%                    riak_cs_s3_policy:print_ip(riak_cs_s3_policy:parse_ip(<<"1.2.3.4">>))),
%%      ?_assertEqual(<<"1.2.3.4/13">>,
%%                    riak_cs_s3_policy:print_ip(riak_cs_s3_policy:parse_ip(<<"1.2.3.4/13">>))),
%%      ?_assertEqual({error, einval}, 
%%                    riak_cs_s3_policy:parse_ip(<<"0">>)),
%%      ?_assertEqual({error, einval}, 
%%                    riak_cs_s3_policy:parse_ip(<<"0/0">>))
%%     ].

simple_plain_conversion_test()->
    {ok, LifeCycle} = riak_cs_s3_lifecycle:from_xml(sample_plain_lifecycle()),
    Rule = hd(LifeCycle#lifecycle_config_v1.rules),
    ?assertEqual(<<"sample-rule">>, Rule#lifecycle_rule_v1.id),
    ?assertEqual(<<"key-prefix">>, Rule#lifecycle_rule_v1.prefix),
    ?assertEqual(enabled, Rule#lifecycle_rule_v1.status),
    T = Rule#lifecycle_rule_v1.transition,
    ?assertEqual(31, T#lifecycle_transition_v1.days),
    ?assertEqual(cold_storage, T#lifecycle_transition_v1.storage_class),
    ?assertEqual(30, Rule#lifecycle_rule_v1.expiration).

sample_plain_lifecycle()->
    "<?xml version=\"1.0\" encoding=\"utf-8\" ?><LifecycleConfiguration>"
        "    <Rule>"
        "        <ID>sample-rule</ID>"
        "        <Prefix>key-prefix</Prefix>"
        "        <Status>Enabled</Status>"
        "        <Transition>"
        "           <Days>31</Days> "
        "           <StorageClass>GLACIER</StorageClass>       "
        "        </Transition>    "
        "        <Expiration>"
        "           <Days>30</Days>"
        "        </Expiration>"
        "    </Rule>"
        "</LifecycleConfiguration>".

sample_policy_check_test()->
    ok.

sample_conversion_test()->
    ok.


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

-endif.
