%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% Upgrading test case from {Riak, CS} = {2.0, 1.5} and {1.4, 2.0}

%% Scenario:
%% | step | riak | stanchion |  cs |
%% |    1 |  1.4 |       1.5 | 1.5 |
%% |    2 |  1.4 |       2.0 | 2.0 |
%% |    3 |  2.0 |       2.0 | 1.5 |

-module(migration_mixed_test).

-export([confirm/0]).
-export([upgrade_stanchion/1,
         transition_to_cs20_with_kv14/1,
         transition_to_cs15_with_kv20/1]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    rt_config:set(console_log_level, info),
    SetupRes = setup_previous(),
    {ok, InitialState} = cs_suites:new(SetupRes),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, history()),
    {ok, _FinalState}  = cs_suites:cleanup(EvolvedState),
    rtcs:pass().

setup_previous() ->
    PrevConfigs = rtcs_config:previous_configs(custom_configs(previous)),
    SetupRes = rtcs:setup(2, PrevConfigs, previous),
    lager:info("rt_nodes> ~p", [rt_config:get(rt_nodes)]),
    lager:info("rt_versions> ~p", [rt_config:get(rt_versions)]),
    {_AdminConfig, {RiakNodes, CSNodes, StanchionNode}} = SetupRes,
    rtcs:assert_versions(riak_kv,   RiakNodes,       "^1\.4\."),
    rtcs:assert_versions(stanchion, [StanchionNode], "^1\.5\."),
    rtcs:assert_versions(riak_cs,   CSNodes,         "^1\.5\."),
    SetupRes.

%% Custom configurations for Riak and Riak CS
%% Make data file size tiny and leeway period tight in order to confirm
%% only small data will remain in bitcask directories after deleting all
%% CS objects, run GC and merge+delete of bitcask.
custom_configs(previous) ->
    [{riak,
      rtcs_config:previous_riak_config([{bitcask, [{max_file_size, 4*1024*1024}]}])},
     {cs,
      rtcs_config:previous_cs_config([{leeway_seconds, 1}])}];
custom_configs(current) ->
    %% This branch is only for debugging this module
    [{riak,
      rtcs_config:riak_config([{bitcask, [{max_file_size, 4*1024*1024}]}])},
     {cs,
      rtcs_config:cs_config([{leeway_seconds, 1}])}].

history() ->
    [
     {cs_suites, set_node1_version,            [previous]},
     {cs_suites, run,                          ["15-14"]},
     {?MODULE  , upgrade_stanchion,            []},
     {?MODULE  , transition_to_cs20_with_kv14, []},
     {cs_suites, run,                          ["20-14"]},
     {?MODULE  , transition_to_cs15_with_kv20, []},
     {cs_suites, run,                          ["15-20"]}
    ].

upgrade_stanchion(State) ->
    rtcs_exec:stop_stanchion(previous),
    rtcs_config:migrate_stanchion(previous, current, cs_suites:admin_credential(State)),
    rtcs_exec:start_stanchion(current),
    rtcs:assert_versions(stanchion, cs_suites:nodes_of(stanchion, State), "^2\."),
    {ok, State}.

transition_to_cs20_with_kv14(State) ->
    RiakNodes = cs_suites:nodes_of(riak, State),
    CsNodes = cs_suites:nodes_of(cs, State),
    AdminCreds = cs_suites:admin_credential(State),
    migrate_nodes_to_cs20_with_kv14(AdminCreds, RiakNodes),
    rtcs:assert_versions(riak_kv, RiakNodes, "^1\.4\."),
    rtcs:assert_versions(riak_cs, CsNodes,   "^2\."),
    rt:setup_log_capture(hd(cs_suites:nodes_of(cs, State))),
    {ok, NewState} = cs_suites:set_node1_version(current, State),
    {ok, NewState}.

migrate_nodes_to_cs20_with_kv14(AdminCreds, RiakNodes) ->
    [begin
         N = rtcs_dev:node_id(RiakNode),
         rtcs_exec:stop_cs(N, previous),
         ok = rtcs_config:upgrade_cs(N, AdminCreds),
         %% actually after CS 2.1.1
         rtcs:set_advanced_conf({cs, current, N},
                                [{riak_cs,
                                  [{riak_host, {"127.0.0.1", rtcs_config:pb_port(1)}}]}]),
         rtcs_exec:start_cs(N, current)
     end
     || RiakNode <- RiakNodes],
    ok.

transition_to_cs15_with_kv20(State) ->
    RiakNodes = cs_suites:nodes_of(riak, State),
    CsNodes = cs_suites:nodes_of(cs, State),
    AdminCreds = cs_suites:admin_credential(State),
    migrate_nodes_to_cs15_with_kv20(AdminCreds, RiakNodes),
    rtcs:assert_versions(riak_kv, RiakNodes, "^2\."),
    rtcs:assert_versions(riak_cs, CsNodes,   "^1\.5\."),
    rt:setup_log_capture(hd(cs_suites:nodes_of(cs, State))),
    {ok, State}.

migrate_nodes_to_cs15_with_kv20(AdminCreds, RiakNodes) ->
    {_, RiakCurrentVsn} =
        rtcs_dev:riak_root_and_vsn(current, rt_config:get(build_type, oss)),
    [begin
         N = rtcs_dev:node_id(RiakNode),
         rtcs_exec:stop_cs(N, current),
         %% to check error log emptyness afterwards, truncate it here.
         rtcs:truncate_error_log(N),
         ok = rt:upgrade(RiakNode, RiakCurrentVsn),
         rt:wait_for_service(RiakNode, riak_kv),
         ok = rtcs_config:migrate_cs(current, previous, N, AdminCreds),
         rtcs_exec:start_cs(N, previous)
     end
     || RiakNode <- RiakNodes],
    rt:wait_until_ring_converged(RiakNodes),
    ok.
