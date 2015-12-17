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

%% Upgrading test case from {Riak, CS} = {1.4, 1.5} to {2.0, 2.0}
%%
%% Scenario:
%% - Setup cluster by Riak 1.4.x and CS/Stanchion 1.5.x (i.e. previous)
%% - Execute some S3 API requests and background admin jobs, 1st time
%% - Upgrade Stanchion to 2.x
%% - Upgrade partial pairs of Riak and Riak CS to 2.x
%% - Execute some S3 API requests and background admin jobs, 2nd time
%% - Upgrade remaining pairs of Riak and Riak CS to 2.x
%% - Execute some S3 API requests and background admin jobs, 3rd time
%% - Delete all S3 objects
%% - Trigger bitcask merge and wait for merge & delete finished
%% - Assert bitcask data files are shrinked to small ones

-module(migration_15_to_20_test).

-export([confirm/0]).
-export([upgrade_stanchion/1, upgrade_partially/2, upgrade_remaining/2]).

-include_lib("eunit/include/eunit.hrl").

%% TODO: More than 1 is better
-define(UPGRADE_FIRST, 1).

confirm() ->
    rt_config:set(console_log_level, info),
    confirm(upgrade_with_full_ops).

%% `upgrade_with_reduced_ops' and `no_upgrade_with_reduced_ops' are only for
%% debugging of this module and/or `cs_suites'.
-spec confirm(Profile::atom()) -> pass | no_return().
confirm(upgrade_with_full_ops) ->
    SetupRes = setup_previous(),
    {ok, InitialState} = cs_suites:new(SetupRes),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, upgrade_history()),
    {ok, _FinalState}  = cs_suites:cleanup(EvolvedState),
    rtcs:pass();
confirm(upgrade_with_reduced_ops) ->
    SetupRes = setup_previous(),
    {ok, InitialState} = cs_suites:new(SetupRes, rtcs:reduced_ops()),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, upgrade_history()),
    {ok, _FinalState}  = cs_suites:cleanup(EvolvedState),
    rtcs:pass();
confirm(no_upgrade_with_reduced_ops) ->
    SetupRes = rtcs:setup(2, rtcs_config:configs(custom_configs(current))),
    {ok, InitialState} = cs_suites:new(SetupRes, cs_suites:reduced_ops()),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, no_upgrade_history()),
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

upgrade_history() ->
    [
     {cs_suites, set_node1_version, [previous]},
     {cs_suites, run,               ["1-pre"]},
     {?MODULE  , upgrade_stanchion, []},
     {?MODULE  , upgrade_partially, [?UPGRADE_FIRST]},
     {cs_suites, run,               ["2-mix"]},
     {?MODULE  , upgrade_remaining, [?UPGRADE_FIRST]},
     {cs_suites, run,               ["3-fin"]}
    ].

no_upgrade_history() ->
    [
     {cs_suites, set_node1_version, [current]},
     {cs_suites, run,["1st"]},
     {cs_suites, run,["2nd"]}
    ].

upgrade_stanchion(State) ->
    rtcs_exec:stop_stanchion(previous),
    rtcs_config:migrate_stanchion(previous, current, cs_suites:admin_credential(State)),
    rtcs_exec:start_stanchion(current),
    rtcs:assert_versions(stanchion, cs_suites:nodes_of(stanchion, State), "^2\."),
    {ok, State}.

upgrade_partially(UpgradeFirstCount, State) ->
    RiakNodes = cs_suites:nodes_of(riak, State),
    CsNodes = cs_suites:nodes_of(cs, State),
    AdminCreds = cs_suites:admin_credential(State),
    {RiakUpgrades, RiakRemainings} = lists:split(UpgradeFirstCount, RiakNodes),
    {CsUpgrades, CsRemainings} = lists:split(UpgradeFirstCount, CsNodes),
    upgrade_nodes(AdminCreds, RiakUpgrades),
    rtcs:assert_versions(riak_kv, RiakUpgrades,   "^2\."),
    rtcs:assert_versions(riak_cs, CsUpgrades,     "^2\."),
    rtcs:assert_versions(riak_kv, RiakRemainings, "^1\.4\."),
    rtcs:assert_versions(riak_cs, CsRemainings,   "^1\.5\."),
    rt:setup_log_capture(hd(cs_suites:nodes_of(cs, State))),
    {ok, NewState} = cs_suites:set_node1_version(current, State),
    {ok, NewState}.

upgrade_remaining(UpgradeFirstCount, State) ->
    RiakNodes = cs_suites:nodes_of(riak, State),
    CsNodes = cs_suites:nodes_of(cs, State),
    AdminCreds = cs_suites:admin_credential(State),
    {_RiakUpgraded, RiakRemainings} = lists:split(UpgradeFirstCount, RiakNodes),
    {_CsUpgraded, CsRemainings} = lists:split(UpgradeFirstCount, CsNodes),
    upgrade_nodes(AdminCreds, RiakRemainings),
    rtcs:assert_versions(riak_kv, RiakRemainings, "^2\."),
    rtcs:assert_versions(riak_cs, CsRemainings,   "^2\."),
    {ok, State}.

%% Upgrade Riak and Riak CS pairs of nodes
upgrade_nodes(AdminCreds, RiakNodes) ->
    {_, RiakCurrentVsn} =
        rtcs_dev:riak_root_and_vsn(current, rt_config:get(build_type, oss)),
    [begin
         N = rtcs_dev:node_id(RiakNode),
         rtcs_exec:stop_cs(N, previous),
         ok = rt:upgrade(RiakNode, RiakCurrentVsn),
         rt:wait_for_service(RiakNode, riak_kv),
         ok = rtcs_config:upgrade_cs(N, AdminCreds),
         rtcs:set_advanced_conf({cs, current, N},
                                [{riak_cs,
                                  [{riak_host, {"127.0.0.1", rtcs_config:pb_port(1)}}]}]),
         rtcs_exec:start_cs(N, current)
     end
     || RiakNode <- RiakNodes],
    ok = rt:wait_until_ring_converged(RiakNodes),
    ok.
