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

-module(riak_cs_debug_test).

-compile(export_all).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(assertContainsAll(ExpectedList,ActualList),
        lists:foreach(
          fun(X) -> ?assert(lists:member(X, ActualList)) end,
          ExpectedList)).

-define(assertMatchAny(Pattern, ActualList),
        ?assert(
           lists:any(
             fun(X) ->
                     case re:run(X, Pattern) of
                         {match, _} -> true;
                         nomatch -> false
                     end
             end, ActualList))).

-define(assertNotMatchAny(Pattern, ActualList),
        ?assert(
           lists:all(
             fun(X) ->
                     case re:run(X, Pattern) of
                         {match, _} -> false;
                         nomatch -> true
                     end
             end, ActualList))).

confirm() ->
    %% Run riak-cs-debug before cuttlefish generates configs.
    TarGz1 = exec_cs_debug(),
    List1 = trim_dir_prefix(list_files(TarGz1)),
    ?assertContainsAll(minimum_necessary_files(), List1),

    _ = rtcs:setup(1),

    %% Run riak-cs-debug after cuttlefish generates configs.
    TarGz2 = exec_cs_debug(),
    List2 = trim_dir_prefix(list_files(TarGz2)),
    ?assertContainsAll(minimum_necessary_files_after_boot(), List2),
    ?assertMatchAny("^logs/platform_log_dir/access.log.*", List2),
    ?assertMatchAny("^config/generated.configs/app.*.config", List2),
    ?assertMatchAny("^config/generated.configs/vm.*.args", List2),
    ?assertNotMatchAny("^config/*.pem$", List2),
    ok = file:rename(TarGz2, TarGz2 ++ ".back"),


    %% Run riak-cs-debug with app.config and vm.args.
    move_generated_configs_as_appconfigs(),
    restart_cs_node(),
    TarGz3 = exec_cs_debug(),
    List3 = trim_dir_prefix(list_files(TarGz3)),
    ?assertContainsAll(minimum_necessary_files_after_boot()
                       ++ ["config/app.config", "config/vm.args"],
                       List3),
    ?assertNotMatchAny("^config/generated.configs/app.*.config", List3),
    ?assertNotMatchAny("^config/generated.configs/vm.*.args", List3),

    rtcs:pass().

restart_cs_node() ->
    rtcs_exec:stop_cs(1),
    rt:wait_until_unpingable(rtcs:cs_node(1)),
    rtcs_exec:start_cs(1),
    ok.

move_generated_configs_as_appconfigs() ->
    DevPath = rtcs_config:devpath(cs, current),
    GenConfPath =  DevPath ++ "/dev/dev1/data/generated.configs/",
    AppConfig = filelib:wildcard([GenConfPath ++ "app.*.config"]),
    VmArgs = filelib:wildcard([GenConfPath ++ "vm.*.args"]),

    ConfPath =  DevPath ++ "/dev/dev1/etc/",
    ok = file:rename(AppConfig, ConfPath ++ "app.config"),
    ok = file:rename(VmArgs, ConfPath ++ "vm.args"),
    ok.

exec_cs_debug() ->
    DevPath = rtcs_config:devpath(cs, current),
    Cmd = rtcs_exec:riakcs_debugcmd(DevPath, 1, []),
    Output = os:cmd("cd " ++ DevPath ++ " && " ++ Cmd),
    [_Results, File] = string:tokens(Output, " \n"),
    File.

list_files(TarGz) ->
    Output = os:cmd("tar tf "++TarGz),
    string:tokens(Output, " \n").

trim_dir_prefix(Files) ->
    lists:map(fun(File) ->
                      [_Prefix|List] = string:tokens(File, "/"),
                      string:join(List, "/")
              end
              ,Files).

minimum_necessary_files()  ->
    [
     "commands/riak_cs_ping",
     "commands/riak_cs_version",
     "commands/riak_cs_status",
     "commands/riak_cs_gc_status",
     "commands/riak_cs_storage_status",
     "commands/riak_cs_multibag_weight",
     "commands/riak_cs_multibag_list_bags",
     "commands/disk_by_id",
     "commands/w",
     "commands/date",
     "commands/diskstats",
     "commands/iostat_linux",
     "commands/hostname",
     "commands/meminfo",
     "commands/schedulers",
     "commands/cpuinfo",
     "commands/limits.conf",
     "commands/ifconfig",
     "commands/uname",
     "commands/rx_crc_errors",
     "commands/df_i",
     "commands/vmstat",
     "commands/netstat_rn",
     "commands/netstat_i",
     "commands/cluster-info",
     "commands/mount",
     "commands/dmesg",
     "commands/swapon",
     "commands/iptables_nat",
     "commands/df",
     "commands/sysctl",
     "commands/last",
     "commands/iptables_rules",
     "commands/free",
     "commands/ps",
     "commands/cluster-info.html",
     "commands/netstat_an",
     "config/advanced.config",
     "config/riak-cs.conf"
    ].

minimum_necessary_files_after_boot()  ->
    minimum_necessary_files() ++
    [
     "logs/platform_log_dir/console.log",
     "logs/platform_log_dir/run_erl.log",
     "logs/platform_log_dir/erlang.log.1",
     "logs/platform_log_dir/crash.log",
     "logs/platform_log_dir/error.log"
    ].
