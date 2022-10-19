%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(stanchion_stats).

%% API
-export([safe_update/2,
         update/2,
         update/3,
         update_with_start/2,
         report_json/0,
         %% report_pretty_json/0,
         get_stats/0]).

-export([init/0]).

-type key() :: [atom()].
-export_type([key/0]).

-spec duration_metrics() -> [key()].
duration_metrics() ->
    [
     [bucket, create],
     [bucket, delete],
     [bucket, put_acl],
     [bucket, set_policy],
     [bucket, delete_policy],
     [bucket, set_versioning],
     [user, create],
     [user, update],

     %% Riak PB client, per key operations
     [riakc, get_cs_bucket],
     [riakc, put_cs_bucket], %% Always strong put
     [riakc, get_cs_user_strong],
     [riakc, put_cs_user],
     [riakc, get_manifest],

     %% Riak PB client, coverage operations
     [riakc, list_all_manifest_keys],
     [riakc, get_user_by_index]
    ].

duration_only_metrics() ->
    [[waiting]].

duration_subkeys() ->
    [{[], spiral},
     {[time], histogram}].

duration_only_subkeys() ->
    [{[time], histogram}].

%% ====================================================================
%% API
%% ====================================================================



-spec safe_update(key(), integer()) -> ok | {error, any()}.
safe_update(BaseId, ElapsedUs) ->
    %% Just in case those metrics happen to be not registered; should
    %% be a bug and also should not interrupt handling requests by
    %% crashing.
    try
        update(BaseId, ElapsedUs)
    catch T:E ->
            lager:error("Failed on storing some metrics: ~p,~p", [T,E])
    end.

-spec update(key(), integer()) -> ok | {error, any()}.
update(BaseId, ElapsedUs) ->
    _ = lager:debug("Updating ~p (~p)", [BaseId, ElapsedUs]),
    ok = exometer:update([stanchion|BaseId], 1),
    ok = exometer:update([stanchion,time|BaseId], ElapsedUs).

update(BaseId, ServiceTime, WaitingTime) ->
    update(BaseId, ServiceTime),
    ok = exometer:update([stanchion, time, waiting], WaitingTime).

-spec update_with_start(key(), erlang:timestamp()) ->
                                   ok | {error, any()}.
update_with_start(BaseId, StartTime) ->
    update(BaseId, timer:now_diff(os:timestamp(), StartTime)).

-spec report_json() -> string().
report_json() ->
    lists:flatten(mochijson2:encode({struct, get_stats()})).

all_metrics() ->
    Metrics0 = [{Subkey ++ Metric, Type, [], []}
                || Metric <- duration_only_metrics(),
                   {Subkey, Type} <- duration_only_subkeys()],
    Metrics1 = [{Subkey ++ Metric, Type, [], []}
               || Metric <- duration_metrics(),
                  {Subkey, Type} <- duration_subkeys()],
    Metrics0 ++ Metrics1.

-spec get_stats() -> proplists:proplist().
get_stats() ->
    DurationStats =
        [report_exometer_item(Key, SubKey, ExometerType) ||
            Key <- duration_metrics(),
            {SubKey, ExometerType} <- duration_subkeys()],
    DurationOnlyStats =
        [report_exometer_item(Key, SubKey, ExometerType) ||
            Key <- duration_only_metrics(),
            {SubKey, ExometerType} <- duration_only_subkeys()],
    MsgStats = [{stanchion_server_msgq_len, stanchion_server:msgq_len()}],
    lists:flatten([DurationStats, MsgStats, DurationOnlyStats,
                   report_mochiweb(), report_memory(),
                   report_system()]).

init() ->
    _ = [init_item(I) || I <- all_metrics()],
    ok.

%% ====================================================================
%% Internal
%% ====================================================================

init_item({Name, Type, Opts, _Aliases}) ->
    ok = exometer:re_register([stanchion|Name], Type, Opts).

-spec report_exometer_item(key(), [atom()], exometer:type()) -> [{atom(), integer()}].
report_exometer_item(Key, SubKey, ExometerType) ->
    _ = lager:debug("~p", [{Key, SubKey, ExometerType}]),
    AtomKeys = [metric_to_atom(Key ++ SubKey, Suffix) ||
                   Suffix <- suffixes(ExometerType)],
    {ok, Values} = exometer:get_value([stanchion | SubKey ++ Key],
                                      datapoints(ExometerType)),
    [{AtomKey, Value} ||
        {AtomKey, {_DP, Value}} <- lists:zip(AtomKeys, Values)].

datapoints(histogram) ->
    [mean, median, 95, 99, max];
datapoints(spiral) ->
    [one, count].

suffixes(histogram) ->
    ["mean", "median", "95", "99", "100"];
suffixes(spiral) ->
    ["one", "total"].

-spec report_mochiweb() -> [[{atom(), integer()}]].
report_mochiweb() ->
    [report_mochiweb(webmachine_mochiweb)].

report_mochiweb(Id) ->
    Children = supervisor:which_children(stanchion_sup),
    case lists:keyfind(Id, 1, Children) of
        false -> [];
        {_, Pid, _, _} -> report_mochiweb(Id, Pid)
    end.

report_mochiweb(Id, Pid) ->
    [{metric_to_atom([Id], PropKey), gen_server:call(Pid, {get, PropKey})} ||
        PropKey <- [active_sockets, waiting_acceptors, port]].

-spec report_memory() -> [{atom(), integer()}].
report_memory() ->
    lists:map(fun({K, V}) -> {metric_to_atom([memory], K), V} end, erlang:memory()).

-spec report_system() -> [{atom(), integer()}].
report_system() ->
    [{nodename, erlang:node()},
     {connected_nodes, erlang:nodes()},
     {sys_driver_version, list_to_binary(erlang:system_info(driver_version))},
     {sys_heap_type, erlang:system_info(heap_type)},
     {sys_logical_processors, erlang:system_info(logical_processors)},
     {sys_monitor_count, system_monitor_count()},
     {sys_otp_release, list_to_binary(erlang:system_info(otp_release))},
     {sys_port_count, erlang:system_info(port_count)},
     {sys_process_count, erlang:system_info(process_count)},
     {sys_smp_support, erlang:system_info(smp_support)},
     {sys_system_version, system_version()},
     {sys_system_architecture, system_architecture()},
     {sys_threads_enabled, erlang:system_info(threads)},
     {sys_thread_pool_size, erlang:system_info(thread_pool_size)},
     {sys_wordsize, erlang:system_info(wordsize)}].

system_monitor_count() ->
    lists:foldl(fun(Pid, Count) ->
                        case erlang:process_info(Pid, monitors) of
                            {monitors, Mons} ->
                                Count + length(Mons);
                            _ ->
                                Count
                        end
                end, 0, processes()).

system_version() ->
    list_to_binary(string:strip(erlang:system_info(system_version), right, $\n)).

system_architecture() ->
    list_to_binary(erlang:system_info(system_architecture)).

metric_to_atom(Key, Suffix) when is_atom(Suffix) ->
    metric_to_atom(Key, atom_to_list(Suffix));
metric_to_atom(Key, Suffix) ->
    StringKey = string:join([atom_to_list(Token) || Token <- Key], "_"),
    list_to_atom(lists:flatten([StringKey, $_, Suffix])).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

stats_metric_test() ->
    [begin
         case hd(Key) of
             time ->
                 ?assertEqual(histogram, Type);
             _ ->
                 ?assertNotEqual(false, lists:keyfind(Key, 1, all_metrics())),
                 ?assertEqual(spiral, Type)
         end,
         ?assertEqual([], Options)
     end || {Key, Type, Options, _} <- all_metrics()].

stats_test_() ->
    Apps = [setup, compiler, syntax_tools, exometer_core],
    {setup,
     fun() ->
             [application:ensure_all_started(App) || App <- Apps],
             ok = stanchion_stats:init()
     end,
     fun(_) ->
             [ok = application:stop(App) || App <- Apps]
     end,
     [{inparallel, [fun() ->
                            %% ?debugVal(Key),
                            case hd(Key) of
                                time -> ok;
                                _ -> stanchion_stats:update(Key, 16#deadbeef, 16#deadbeef)
                            end
                    end || {Key, _, _, _} <- all_metrics()]},
     fun() ->
             [begin
                  ?debugVal(Key),
                  CountItems = report_exometer_item(Key, [], spiral),
                  DurationItems = report_exometer_item(Key, [time], histogram),
                  ?debugVal(CountItems),
                  ?debugVal(DurationItems),
                  ?assertMatch([1, 1],
                               [N || {_, N} <- CountItems]),
                  ?assertMatch([16#deadbeef, 16#deadbeef, 16#deadbeef, 16#deadbeef, 16#deadbeef],
                               [N || {_, N} <- DurationItems])
              end || Key <- duration_metrics()]
     end]}.

-endif.
