%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_stats).

%% API
-export([
         inflow/1,
         update_with_start/3,
         update_with_start/2,
         update_error_with_start/2,
         update/2,
         countup/1,
         report_json/0,
         report_pretty_json/0,
         get_stats/0]).

%% Lower level API, mainly for debugging or investigation from shell
-export([report_exometer_item/3,
         report_pool/0,
         report_pool/1,
         report_mochiweb/0,
         report_memory/0,
         report_system/0,
         system_monitor_count/0,
         system_version/0,
         system_architecture/0]).

-export([init/0]).

-type key() :: [atom()].
-export_type([key/0]).

-type ok_error_res() :: ok | {ok, _} | {error, _}.

-spec duration_metrics() -> [key()].
duration_metrics() ->
        [
         [service, get],

         [bucket, put],
         [bucket, head],
         [bucket, delete],
         [bucket_acl, get],
         [bucket_acl, put],
         [bucket_policy, get],
         [bucket_policy, put],
         [bucket_policy, delete],
         [bucket_location, get],
         [bucket_versioning, get],
         [bucket_versioning, put],
         [bucket_request_payment, get],
         [list_uploads, get],
         [multiple_delete, post],
         [list_objects, get],
         [list_object_versions, get],

         [object, get],
         [object, put],
         [object, put_copy],
         [object, head],
         [object, delete],
         [object_acl, get],
         [object_acl, put],

         [multipart, post],            % Initiate
         [multipart_upload, put],      % Upload Part
         [multipart_upload, put_copy], % Upload Part (Copy)
         [multipart_upload, post],     % Complete
         [multipart_upload, delete],   % Abort
         [multipart_upload, get],      % List Parts

         [velvet, create_user],
         [velvet, update_user],
         [velvet, create_bucket],
         [velvet, delete_bucket],
         [velvet, set_bucket_acl],
         [velvet, set_bucket_policy],
         [velvet, delete_bucket_policy],
         [velvet, set_bucket_versioning],

         %% Riak PB client, per key operations
         [riakc, ping],
         [riakc, get_cs_bucket],
         [riakc, get_cs_user_strong],
         [riakc, get_cs_user],
         [riakc, put_cs_user],
         [riakc, get_cs_role],

         [riakc, get_manifest],
         [riakc, put_manifest],
         [riakc, delete_manifest],

         [riakc, get_block_n_one],
         [riakc, get_block_n_all],
         [riakc, get_block_remote],
         [riakc, get_block_legacy],
         [riakc, get_block_legacy_remote],
         [riakc, put_block],
         [riakc, put_block_resolved],
         [riakc, head_block],
         [riakc, delete_block_constrained],
         [riakc, delete_block_secondary],

         [riakc, get_gc_manifest_set],
         [riakc, put_gc_manifest_set],
         [riakc, delete_gc_manifest_set],
         [riakc, get_access],
         [riakc, put_access],
         [riakc, get_storage],
         [riakc, put_storage],

         %% Riak PB client, coverage operations
         [riakc, fold_manifest_objs],
         [riakc, mapred_storage],
         [riakc, list_all_user_keys],
         [riakc, list_all_bucket_keys],
         [riakc, list_all_manifest_keys],
         [riakc, list_users_receive_chunk],
         [riakc, get_uploads_by_index],
         [riakc, get_user_by_index],
         [riakc, get_gc_keys_by_index],
         [riakc, get_cs_buckets_by_index],

         %% Riak PB client, misc
         [riakc, get_clusterid]
        ].

counting_metrics() ->
    [
         %% Almost deprecated
         [manifest, siblings_bp_sleep]
    ].

duration_subkeys() ->
    [{[in], spiral},
     {[out], spiral},
     {[time], histogram},
     {[out, error], spiral},
     {[time, error], histogram}].

counting_subkeys() ->
    [{[], spiral}].

%% ====================================================================
%% API
%% ====================================================================

-spec inflow(key()) -> ok.
inflow(Key) ->
    safe_update([riak_cs, in | Key], 1).

-spec update_with_start(key(), erlang:timestamp(), ok_error_res()) -> ok.
update_with_start(Key, StartTime, ok) ->
    update_with_start(Key, StartTime);
update_with_start(Key, StartTime, {ok, _}) ->
    update_with_start(Key, StartTime);
update_with_start(Key, StartTime, {error, _}) ->
    update_error_with_start(Key, StartTime).

-spec update_with_start(key(), erlang:timestamp()) -> ok.
update_with_start(Key, StartTime) ->
    update(Key, timer:now_diff(os:timestamp(), StartTime)).

-spec update_error_with_start(key(), erlang:timestamp()) -> ok.
update_error_with_start(Key, StartTime) ->
    update([error | Key], timer:now_diff(os:timestamp(), StartTime)).

-spec countup(key()) -> ok.
countup(Key) ->
    safe_update([riak_cs | Key], 1).

-spec report_json() -> string().
report_json() ->
    lists:flatten(mochijson2:encode({struct, get_stats()})).

-spec report_pretty_json() -> string().
report_pretty_json() ->
    lists:flatten(riak_cs_utils:json_pp_print(report_json())).

-spec get_stats() -> proplists:proplist().
get_stats() ->
    DurationStats =
        [report_exometer_item(Key, SubKey, ExometerType) ||
            Key <- duration_metrics(),
            {SubKey, ExometerType} <- duration_subkeys()],
    CountingStats =
        [report_exometer_item(Key, SubKey, ExometerType) ||
            Key <- counting_metrics(),
            {SubKey, ExometerType} <- counting_subkeys()],
    lists:flatten([DurationStats, CountingStats,
                   report_pool(), report_mochiweb(),
                   report_memory(), report_system()]).

%% ====================================================================
%% Internal
%% ====================================================================

init() ->
    _ = [init_duration_item(I) || I <- duration_metrics()],
    _ = [init_counting_item(I) || I <- counting_metrics()],
    ok.

init_duration_item(Key) ->
    [ok = exometer:re_register([riak_cs | SubKey ++ Key], ExometerType, []) ||
        {SubKey, ExometerType} <- duration_subkeys()].

init_counting_item(Key) ->
    [ok = exometer:re_register([riak_cs | SubKey ++ Key], ExometerType, []) ||
        {SubKey, ExometerType} <- counting_subkeys()].

-spec update(key(), integer()) -> ok.
update(Key, ElapsedUs) ->
    safe_update([riak_cs, out | Key], 1),
    safe_update([riak_cs, time | Key], ElapsedUs).

safe_update(ExometerKey, Arg) ->
    case exometer:update(ExometerKey, Arg) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("Stats update for key ~p error: ~p", [ExometerKey, Reason]),
            ok
    end.

-spec report_exometer_item(key(), [atom()], exometer:type()) -> [{atom(), integer()}].
report_exometer_item(Key, SubKey, ExometerType) ->
    AtomKeys = [metric_to_atom(Key ++ SubKey, Suffix) ||
                   Suffix <- suffixes(ExometerType)],
    {ok, Values} = exometer:get_value([riak_cs | SubKey ++ Key],
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

-spec report_pool() -> [[{atom(), integer()}]].
report_pool() ->
    Pools = [request_pool, bucket_list_pool | riak_cs_riak_client:pbc_pools()],
    [report_pool(Pool) || Pool <- Pools].

-spec report_pool(atom()) -> [{atom(), integer()}].
report_pool(Pool) ->
    {_PoolState, PoolWorkers, PoolOverflow, PoolSize} = poolboy:status(Pool),
    [{metric_to_atom([Pool], "workers"), PoolWorkers},
     {metric_to_atom([Pool], "overflow"), PoolOverflow},
     {metric_to_atom([Pool], "size"), PoolSize}].

-spec report_mochiweb() -> [[{atom(), integer()}]].
report_mochiweb() ->
    MochiIds = [object_web, admin_web],
    [report_mochiweb(Id) || Id <- MochiIds].

report_mochiweb(Id) ->
    Children = supervisor:which_children(riak_cs_sup),
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

stats_test_() ->
    Apps = [setup, compiler, syntax_tools, exometer_core],
    {setup,
     fun() ->
             [application:ensure_all_started(App) || App <- Apps],
             ok = init()
     end,
     fun(_) ->
             [ok = application:stop(App) || App <- Apps]
     end,
     [{inparallel,
       [fun() ->
                inflow(Key),
                update(Key, 16#deadbeef),
                update([error | Key], 16#deadbeef)
        end || Key <- duration_metrics()]},
      {inparallel,
       [fun() ->
                countup(Key)
        end || Key <- counting_metrics()]},
      fun() ->
              [begin
                   Report = [N || {_, N} <- report_exometer_item(Key, SubKey, ExometerType)],
                   case ExometerType of
                       spiral ->
                           ?assertEqual([1, 1], Report);
                       histogram ->
                           ?assertEqual(
                              [16#deadbeef, 16#deadbeef, 16#deadbeef,
                               16#deadbeef, 16#deadbeef],
                              Report)
                   end
               end || Key <- duration_metrics(),
                      {SubKey, ExometerType} <- duration_subkeys()],
              [begin
                   Report = [N || {_, N} <- report_exometer_item(Key, SubKey, ExometerType)],
                   case ExometerType of
                       spiral ->
                           ?assertEqual([1, 1], Report)
                   end
               end || Key <- counting_metrics(),
                      {SubKey, ExometerType} <- counting_subkeys()]
      end]}.

-endif.
