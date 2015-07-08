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

-module(riak_cs_stats).

%% API
-export([update/2,
         update_with_start/3,
         update_with_start/2,
         update_error_with_start/2,
         inflow/1,
         report_json/0,
         report_pretty_json/0,
         get_stats/0]).

%% For debugging or investigation from shell
-export([report_duration_item/3,
         raw_report_pool/1]).

-export([init/0]).

-type metric_key() :: [atom()].
-export_type([metric_key/0]).

-spec duration_metrics() -> [metric_key()].
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
         [list_uploads, get],
         [multiple_delete, post],
         [list_objects, get],

         [object, get],
         [object, put],
         [object, put_copy],
         [object, head],
         [object, delete],
         [object_acl, get],
         [object_acl, put],

         [multipart, post],          % Initiate
         [multipart_upload, put],    % Upload Part (Copy)
         [multipart_upload, post],   % Complete
         [multipart_upload, delete], % Abort
         [multipart_upload, get],    % List Parts

         [velvet, create_user],
         [velvet, update_user],
         [velvet, create_bucket],
         [velvet, delete_bucket],
         [velvet, set_bucket_acl],
         [velvet, set_bucket_policy],
         [velvet, delete_bucket_policy],

         %% TODO: Remove backpresure sleep
         [manifest, siblings, bp, sleep],

         [block, get],
         [block, get, retry],
         [block, put],
         [block, delete]
        ].

%% ====================================================================
%% API
%% ====================================================================

-spec inflow(metric_key()) -> ok | {error, any()}.
inflow(Key) ->
    lager:debug("~p:inflow Key: ~p", [?MODULE, Key]),
    ok = exometer:update([riak_cs, in | Key], 1).

-type op_result() :: ok | {ok, _} | {error, _}.
-spec update_with_start(metric_key(), erlang:timestamp(), op_result()) ->
                               ok | {error, any()}.
update_with_start(BaseId, StartTime, ok) ->
    update_with_start(BaseId, StartTime);
update_with_start(BaseId, StartTime, {ok, _}) ->
    update_with_start(BaseId, StartTime);
update_with_start(BaseId, StartTime, {error, _}) ->
    update_error_with_start(BaseId, StartTime).

-spec update_with_start(metric_key(), erlang:timestamp()) ->
                               ok | {error, any()}.
update_with_start(BaseId, StartTime) ->
    update(BaseId, timer:now_diff(os:timestamp(), StartTime)).

-spec update_error_with_start(metric_key(), erlang:timestamp()) ->
                                     ok | {error, any()}.
update_error_with_start(BaseId, StartTime) ->
    update([error | BaseId], timer:now_diff(os:timestamp(), StartTime)).

-spec report_json() -> string().
report_json() ->
    lists:flatten(mochijson2:encode({struct, get_stats()})).

-spec report_pretty_json() -> string().
report_pretty_json() ->
    lists:flatten(riak_cs_utils:json_pp_print(report_json())).

-spec get_stats() -> proplists:proplist().
get_stats() ->
    Stats = [report_duration_item(K, Kind, ExometerType) ||
                K <- duration_metrics(),
                {Kind, ExometerType} <- [{[in], spiral},
                                         {[out], spiral}, {[time], histogram},
                                         {[out, error], spiral}, {[time, error], histogram}]]
        ++ [raw_report_pool(P) || P <- [request_pool, bucket_list_pool]],
    lists:flatten(Stats).

%% ====================================================================
%% Internal
%% ====================================================================

init() ->
    _ = [init_duration_item(I) || I <- duration_metrics()],
    ok.

init_duration_item(Key) ->
    ok = exometer:re_register([riak_cs, in   | Key], spiral, []),
    ok = exometer:re_register([riak_cs, out  | Key], spiral, []),
    ok = exometer:re_register([riak_cs, time | Key], histogram, []),
    ok = exometer:re_register([riak_cs, out,  error | Key], spiral, []),
    ok = exometer:re_register([riak_cs, time, error | Key], histogram, []).

-spec update(metric_key(), integer()) -> ok | {error, any()}.
update(Key, ElapsedUs) ->
    lager:debug("~p:update Key: ~p", [?MODULE, Key]),
    ok = exometer:update([riak_cs, out | Key], 1),
    ok = exometer:update([riak_cs, time | Key], ElapsedUs).

report_duration_item(Key, Kind, ExometerType) ->
    AtomKeys = [metric_to_atom(Key ++ Kind, Suffix) || Suffix <- suffixes(ExometerType)],
    {ok, Values} = exometer:get_value([riak_cs | Kind ++ Key], datapoints(ExometerType)),
    [{AtomKey, Value} ||
        {AtomKey, {_DP, Value}} <- lists:zip(AtomKeys, Values)].

datapoints(histogram) ->
    [mean, median, 95, 99, 100];
datapoints(spiral) ->
    [one, count].

suffixes(histogram) ->
    ["_mean", "_median", "_95", "_99", "_100"];
suffixes(spiral) ->
    ["_one", "_count"].

raw_report_pool(Pool) ->
    {_PoolState, PoolWorkers, PoolOverflow, PoolSize} = poolboy:status(Pool),
    Name = binary_to_list(atom_to_binary(Pool, latin1)),
    [{list_to_atom(lists:flatten([Name, $_, "workers"])), PoolWorkers},
     {list_to_atom(lists:flatten([Name, $_, "overflow"])), PoolOverflow},
     {list_to_atom(lists:flatten([Name, $_, "size"])), PoolSize}].

metric_to_atom(Key, Suffix) ->
    StringKey = string:join([atom_to_list(Token) || Token <- Key], "_"),
    list_to_atom(lists:flatten([StringKey, Suffix])).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

stats_test_() ->
    Apps = [setup, compiler, syntax_tools, goldrush, lager, exometer_core],
    {setup,
     fun() ->
             [ok = application:start(App) || App <- Apps],
             ok = riak_cs_stats:init()
     end,
     fun(_) ->
             [ok = application:stop(App) || App <- Apps]
     end,
     [{inparallel, [fun() ->
                            riak_cs_stats:update(Key, 16#deadbeef)
                    end || Key <- duration_metrics()]},
      fun() ->
              [begin
                   ?assertEqual([1, 1],
                                [N || {_, N} <- report_duration_item(I, spiral)]),
                   ?assertEqual([16#deadbeef, 16#deadbeef, 16#deadbeef, 16#deadbeef, 0],
                                [N || {_, N} <- report_duration_item(I, histogram)])
               end || I <- duration_metrics()]
      end]}.

-endif.
