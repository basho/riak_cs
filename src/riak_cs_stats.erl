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
-export([safe_update/2,
         update/2,
         update_with_start/2,
         report_json/0,
         report_pretty_json/0,
         get_stats/0]).

-export([init/0]).

-type metric_name() :: list(atom()).
-export_type([metric_name/0]).

-define(METRICS,
        %% [{metric_name(), exometer:type(), [exometer:option()], Aliases}]
        [{[block, get], spiral, [],
          [{one, block_gets}, {count, block_gets_total}]},
         {[block, get, retry], spiral, [],
          [{one, block_gets_retry}, {count, block_gets_retry_total}]},
         {[block, put], spiral, [],
          [{one, block_puts}, {count, block_puts_total}]},
         {[block, delete], spiral, [],
          [{one, block_deletes}, {count, block_deletes_total}]},

         {[block, get, time], histogram, [],
          [{mean  , block_get_time_mean},
           {median, block_get_time_median},
           {95    , block_get_time_95},
           {99    , block_get_time_99},
           {100   , block_get_time_100}]},
         {[block, get, retry, time], histogram, [],
          [{mean  , block_get_retry_time_mean},
           {median, block_get_retry_time_median},
           {95    , block_get_retry_time_95},
           {99    , block_get_retry_time_99},
           {100   , block_get_retry_time_100}]},
         {[block, put, time], histogram, [],
          [{mean  , block_put_time_mean},
           {median, block_put_time_median},
           {95    , block_put_time_95},
           {99    , block_put_time_99},
           {100   , block_put_time_100}]},
         {[block, delete, time], histogram, [],
          [{mean  , block_delete_time_mean},
           {median, block_delete_time_median},
           {95    , block_delete_time_95},
           {99    , block_delete_time_99},
           {100   , block_delete_time_100}]},

         {[service, get, buckets], spiral, [],
          [{one, service_get_buckets},
           {count, service_get_buckets_total}]},
         {[service, get, buckets, time], histogram, [],
          [{mean  , service_get_buckets_time_mean},
           {median, service_get_buckets_time_median},
           {95    , service_get_buckets_time_95},
           {99    , service_get_buckets_time_99},
           {100   , service_get_buckets_time_100}]},

         {[bucket, list_keys], spiral, [],
          [{one, bucket_list_keys}, {count, bucket_list_keys_total}]},
         {[bucket, create], spiral, [],
          [{one, bucket_creates}, {count, bucket_creates_total}]},
         {[bucket, delete], spiral, [],
          [{one, bucket_deletes}, {count, bucket_deletes_total}]},
         {[bucket, get_acl], spiral, [],
          [{one, bucket_get_acl}, {count, bucket_get_acl_total}]},
         {[bucket, put_acl], spiral, [],
          [{one, bucket_put_acl}, {count, bucket_put_acl_total}]},
         {[bucket, put_policy], spiral, [],
          [{one, bucket_put_policy}, {count, bucket_put_policy_total}]},

         {[bucket, list_keys, time], histogram, [],
          [{mean  , bucket_list_keys_time_mean},
           {median, bucket_list_keys_time_median},
           {95    , bucket_list_keys_time_95},
           {99    , bucket_list_keys_time_99},
           {100   , bucket_list_keys_time_100}]},
         {[bucket, create, time], histogram, [],
          [{mean  , bucket_create_time_mean},
           {median, bucket_create_time_median},
           {95    , bucket_create_time_95},
           {99    , bucket_create_time_99},
           {100   , bucket_create_time_100}]},
         {[bucket, delete, time], histogram, [],
          [{mean  , bucket_delete_time_mean},
           {median, bucket_delete_time_median},
           {95    , bucket_delete_time_95},
           {99    , bucket_delete_time_99},
           {100   , bucket_delete_time_100}]},
         {[bucket, get_acl, time], histogram, [],
          [{mean  , bucket_get_acl_time_mean},
           {median, bucket_get_acl_time_median},
           {95    , bucket_get_acl_time_95},
           {99    , bucket_get_acl_time_99},
           {100   , bucket_get_acl_time_100}]},
         {[bucket, put_acl, time], histogram, [],
          [{mean  , bucket_put_acl_time_mean},
           {median, bucket_put_acl_time_median},
           {95    , bucket_put_acl_time_95},
           {99    , bucket_put_acl_time_99},
           {100   , bucket_put_acl_time_100}]},
         {[bucket, put_policy, time], histogram, [],
          [{mean  , bucket_put_policy_time_mean},
           {median, bucket_put_policy_time_median},
           {95    , bucket_put_policy_time_95},
           {99    , bucket_put_policy_time_99},
           {100   , bucket_put_policy_time_100}]},

         {[object, get], spiral, [],
          [{one, object_gets}, {count, object_gets_total}]},
         {[object, put], spiral, [],
          [{one, object_puts}, {count, object_puts_total}]},
         {[object, head], spiral, [],
          [{one, object_heads}, {count, object_heads_total}]},
         {[object, delete], spiral, [],
          [{one, object_deletes}, {count, object_deletes_total}]},
         {[object, get_acl], spiral, [],
          [{one, object_get_acl}, {count, object_get_acl_total}]},
         {[object, put_acl], spiral, [],
          [{one, object_put_acl}, {count, object_put_acl_total}]},

         {[object, get, time], histogram, [],
          [{mean  , object_get_time_mean},
           {median, object_get_time_median},
           {95    , object_get_time_95},
           {99    , object_get_time_99},
           {100   , object_get_time_100}]},
         {[object, put, time], histogram, [],
          [{mean  , object_put_time_mean},
           {median, object_put_time_median},
           {95    , object_put_time_95},
           {99    , object_put_time_99},
           {100   , object_put_time_100}]},
         {[object, head, time], histogram, [],
          [{mean  , object_head_time_mean},
           {median, object_head_time_median},
           {95    , object_head_time_95},
           {99    , object_head_time_99},
           {100   , object_head_time_100}]},
         {[object, delete, time], histogram, [],
          [{mean  , object_delete_time_mean},
           {median, object_delete_time_median},
           {95    , object_delete_time_95},
           {99    , object_delete_time_99},
           {100   , object_delete_time_100}]},
         {[object, get_acl, time], histogram, [],
          [{mean  , object_get_acl_time_mean},
           {median, object_get_acl_time_median},
           {95    , object_get_acl_time_95},
           {99    , object_get_acl_time_99},
           {100   , object_get_acl_time_100}]},
         {[object, put_acl, time], histogram, [],
          [{mean  , object_put_acl_time_mean},
           {median, object_put_acl_time_median},
           {95    , object_put_acl_time_95},
           {99    , object_put_acl_time_99},
           {100   , object_put_acl_time_100}]},

         {[manifest, siblings_bp_sleep], spiral, [],
          [{one, manifest_siblings_bp_sleep},
           {count, manifest_siblings_bp_sleep_total}]},
         {[manifest, siblings_bp_sleep, time], histogram, [],
          [{mean  , manifest_siblings_bp_sleep_time_mean},
           {median, manifest_siblings_bp_sleep_time_median},
           {95    , manifest_siblings_bp_sleep_time_95},
           {99    , manifest_siblings_bp_sleep_time_99},
           {100   , manifest_siblings_bp_sleep_time_100}]}
        ]).

%% ====================================================================
%% API
%% ====================================================================



-spec safe_update(metric_name(), integer()) -> ok | {error, any()}.
safe_update(BaseId, ElapsedUs) ->
    %% Just in case those metrics happen to be not registered; should
    %% be a bug and also should not interrupt handling requests by
    %% crashing.
    try
        update(BaseId, ElapsedUs)
    catch T:E ->
            lager:error("Failed on storing some metrics: ~p,~p", [T,E])
    end.

-spec update(metric_name(), integer()) -> ok | {error, any()}.
update(BaseId, ElapsedUs) ->
    ok = exometer:update([riak_cs|BaseId], 1),
    ok = exometer:update([riak_cs|BaseId]++[time], ElapsedUs).

-spec update_with_start(metric_name(), erlang:timestamp()) ->
                                   ok | {error, any()}.
update_with_start(BaseId, StartTime) ->
    update(BaseId, timer:now_diff(os:timestamp(), StartTime)).

-spec report_json() -> string().
report_json() ->
    lists:flatten(mochijson2:encode({struct, get_stats()})).

-spec report_pretty_json() -> string().
report_pretty_json() ->
    lists:flatten(riak_cs_utils:json_pp_print(report_json())).

-spec get_stats() -> proplists:proplist().
get_stats() ->
    Stats = [raw_report_item(I) || I <- ?METRICS]
        ++ [raw_report_pool(P) || P <- [request_pool, bucket_list_pool]],
    lists:flatten(Stats).

init() ->
    _ = [init_item(I) || I <- ?METRICS],
    ok.

%% ====================================================================
%% Internal
%% ====================================================================

init_item({Name, Type, Opts, Aliases}) ->
    ok = exometer:re_register([riak_cs|Name], Type,
                              [{aliases, Aliases}|Opts]).

raw_report_item({Name, _Type, _Options, Aliases}) ->

    {ok, Values} = exometer:get_value([riak_cs|Name], [D||{D,_Alias}<-Aliases]),
    [{Alias, Value} ||
        {{D, Alias}, {D, Value}} <- lists:zip(Aliases, Values)].

raw_report_pool(Pool) ->
    {_PoolState, PoolWorkers, PoolOverflow, PoolSize} = poolboy:status(Pool),
    Name = binary_to_list(atom_to_binary(Pool, latin1)),
    [{list_to_atom(lists:flatten([Name, $_, "workers"])), PoolWorkers},
     {list_to_atom(lists:flatten([Name, $_, "overflow"])), PoolOverflow},
     {list_to_atom(lists:flatten([Name, $_, "size"])), PoolSize}].



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

stats_metric_test() ->
    [begin
         ?debugVal(Key),
         case lists:last(Key) of
             time ->
                 ?assertEqual(histogram, Type),
                 [?assert(proplists:is_defined(M, Aliases))
                  || M <- [mean, median, 95, 99, 100]];
             _ ->
                 ?assertNotEqual(false, lists:keyfind(Key, 1, ?METRICS)),
                 ?assertEqual(spiral, Type),
                 ?assert(proplists:is_defined(one, Aliases)),
                 ?assert(proplists:is_defined(count, Aliases))
         end,
         ?assertEqual([], Options)
     end || {Key, Type, Options, Aliases} <- ?METRICS].

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
                            %% ?debugVal(Key),
                            case lists:last(Key) of
                                time -> ok;
                                _ -> riak_cs_stats:update(Key, 16#deadbeef)
                            end
                    end || {Key, _, _, _} <- ?METRICS]},
     fun() ->
             [begin
                  Items = raw_report_item(I),
                  %% ?debugVal(Items),
                  case length(Items) of
                      2 ->
                          ?assertEqual([1, 1],
                                       [N || {_, N} <- Items]);
                      5 ->
                          ?assertEqual([16#deadbeef, 16#deadbeef, 16#deadbeef, 16#deadbeef, 0],
                                       [N || {_, N} <- Items])
                  end
              end || I <- ?METRICS]
     end]}.

-endif.
