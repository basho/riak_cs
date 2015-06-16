%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-behaviour(gen_server).

%% API
-export([start_link/0,
         update/2,
         update_with_start/2,
         report_json/0,
         report_pretty_json/0,
         get_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

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
          [{one, service_get_buckets}]},

         {[bucket, list_keys], spiral, [], [{one, bucket_list_keys}]},
         {[bucket, create], spiral, [],   [{one, bucket_creates}]},
         {[bucket, delete], spiral, [],   [{one, bucket_deletes}]},
         {[bucket, get_acl], spiral, [],  [{one, bucket_get_acl}]},
         {[bucket, put_acl], spiral, [],  [{one, bucket_put_acl}]},

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

         {[object, get], spiral, [], [{one, object_gets}]},
         {[object, put], spiral, [], [{one, object_puts}]},
         {[object, head], spiral, [], [{one, object_heads}]},
         {[object, delete], spiral, [], [{one, object_deletes}]},
         {[object, get_acl], spiral, [], [{one, object_get_acl}]},
         {[object, put_acl], spiral, [], [{one, object_put_acl}]},

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
           {100   , object_put_acl_time_100}]}

        ]).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec update(metric_name(), integer()) -> ok | {error, any()}.
update(BaseId, ElapsedUs) ->
    %% Just in case those metrics happen to be not registered; should
    %% be a bug and also should not interrupt handling requests by
    %% crashing.
    try
        ok = exometer:update([riak_cs|BaseId], 1),
        ok = exometer:update([riak_cs|BaseId]++[time], ElapsedUs)
    catch T:E ->
            lager:error("Failed on storing some metrics: ~p,~p", [T,E])
    end.

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

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% There are no need to keep this as gen_server in supervision
    %% tree, should we remove this from the tree, and let it
    %% initialize somewhere like riak_cs_app?
    _ = [init_item(I) || I <- ?METRICS],
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
