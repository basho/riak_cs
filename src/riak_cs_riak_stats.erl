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

%% @doc Helpers for Riak node stats (via Riak HTTP API)

-module(riak_cs_riak_stats).

-export([
         riak_stats/0,
         get_riak_stat/1,
         get_riak_stat/2,
         get_riak_disk_stat/0,
         get_riak_disk_stat/1,
         get_riak_root_disk_usage/0,
         get_riak_node_count/0,
         get_riak_cluster_disk_usage/0
]).

riak_stats() ->
    Rhc = riak_cs_riak_http_client:get_rhc(),
    Rhc:get_server_stats().

get_riak_stat(Key) ->
    case riak_stats() of
        {ok, Stats} ->
            get_riak_stat(Stats, Key);            
        {error, Error} ->
            {error, Error}
    end.

get_riak_stat(Stats, Key) ->
    proplists:get_value(Key, Stats).

get_riak_disk_stat() ->
    get_riak_stat(<<"disk">>).

get_riak_disk_stat(PartitionKey) ->
    DiskStats = get_riak_disk_stat(),
    DiskStatProplist = [ {PartitionId, [{<<"id">>, PartitionId} | PartitionStats]} 
                            || {struct, [{<<"id">>, PartitionId} | PartitionStats]} <- DiskStats ],
    [_PartitionId, {<<"size">>, PartitionTotalKB}, {<<"used">>, PercentageUsed}] = 
        proplists:get_value(PartitionKey, DiskStatProplist),
    {PartitionKey, PartitionTotalKB, PercentageUsed}.

get_riak_root_disk_usage() ->
    RootPartition = <<"/">>,  % TODO: assume/hardcode for now, load from config or discover later
    get_riak_disk_stat(RootPartition).

get_riak_node_count() ->
    NodeList = get_riak_stat(<<"ring_members">>),
    length(NodeList).

get_riak_cluster_disk_usage() ->
    {_PartitionKey, PartitionTotalKB, PercentageUsed} = get_riak_root_disk_usage(),
    PartitionUsedKB = round(PartitionTotalKB * PercentageUsed / 100),
    % PartitionFreeKB = PartitionTotalKB - PartitionUsedKB,
    NVal = 3,  % TODO: hardcode for now, discover via api later
    NodeCount = get_riak_node_count(),
    ClusterCapacityKB = PartitionTotalKB * NodeCount,
    ClusterDiskUsageKB = PartitionUsedKB * NodeCount,
    ClusterDiskFreeKB = ClusterCapacityKB - ClusterDiskUsageKB,
    ObjectCapacityRemainingKB = round(ClusterDiskFreeKB / NVal),
    {struct, 
    [{<<"cluster_capacity">>, ClusterCapacityKB},
     {<<"cluster_disk_usage_kb">>, ClusterDiskUsageKB},
     {<<"cluster_disk_free_kb">>, ClusterDiskFreeKB},
     {<<"cluster_node_count">>, NodeCount},
     {<<"n_val">>, NVal},
     {<<"object_storage_capacity_remaining_kb">>, ObjectCapacityRemainingKB}
     ]
    }.
