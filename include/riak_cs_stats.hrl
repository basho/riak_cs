%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

-define(WITH_STATS(StatsKey, Statement),
        begin
            _ = riak_cs_stats:inflow(StatsKey),
            StartTime__with_stats = os:timestamp(),
            Result__with_stats = Statement,
            _ = riak_cs_stats:update_with_start(StatsKey, StartTime__with_stats,
                                                Result__with_stats),
            Result__with_stats
        end).
