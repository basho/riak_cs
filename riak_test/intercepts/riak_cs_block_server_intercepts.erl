%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

-module(riak_cs_block_server_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_cs_block_server_orig).

get_block_local_timeout(_RcPid, _FullBucket, _FullKey, _GetOptions, _Timeout, _StatsKey) ->
    {error, timeout}.

get_block_local_insufficient_vnode_at_nval1(RcPid, FullBucket, FullKey, GetOptions, Timeout, StatsKey) ->
    case proplists:get_value(n_val, GetOptions) of
        1 ->
            ?I_INFO("riak_cs_block_server:get_block_local/6 returns insufficient_vnodes"),
            {error, <<"{insufficient_vnodes,0,need,1}">>};
        N ->
            ?I_INFO("riak_cs_block_server:get_block_local/6 forwards original code with n_val=~p", [N]),
            ?M:get_block_local_orig(RcPid, FullBucket, FullKey, GetOptions, Timeout, StatsKey)
    end.
