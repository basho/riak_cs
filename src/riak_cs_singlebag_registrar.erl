%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_singlebag_registrar).

-export([process_specs/0, pool_specs/1, pool_name/3, choose_bag_id/1,
         set_bag_id_to_manifest/2,
         list_pool/1, pool_status/0]).

-behaiviour(riak_cs_bag_registrar).

process_specs() ->
    [].

pool_specs(_Props) ->
    [].

pool_name(_MasterPid, _PoolType, _Target) ->
    {ok, undefined}.

choose_bag_id(_PoolType) ->
    undefined.

set_bag_id_to_manifest(_BagId, Manifest) ->
    Manifest.

list_pool(PoolType) ->
    [{PoolType, PoolType, master, []}].

pool_status() ->
    [].
