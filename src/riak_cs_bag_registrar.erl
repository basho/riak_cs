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

-module(riak_cs_bag_registrar).

-export([process_specs/0, pool_specs/1, pool_name/3, choose_bag_id/1,
         set_bag_id_to_manifest/2,
         list_pool/1, pool_status/0]).
-export([registar_module/0, is_multibag_enabled/0]).

-export_type([bag_id/0, pool_type/0]).

-include("riak_cs.hrl").

-callback process_specs() -> [term()].
-callback pool_specs(proplists:proplists()) ->
    [{atom(), {non_neg_integer(), non_neg_integer()}}].
-callback pool_name(pid(), pool_type(), term()) -> {ok, atom()} | {error, term()}.
-callback choose_bag_id(manifest | block, term()) -> bag_id().
-callback set_bag_id_to_manifest(bag_id(), lfs_manifest()) -> lfs_manifest().
-callback list_pool(pool_type()) -> [{Name::atom(), pool_type(), bag_id()}].
-callback pool_status() -> [term()].
-callback tab_info() -> term().

-type bag_id() :: undefined | binary().
-type pool_type() :: request_pool | bucket_list_pool.

process_specs() ->
    (registar_module()):process_specs().

pool_specs(Props) ->
    (registar_module()):pool_specs(Props).

%% Translate bag ID in buckets and manifests to pool name.
-spec pool_name(pid(), pool_type(), term()) -> {ok, atom()} | {error, term()}.
pool_name(MasterRiakc, PoolType, Target) ->
    (registar_module()):pool_name(MasterRiakc, PoolType, Target).

choose_bag_id(AllocateType) ->
    (registar_module()):choose_bag_id(AllocateType).

set_bag_id_to_manifest(BagId, Manifest) ->
    (registar_module()):set_bag_id_to_manifest(BagId, Manifest).

list_pool(PoolType) ->
    (registar_module()):list_pool(PoolType).

pool_status() ->
    (registar_module()):list_pool().

is_multibag_enabled() ->
    application:get_env(riak_cs_multibag, bags) =/= undefined.

registar_module() ->
    case is_multibag_enabled() of
        true ->
            riak_cs_multibag_registrar;
        false ->
            riak_cs_singlebag_registrar
    end.
