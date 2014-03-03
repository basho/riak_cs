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

%% @doc Support multi Riak clusters in single Riak CS system

-module(riak_cs_mc).

-export([pool_specs/0]).
-export([pool_name/2, default_bag_id/1,
         assign_bag_id/1, set_bag_id_to_manifest/2,
         bag_id_from_manifest/1]).

-export([tab_info/0]).

-export_type([pool_key/0, pool_type/0, bag_id/0]).

-type pool_type() :: blocks | manifests.
%% Use "bag ID" instead of "cluster ID".
%% There are more than one clusters in case of MDC.
-type bag_id() :: binary().
-type pool_key() :: {pool_type(), bag_id()}.

%% FIXME: hardcoded, use values of connection_pools?
-define(WORKERS, 128).
-define(OVERFLOW, 0).
-define(ETS_TAB, ?MODULE).
-record(pool, {key :: pool_key(),
               type :: pool_type(), % for match spec
               ip :: string(),
               port :: non_neg_integer(),
               name :: atom()}).

-include_lib("stdlib/include/ms_transform.hrl").
-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

%% Return pool specs from application configuration.
%% This function assumes that it is called ONLY ONCE at initialization.
%% TODO: return specs from ETS after initialization?
-spec pool_specs() -> [{atom(), {non_neg_integer(), non_neg_integer()}}].
pool_specs() ->
    init_ets(),
    BlockPools = register_bags(block, block_bags, default_block_bag),
    ManifestPools = register_bags(manifest, manifest_bags, default_manifest_bag),
    BlockPools ++ ManifestPools.

register_bags(Type, PoolConfigName, DefaultConfigName) ->
    Pools = case application:get_env(riak_cs, PoolConfigName) of
                undefined ->
                    application:set_env(riak_cs, DefaultConfigName, undefined),
                    [];
                [] ->
                    application:set_env(riak_cs, DefaultConfigName, undefined),
                    [];
                {ok, Bags} ->
                    register_props(Type, Bags, [])
            end,
    Pools.

register_props(_, [], Names) ->
    Names;
register_props(Type, [{BagId, Address, Port} | Rest], PoolSpecs) ->
    lager:debug("{BagId, Type, Address, Port}: ~p~n", [{BagId, Type, Address, Port}]),
    NewPoolSpec = {register_and_get_pool_name(Type, BagId, Address, Port),
                   {?WORKERS, ?OVERFLOW, Address, Port}},
    register_props(Type, Rest, [NewPoolSpec | PoolSpecs]).

%% Translate bag ID in buckets and manifests to pool name.
-spec pool_name(pool_type(),
                undefined | riakc_obj:riakc_obj() | lfs_manifest() | cs_bucket()) ->
                       atom().
pool_name(block, Manifest) when is_record(Manifest, ?MANIFEST_REC) ->
    bag_pool_name(block, bag_id_from_manifest(Manifest));
pool_name(manifest, ?RCS_BUCKET{} = Bucket) ->
    bag_pool_name(manifest, bag_id_from_cs_bucket(Bucket));
pool_name(manifest, BucketObj) ->
    bag_pool_name(manifest, bag_id_from_bucket(BucketObj)).

%% 'undefined' in second argument means buckets and manifests were stored
%% under single bag configuration.
bag_pool_name(_Type, undefined) ->
    undefined;
bag_pool_name(Type, BagId) when is_binary(BagId) ->
    lager:debug("Type: ~p~n", [Type]),
    case ets:lookup(?ETS_TAB, {Type, BagId}) of
        [] ->
            %% TODO: Misconfiguration? Should throw error?
            %% Another possibility is number of bags are reduced.
            undefined;
        [#pool{name = Name}] ->
            Name
    end.

-spec bag_id_from_manifest(lfs_manifest()) -> undefined | bag_id().
bag_id_from_manifest(?MANIFEST{props = Props}) ->
    case Props of
        undefined ->
            application:get_env(riak_cs, default_block_bag);
        _ ->
            proplists:get_value(block_bag, Props)
    end.

-spec bag_id_from_cs_bucket(cs_bucket()) -> undefined | bag_id().
bag_id_from_cs_bucket(?RCS_BUCKET{manifest_bag=undefined}) ->
    application:get_env(riak_cs, default_manifest_bag);
bag_id_from_cs_bucket(?RCS_BUCKET{manifest_bag=BagId}) ->
    BagId.

-spec bag_id_from_bucket(riakc_obj:riakc_obj()) -> undefined | bag_id().
bag_id_from_bucket(BucketObj) ->
    lager:debug("BucketObj: ~p~n", [BucketObj]),
    Contents = riakc_obj:get_contents(BucketObj),
    bag_id_from_contents(Contents).

bag_id_from_contents([]) ->
    application:get_env(riak_cs, default_manifest_bag);
bag_id_from_contents([{MD, _} | Contents]) ->
    case bag_id_from_meta(dict:fetch(?MD_USERMETA, MD)) of
        undefined ->
            bag_id_from_contents(Contents);
        BagId ->
            BagId
    end.

bag_id_from_meta([]) ->
    undefined;
bag_id_from_meta([{?MD_BAG, Value} | _]) ->
    binary_to_term(Value);
bag_id_from_meta([_MD | MDs]) ->
    bag_id_from_meta(MDs).

-spec default_bag_id(pool_type()) -> bag_id().
default_bag_id(block) ->
    application:get_env(riak_cs, default_block_bag_id);
default_bag_id(manifest) ->
    application:get_env(riak_cs, default_manifest_bag_id).

%% Choose bag ID for new bucket or new manifest
assign_bag_id(Type) ->
    case ets:first(?ETS_TAB) of
        %% single bag
        '$end_of_table' ->
            undefined;
        %% multiple bags
        _Key ->
            {ok, BagId} = riak_cs_mc_server:allocate(Type),
            BagId
    end.

%% Choose bag ID to store blocks for new manifest and
%% return new manifest
-spec set_bag_id_to_manifest(bag_id() | undefined, lfs_manifest()) -> lfs_manifest().
set_bag_id_to_manifest(undefined, Manifest) ->
    Manifest;
set_bag_id_to_manifest(BagId, ?MANIFEST{props = Props} = Manifest) ->
    Manifest?MANIFEST{props = [{block_bag, BagId} | Props]}.

init_ets() ->
    ets:new(?ETS_TAB, [{keypos, 2}, named_table, protected,
                       {read_concurrency, true}]).

-spec register_and_get_pool_name(pool_type(), string(),
                                 non_neg_integer(), bag_id()) -> atom().
register_and_get_pool_name(Type, BagId, IP, Port) ->
    %% TODO: Better to check bag_id for safety
    %%       Or get bag_id on the fly?
    %% TODO(shino): IP and Port are better than BagId?
    %%              Or just serial number?
    Name = list_to_atom(lists:flatten(io_lib:format("~s:~s", [Type, BagId]))),
    ets:insert(?ETS_TAB, #pool{key = {Type, BagId},
                               type = Type,
                               ip = IP,
                               port = Port,
                               name = Name}),
    Name.

%% For Debugging

tab_info() ->
    ets:tab2list(?ETS_TAB).
