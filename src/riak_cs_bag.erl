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

-module(riak_cs_bag).

-export([pool_specs/1]).
-export([pool_name/2, default_bag_id/1,
         assign_bag_id/1, set_bag_id_to_manifest/2,
         bag_id_from_manifest/1]).

-export([is_multi_bag_ebabled/0,
         pool_status/0,
         tab_info/0]).

-export_type([allocate_type/0, pool_key/0, pool_type/0, bag_id/0, weight_info/0]).

-define(ETS_TAB, ?MODULE).
-record(pool, {key :: pool_key(),
               type :: pool_type(), % for match spec
               ip :: string(),
               port :: non_neg_integer(),
               name :: atom()}).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include("riak_cs.hrl").
-include("riak_cs_bag.hrl").

-type allocate_type() :: manifest | block.
-type pool_type() :: request_pool | bucket_list_pool.
%% Use "bag ID" instead of "cluster ID".
%% There are more than one clusters in case of MDC.
-type bag_id() :: binary().
-type pool_key() :: {pool_type(), bag_id()}.
-type weight_info() :: #weight_info{}.

-spec is_multi_bag_ebabled() -> boolean().
is_multi_bag_ebabled() ->
    application:get_env(riak_cs, multi_bag) =/= undefined.

%% Return pool specs from application configuration.
%% This function assumes that it is called ONLY ONCE at initialization.
%% TODO: return specs from ETS after initialization?
-spec pool_specs(term()) -> [{atom(), {non_neg_integer(), non_neg_integer()}}].
pool_specs(MasterPoolConfig) ->
    init_ets(),
    case application:get_env(riak_cs, multi_bag) of
        undefined ->
            [];
        {ok, BagConfig} ->
            register_pools(BagConfig, BagConfig, MasterPoolConfig, [])
    end.

register_pools(_OriginalBagConfig, _BagConfig, [], PoolSpecs) ->
    PoolSpecs;
register_pools(OriginalBagConfig, [],
               [_ | MasterPoolConfigRest],
               PoolSpecs) ->
    register_pools(OriginalBagConfig, OriginalBagConfig,
                   MasterPoolConfigRest, PoolSpecs);
register_pools(OriginalBagConfig, [{BagIdStr, Address, Port} | Bags],
               [{PoolType, PoolSize} | _] = MasterPoolConfig,
               PoolSpecs) ->
    BagId = list_to_binary(BagIdStr),
    lager:debug("{PoolType, BagId, Address, Port}: ~p~n",
                [{PoolType, BagId, Address, Port}]),
    NewPoolSpec = {
      register_and_get_pool_name(PoolType, BagId, Address, Port),
      PoolSize, {Address, Port}},
    register_pools(OriginalBagConfig, Bags,
                   MasterPoolConfig, [NewPoolSpec | PoolSpecs]).

%% Translate bag ID in buckets and manifests to pool name.
-spec pool_name(pool_type(),
                undefined | riakc_obj:riakc_obj() | lfs_manifest() | cs_bucket()) ->
                       atom().
pool_name(request_pool = PoolType, Manifest) when is_record(Manifest, ?MANIFEST_REC) ->
    bag_pool_name(PoolType, bag_id_from_manifest(Manifest));
pool_name(request_pool = PoolType, ?RCS_BUCKET{} = Bucket) ->
    bag_pool_name(PoolType, bag_id_from_cs_bucket(Bucket));
pool_name(PoolType, BucketObj) ->
    bag_pool_name(PoolType, bag_id_from_bucket(BucketObj)).

%% 'undefined' in second argument means buckets and manifests were stored
%% under single bag configuration.
bag_pool_name(_Type, undefined) ->
    undefined;
bag_pool_name(Type, BagId) when is_binary(BagId) ->
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
            undefined;
        _ ->
            case lists:keyfind(block_bag, 1, Props) of
                false -> undefined;
                {block_bag, BagId} -> BagId
            end
    end.

-spec bag_id_from_cs_bucket(cs_bucket()) -> undefined | bag_id().
bag_id_from_cs_bucket(?RCS_BUCKET{manifest_bag=undefined}) ->
    undefined;
bag_id_from_cs_bucket(?RCS_BUCKET{manifest_bag=BagId}) ->
    BagId.

-spec bag_id_from_bucket(riakc_obj:riakc_obj()) -> undefined | bag_id().
bag_id_from_bucket(BucketObj) ->
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

-spec default_bag_id(pool_type()) -> undefined | bag_id().
default_bag_id(Type) ->
    case application:get_env(riak_cs, default_bag) of
        undefined ->
            undefined;
        {ok, DefaultBags} ->
            case lists:keyfind(Type, 1, DefaultBags) of
                false ->
                    undefined;
                {Type, BagId} ->
                    list_to_binary(BagId)
            end
    end.

%% Choose bag ID for new bucket or new manifest
-spec assign_bag_id(allocate_type()) -> undefined | bag_id().
assign_bag_id(Type) ->
    case is_multi_bag_ebabled() of
        false ->
            undefined;
        true ->
            {ok, BagId} = riak_cs_bag_server:allocate(Type),
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

-spec register_and_get_pool_name(pool_type(), bag_id(),
                                 string(), bag_id()) -> atom().
register_and_get_pool_name(PoolType, BagId, IP, Port) ->
    %% TODO: Better to check bag_id for safety
    %%       Or get bag_id on the fly?
    Name = list_to_atom(lists:flatten(io_lib:format("~s:~s", [PoolType, BagId]))),
    ets:insert(?ETS_TAB, #pool{key = {PoolType, BagId},
                               type = PoolType,
                               ip = IP,
                               port = Port,
                               name = Name}),
    Name.

%% For Debugging

tab_info() ->
    ets:tab2list(?ETS_TAB).

pool_status() ->
    [{Type, BagId, Name, poolboy:status(Name)} ||
        #pool{key={Type, BagId}, name=Name} <- ets:tab2list(?ETS_TAB)].
