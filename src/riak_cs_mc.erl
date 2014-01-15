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

%% @doc Support multi containers (multi Riak clusters) in single Riak CS system

-module(riak_cs_mc).

-export([pool_specs/0]).
-export([pool_name/2, default_container_id/1,
         assign_container_id/1, assign_container_id/2]).

-export([tab_info/0]).

-export_typ([pool_key/0, pool_type/0, container_id/0]).

-type pool_type() :: blocks | manifests.
%% Use "container ID" instead of "cluster ID".
%% The reason is cluster ID is improper in case of multi-datacenter replication.
-type container_id() :: binary().
-type pool_key() :: {pool_type(), container_id()}.

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
    BlockPools = register_props(block_containers, default_block_container),
    ManifestPools = register_props(manifest_containers, default_manifest_container),
    BlockPools ++ ManifestPools.

register_props(PoolConfigName, DefaultConfigName) ->
    Pools = case application:get_env(riak_cs, PoolConfigName) of
                undefined ->
                    application:set_env(riak_cs, DefaultConfigName, undefined),
                    [];
                [] ->
                    application:set_env(riak_cs, DefaultConfigName, undefined),
                    [];
                {ok, Containers} ->
                    register_props(block, Containers, [])
            end,
    Pools.

%% FIXME: manifests
register_props(_, [], Names) ->
    Names;
register_props(Type, [{ContainerId, Address, Port} | Rest], PoolSpecs) ->
    lager:log(warning, self(), "{ContainerId, Type, Address, Port}: ~p~n",
              [{ContainerId, Type, Address, Port}]),
    NewPoolSpec = {register_and_get_pool_name(Type, ContainerId, Address, Port),
                   {?WORKERS, ?OVERFLOW, Address, Port}},
    register_props(block, Rest, [NewPoolSpec | PoolSpecs]).

%% Translate container ID in buckets and manifests to pool name.
%% 'undefined' in second argument means buckets and manifests were stored
%% under single cluster configuration.
-spec pool_name(pool_type(), undefined | container_id() | lfs_manifest()) -> atom().
pool_name(block, Manifest) when is_record(Manifest, ?MANIFEST_REC) ->
    container_pool_name(block, container_id_from_manifest(Manifest));
pool_name(manifest, BucketObj) ->
    container_pool_name(manifest, container_id_from_bucket(BucketObj)).

container_pool_name(_Type, undefined) ->
    undefined;
container_pool_name(Type, ContainerId) when is_binary(ContainerId) ->
    case ets:lookup(?ETS_TAB, {Type, ContainerId}) of
        [] ->
            undefined;
        [#pool{name = Name}] ->
            Name
    end.

-spec container_id_from_manifest(lfs_manifest()) -> undefined | container_id().
container_id_from_manifest(?MANIFEST{props = Props}) ->
    case Props of
        undefined ->
            undefined;
        _ ->
            proplists:get_value(block_container, Props)
    end.

-spec container_id_from_bucket(riakc_obj:riakc_obj()) -> undefined | container_id().
container_id_from_bucket(BucketObj) ->
    Contents = riakc_obj:get_contents(BucketObj),
    container_id_from_contents(Contents).

container_id_from_contents([]) ->
    undefined;
container_id_from_contents([{MD, _} | Contents]) ->
    case container_id_from_meta(dict:fetch(?MD_USERMETA, MD)) of
        undefined ->
            container_id_from_contents(Contents);
        ContainerId ->
            ContainerId
    end.

container_id_from_meta([]) ->
    undefined;
container_id_from_meta([{?MD_CONTAINER, Value} | _]) ->
    binary_to_term(Value);
container_id_from_meta([_ | MDs]) ->
    container_id_from_meta(MDs).

-spec default_container_id(pool_type()) -> container_id().
default_container_id(block) ->
    application:get_env(riak_cs, default_block_container_id);
default_container_id (manifest) ->
    application:get_env(riak_cs, default_manifest_container_id).

%% Choose container ID for new bucket or new manifest
assign_container_id(Type) ->
    case ets:first(?ETS_TAB) of
        %% single container
        '$end_of_table' ->
            undefined;
        %% multiple containers
        _Key ->
            {ok, ContainerId} = riak_cs_mc_server:allocate(Type),
            ContainerId
    end.

%% Choose container ID to store blocks for new manifest and
%% return new manifest
-spec assign_container_id(pool_type(), lfs_manifest()) -> lfs_manifest().
assign_container_id(block, ?MANIFEST{props = Props} = Manifest) ->
    case assign_container_id(block) of
        undefined ->
            Manifest;
        ContainerId ->
            Manifest?MANIFEST{props = [{block_container, ContainerId} | Props]}
    end.

init_ets() ->
    ets:new(?ETS_TAB, [{keypos, 2}, named_table, protected,
                       {read_concurrency, true}]).

-spec register_and_get_pool_name(pool_type(), string(),
                                 non_neg_integer(), container_id()) -> atom().
register_and_get_pool_name(Type, ContainerId, IP, Port) ->
    %% TODO: Better to check container_id for safety
    %%       Or get container_id on the fly?
    %% TODO(shino): IP and Port are better than ContainerId?
    %%              Or just serial number?
    Name = list_to_atom(lists:flatten(io_lib:format("~s:~s", [Type, ContainerId]))),
    ets:insert(?ETS_TAB, #pool{key = {Type, ContainerId},
                               type = Type,
                               ip = IP,
                               port = Port,
                               name = Name}),
    Name.

%% For Debugging

tab_info() ->
    ets:tab2list(?ETS_TAB).
