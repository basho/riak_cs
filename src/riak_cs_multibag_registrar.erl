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

-module(riak_cs_multibag_registrar).

-export([process_specs/0, pool_specs/1, pool_name/3, choose_bag_id/1,
         set_bag_id_to_manifest/2,
         list_pool/1, pool_status/0]).

-behaiviour(riak_cs_bag_registrar).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

process_specs() ->
    riak_cs_multibag:process_specs().

pool_specs(Props) ->
    riak_cs_multibag:pool_specs(Props).

-spec pool_name(pid(), riak_cs_bag_registrar:pool_type(),
                lfs_manifest() | cs_bucket() | riakc_obj:riakc_obj()) ->
                       {ok, atom()} | {error, term()}.
pool_name(_MasterRiakc, request_pool = PoolType, Manifest)
  when is_record(Manifest, ?MANIFEST_REC) ->
    riak_cs_multibag:pool_name_for_bag(PoolType, bag_id_from_manifest(Manifest));
pool_name(MasterRiakc, request_pool = PoolType, ?RCS_BUCKET{} = Bucket) ->
    case bag_id_from_bucket_record(MasterRiakc, Bucket) of
        {ok, BagId} ->
            riak_cs_multibag:pool_name_for_bag(PoolType, BagId);
        {error, Reason} ->
            {error, Reason}
    end;
pool_name(_MasterRiakc, PoolType, BucketObj) ->
    riak_cs_multibag:pool_name_for_bag(PoolType, bag_id_from_bucket(BucketObj)).

-spec bag_id_from_manifest(lfs_manifest()) -> riak_cs_bag_registrar:bag_id().
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

-spec bag_id_from_bucket_record(pid(), cs_bucket()) ->
                                       {ok, riak_cs_bag_registrar:bag_id()} |
                                       {error, term()}.
bag_id_from_bucket_record(MasterRiakc, ?RCS_BUCKET{name=BucketName})
  when is_list(BucketName) ->
    bag_id_from_bucket_name(MasterRiakc, list_to_binary(BucketName));
bag_id_from_bucket_record(MasterRiakc, ?RCS_BUCKET{name=BucketName})
  when is_binary(BucketName) ->
    bag_id_from_bucket_name(MasterRiakc, BucketName).

bag_id_from_bucket_name(MasterRiakc, BucketName) ->
    case riak_cs_utils:fetch_bucket_object(BucketName, MasterRiakc) of
        {ok, BucketObj} ->
            {ok, bag_id_from_bucket(BucketObj)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec bag_id_from_bucket(riakc_obj:riakc_obj()) -> riak_cs_bag_registrar:bag_id().
bag_id_from_bucket(BucketObj) ->
    Contents = riakc_obj:get_contents(BucketObj),
    bag_id_from_contents(Contents).

bag_id_from_contents([]) ->
    undefined;
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

choose_bag_id(PoolType) ->
    riak_cs_multibag:choose_bag_id(PoolType).

list_pool(PoolType) ->
    [{PoolType, PoolType, master, []} |
     riak_cs_multibag:list_pool(PoolType)].

pool_status() ->
    riak_cs_multibag:list_pool().

-spec set_bag_id_to_manifest(riak_cs_bag_registrar:bag_id(), lfs_manifest()) ->
                                    lfs_manifest().
set_bag_id_to_manifest(undefined, Manifest) ->
    Manifest;
set_bag_id_to_manifest(BagId, ?MANIFEST{props = Props} = Manifest) ->
    Manifest?MANIFEST{props = [{block_bag, BagId} | Props]}.
