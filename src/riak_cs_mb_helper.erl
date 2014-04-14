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

%% @doc Helper functions for multibag functionality.

-module(riak_cs_mb_helper).

-export([process_specs/0, bags/0,
         choose_bag_id/1,
         set_bag_id_to_manifest/2,
         bag_id_from_manifest/1]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-define(MB_ENABLED(Normal, Multibag),
        case riak_cs_config:is_multibag_enabled() of
            false -> Normal;
            true  -> Multibag
        end).

process_specs() ->
    ?MB_ENABLED([], riak_cs_multibag:process_specs()).

bags() ->
    {MasterAddress, MasterPort} = riak_cs_riakc_pool_worker:riak_host_port(),
    ?MB_ENABLED([{<<"master">>, MasterAddress, MasterPort}],
                riak_cs_multibag:bags()).

choose_bag_id(PoolType) ->
    ?MB_ENABLED(undefined, riak_cs_multibag:choose_bag_id(PoolType)).

set_bag_id_to_manifest(undefined, Manifest) ->
    Manifest;
set_bag_id_to_manifest(BagId, ?MANIFEST{props = Props} = Manifest)
  when is_binary(BagId) ->
    Manifest?MANIFEST{props = [{block_bag, BagId} | Props]}.

-spec bag_id_from_manifest(lfs_manifest()) -> bag_id().
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
