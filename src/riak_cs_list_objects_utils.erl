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

%% @doc

-module(riak_cs_list_objects_utils).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

-type tagged_item() :: {prefix, binary()} |
                       {manifest, lfs_manifest()}.

-type tagged_item_list() :: list(tagged_item()).

-export_type([tagged_item/0,
              tagged_item_list/0]).

%%%===================================================================
%%% Exports
%%%===================================================================

%% API
-export([start_link/5,
         manifests_and_prefix_length/1]).

%% Observability / Configuration
-export([get_key_list_multiplier/0,
         set_key_list_multiplier/1,
         fold_objects_for_list_keys/0]).

%%%===================================================================
%%% API
%%%===================================================================


-spec start_link(pid(), pid(), list_object_request(), term(), UseCache :: boolean()) ->
    {ok, pid()} | {error, term()}.
%% @doc An abstraction between the old and new list-keys mechanism. Uses the
%% old mechanism if `fold_objects_for_list_keys' is false, otherwise uses
%% the new one. After getting a pid back, the API is the same, so users don't
%% need to differentiate.
start_link(RiakcPid, CallerPid, ListKeysRequest, CacheKey, UseCache) ->
    case fold_objects_for_list_keys() of
        true ->
            riak_cs_list_objects_fsm_v2:start_link(RiakcPid, ListKeysRequest);
        false ->
            riak_cs_list_objects_fsm:start_link(RiakcPid, CallerPid,
                                                ListKeysRequest, CacheKey,
                                                UseCache)
    end.

-spec manifests_and_prefix_length({list(), ordsets:ordset()}) ->
                                   non_neg_integer().
manifests_and_prefix_length({List, Set}) ->
    length(List) + ordsets:size(Set).

%%%===================================================================
%%% Observability / Configuration
%%%===================================================================

-spec get_key_list_multiplier() -> float().
get_key_list_multiplier() ->
    riak_cs_utils:get_env(riak_cs, key_list_multiplier,
                          ?KEY_LIST_MULTIPLIER).

-spec set_key_list_multiplier(float()) -> 'ok'.
set_key_list_multiplier(Multiplier) ->
    application:set_env(riak_cs, key_list_multiplier,
                        Multiplier).


-spec fold_objects_for_list_keys() -> boolean().
fold_objects_for_list_keys() ->
    riak_cs_utils:get_env(riak_cs, fold_objects_for_list_keys,
                          ?FOLD_OBJECTS_FOR_LIST_KEYS).
