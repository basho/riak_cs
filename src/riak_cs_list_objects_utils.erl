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
                       {manifest, lfs_manifest()} |
                       {manifest, {Key :: binary(), lfs_manifest()}}.

-type tagged_item_list() :: list(tagged_item()).

-type manifests_and_prefixes() :: {list(lfs_manifest()), ordsets:ordset(binary())}.

-export_type([tagged_item/0,
              tagged_item_list/0,
              manifests_and_prefixes/0]).

%%%===================================================================
%%% Exports
%%%===================================================================

%% API
-export([start_link/5,
         get_object_list/1,
         get_internal_state/1]).

%% Shared Helpers
-export([manifests_and_prefix_length/1,
         tagged_manifest_and_prefix/1,
         untagged_manifest_and_prefix/1,
         manifests_and_prefix_slice/2,
         filter_prefix_keys/2]).

%% Observability / Configuration
-export([get_key_list_multiplier/0,
         set_key_list_multiplier/1,
         fold_objects_for_list_keys/0]).

%%%===================================================================
%%% API
%%%===================================================================


-spec start_link(pid(), pid(), list_object_request(), term(),
                 UseCache :: boolean()) ->
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

-spec get_object_list(pid()) ->
    {ok, list_object_response()} |
    {error, term()}.
get_object_list(FSMPid) ->
    gen_fsm:sync_send_all_state_event(FSMPid, get_object_list, infinity).

get_internal_state(FSMPid) ->
    gen_fsm:sync_send_all_state_event(FSMPid, get_internal_state, infinity).

%%%===================================================================
%%% Shared Helpers
%%%===================================================================

-spec manifests_and_prefix_length({list(), ordsets:ordset()}) ->
                                   non_neg_integer().
manifests_and_prefix_length({List, Set}) ->
    length(List) + ordsets:size(Set).

-spec tagged_manifest_and_prefix(manifests_and_prefixes()) ->
    riak_cs_list_objects_utils:tagged_item_list().
tagged_manifest_and_prefix({Manifests, Prefixes}) ->
    tagged_manifest_list(Manifests) ++ tagged_prefix_list(Prefixes).

-spec tagged_manifest_list(list()) ->
    list({manifest, term()}).
tagged_manifest_list(KeyAndManifestList) ->
    [{manifest, M} || M <- KeyAndManifestList].

-spec tagged_prefix_list(list(binary())) ->
    list({prefix, binary()}).
tagged_prefix_list(Prefixes) ->
    [{prefix, P} || P <- ordsets:to_list(Prefixes)].

-spec untagged_manifest_and_prefix(riak_cs_list_objects_utils:tagged_item_list()) ->
    manifests_and_prefixes().
untagged_manifest_and_prefix(TaggedInput) ->
    Pred = fun({manifest, _}) -> true;
              (_Else) -> false end,
    {A, B} = lists:partition(Pred, TaggedInput),
    {[element(2, M) || M <- A],
     [element(2, P) || P <- B]}.

-spec manifests_and_prefix_slice(riak_cs_list_objects_utils:manifests_and_prefixes(),
                                 non_neg_integer()) ->
    riak_cs_list_objects_utils:tagged_item_list().
manifests_and_prefix_slice(ManifestsAndPrefixes, MaxObjects) ->
    TaggedList =
    riak_cs_list_objects_utils:tagged_manifest_and_prefix(ManifestsAndPrefixes),

    Sorted = lists:sort(fun tagged_sort_fun/2, TaggedList),
    lists:sublist(Sorted, MaxObjects).

-spec tagged_sort_fun(riak_cs_list_objects_utils:tagged_item(),
                      riak_cs_list_objects_utils:tagged_item()) ->
    boolean().
tagged_sort_fun(A, B) ->
    AKey = key_from_tag(A),
    BKey = key_from_tag(B),
    AKey =< BKey.

-spec key_from_tag(riak_cs_list_objects_utils:tagged_item()) -> binary().
key_from_tag({manifest, ?MANIFEST{bkey={_Bucket, Key}}}) ->
    Key;
key_from_tag({prefix, Key}) ->
    Key.

-spec filter_prefix_keys({ManifestList :: list(lfs_manifest()),
                          CommonPrefixes :: ordsets:ordset(binary())},
                         list_object_request()) ->
    riak_cs_list_objects_utils:manifests_and_prefixes().
filter_prefix_keys({_ManifestList, _CommonPrefixes}=Input,
                   ?LOREQ{prefix=undefined,
                          delimiter=undefined}) ->
    Input;
filter_prefix_keys({ManifestList, CommonPrefixes},
                   ?LOREQ{prefix=Prefix,
                          delimiter=Delimiter}) ->
    PrefixFilter =
        fun(Manifest, Acc) ->
                prefix_filter(Manifest, Acc, Prefix, Delimiter)
        end,
    lists:foldl(PrefixFilter, {[], CommonPrefixes}, ManifestList).

prefix_filter(Manifest=?MANIFEST{bkey={_Bucket, Key}},
              Acc, undefined, Delimiter) ->
    Group = extract_group(Key, Delimiter),
    update_keys_and_prefixes(Acc, Manifest, <<>>, 0, Group);
prefix_filter(Manifest=?MANIFEST{bkey={_Bucket, Key}},
              {ManifestList, Prefixes}=Acc, Prefix, undefined) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        << Prefix:PrefixLen/binary, _/binary >> ->
            {[Manifest | ManifestList], Prefixes};
        _ ->
            Acc
    end;
prefix_filter(Manifest=?MANIFEST{bkey={_Bucket, Key}},
              {_ManifestList, _Prefixes}=Acc, Prefix, Delimiter) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        << Prefix:PrefixLen/binary, Rest/binary >> ->
            Group = extract_group(Rest, Delimiter),
            update_keys_and_prefixes(Acc, Manifest, Prefix, PrefixLen, Group);
        _ ->
            Acc
    end.

extract_group(Key, Delimiter) ->
    case binary:match(Key, [Delimiter]) of
        nomatch ->
            nomatch;
        {Pos, Len} ->
            binary:part(Key, {0, Pos+Len})
    end.

update_keys_and_prefixes({ManifestList, Prefixes},
                         Manifest, _, _, nomatch) ->
    {[Manifest | ManifestList], Prefixes};
update_keys_and_prefixes({ManifestList, Prefixes},
                         _, Prefix, PrefixLen, Group) ->
    NewPrefix = << Prefix:PrefixLen/binary, Group/binary >>,
    {ManifestList, ordsets:add_element(NewPrefix, Prefixes)}.



%%%===================================================================
%%% Observability / Configuration
%%%===================================================================

-spec get_key_list_multiplier() -> float().
get_key_list_multiplier() ->
    riak_cs_config:get_env(riak_cs, key_list_multiplier,
                          ?KEY_LIST_MULTIPLIER).

-spec set_key_list_multiplier(float()) -> 'ok'.
set_key_list_multiplier(Multiplier) ->
    application:set_env(riak_cs, key_list_multiplier,
                        Multiplier).


-spec fold_objects_for_list_keys() -> boolean().
fold_objects_for_list_keys() ->
    riak_cs_config:get_env(riak_cs, fold_objects_for_list_keys,
                          ?FOLD_OBJECTS_FOR_LIST_KEYS).
