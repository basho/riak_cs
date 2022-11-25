%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

-module(stanchion_multipart).

-export([check_no_multipart_uploads/2]).

-include("stanchion.hrl").
-include("manifest.hrl").


check_no_multipart_uploads(Bucket, RiakPid) ->
    HashBucket = stanchion_utils:to_bucket_name(objects, Bucket),

    {{ok, Keys}, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:list_keys(RiakPid, HashBucket)),
    stanchion_stats:update([riakc, list_all_manifest_keys], TAT),

    %% check all up
    lists:all(fun(VKey) ->
                      {Key, Vsn} = rcs_common_manifest:decompose_versioned_key(VKey),
                      GetResult = stanchion_utils:get_manifests_raw(RiakPid, Bucket, Key, Vsn),
                      has_no_upload(GetResult)
              end, Keys).

has_no_upload({ok, Obj}) ->
    Manifests = manifests_from_riak_object(Obj),
    lists:all(fun({_UUID,Manifest}) ->
                      case Manifest?MANIFEST.state of
                          writing ->
                              %% if this is mp => false
                              not proplists:is_defined(multipart, Manifest?MANIFEST.props);
                          _ ->
                              true
                      end
              end, Manifests);
has_no_upload({error, notfound}) -> true.

-spec manifests_from_riak_object(riakc_obj:riakc_obj()) -> orddict:orddict().
manifests_from_riak_object(RiakObject) ->
    %% For example, riak_cs_manifest_fsm:get_and_update/4 may wish to
    %% update the #riakc_obj without a roundtrip to Riak first.  So we
    %% need to see what the latest
    Contents = try
                   %% get_update_value will return the updatevalue or
                   %% a single old original value.
                   [{riakc_obj:get_update_metadata(RiakObject),
                     riakc_obj:get_update_value(RiakObject)}]
               catch throw:_ ->
                       %% Original value had many contents
                       riakc_obj:get_contents(RiakObject)
               end,
    DecodedSiblings = [binary_to_term(V) ||
                          {_, V}=Content <- Contents,
                          not stanchion_utils:has_tombstone(Content)],

    %% Upgrade the manifests to be the latest erlang
    %% record version
    Upgraded = rcs_common_manifest_utils:upgrade_wrapped_manifests(DecodedSiblings),

    %% resolve the siblings
    rcs_common_manifest_resolution:resolve(Upgraded).
