%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_manifest).

-export([fetch/3,
         get_manifests/3,
         manifests_from_riak_object/1,
         etag/1,
         etag_no_quotes/1,
         object_acl/1]).

-include("riak_cs.hrl").

-spec fetch(pid(), binary(), binary()) -> {ok, lfs_manifest()} | {error, term()}.
fetch(RcPid, Bucket, Key) ->
    case riak_cs_manifest:get_manifests(RcPid, Bucket, Key) of
        {ok, _, Manifests} ->
            riak_cs_manifest_utils:active_manifest(orddict:from_list(Manifests));
        Error ->
            Error
    end.

-spec get_manifests(riak_client(), binary(), binary()) ->
    {ok, term(), term()} | {error, term()}.
get_manifests(RcPid, Bucket, Key) ->
    case get_manifests_raw(RcPid, Bucket, Key) of
        {ok, Object} ->
            Manifests = manifests_from_riak_object(Object),
            maybe_warn_bloated_manifests(Bucket, Key, Object, Manifests),
            _  = gc_deleted_while_writing_manifests(Object, Manifests, Bucket, Key, RcPid),
            {ok, Object, Manifests};
        {error, _Reason}=Error ->
            Error
    end.

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
                          not riak_cs_utils:has_tombstone(Content)],

    %% Upgrade the manifests to be the latest erlang
    %% record version
    Upgraded = riak_cs_manifest_utils:upgrade_wrapped_manifests(DecodedSiblings),

    %% resolve the siblings
    Resolved = riak_cs_manifest_resolution:resolve(Upgraded),

    %% prune old scheduled_delete manifests
    riak_cs_manifest_utils:prune(Resolved).

-spec etag(lfs_manifest()) -> string().
etag(?MANIFEST{content_md5={MD5, Suffix}}) ->
    riak_cs_utils:etag_from_binary(MD5, Suffix);
etag(?MANIFEST{content_md5=MD5}) ->
    riak_cs_utils:etag_from_binary(MD5).

-spec etag_no_quotes(lfs_manifest()) -> string().
etag_no_quotes(?MANIFEST{content_md5=ContentMD5}) ->
    riak_cs_utils:etag_from_binary_no_quotes(ContentMD5).

-spec object_acl(notfound|lfs_manifest()) -> undefined|acl().
object_acl(notfound) ->
    undefined;
object_acl(?MANIFEST{acl=Acl}) ->
    Acl.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% Retrieve the riak object at a bucket/key
-spec get_manifests_raw(riak_client(), binary(), binary()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_manifests_raw(RcPid, Bucket, Key) ->
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    ok = riak_cs_riak_client:set_bucket_name(RcPid, Bucket),
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    Timeout = riak_cs_config:get_manifest_timeout(),
    case riakc_pb_socket:get(ManifestPbc, ManifestBucket, Key, Timeout) of
        {ok, _} = Result -> Result;
        {error, disconnected} ->
            riak_cs_pbc:check_connection_status(ManifestPbc, get_manifests_raw),
            {error, disconnected};
        Error ->
            Error
    end.

gc_deleted_while_writing_manifests(Object, Manifests, Bucket, Key, RcPid) ->
    UUIDs = riak_cs_manifest_utils:deleted_while_writing(Manifests),
    riak_cs_gc:gc_specific_manifests(UUIDs, Object, Bucket, Key, RcPid).

-spec maybe_warn_bloated_manifests(binary(), binary(), riakc_obj:riakc_obj(), [term()]) -> ok.
maybe_warn_bloated_manifests(Bucket, Key, Object, Manifests) ->
    maybe_warn_bloated_manifests(
      Bucket, Key,
      riakc_obj:value_count(Object),
      riak_cs_config:get_env(riak_cs, manifest_warn_siblings,
                             ?DEFAULT_MANIFEST_WARN_SIBLINGS),
      "Many manifest siblings", "siblings"),
    maybe_warn_bloated_manifests(
      Bucket, Key,
      %% Approximate object size by the sum of only values, ignoring metadata
      lists:sum([byte_size(V) || V <- riakc_obj:get_values(Object)]),
      riak_cs_config:get_env(riak_cs, manifest_warn_bytes,
                             ?DEFAULT_MANIFEST_WARN_BYTES),
      "Large manifest size", "bytes"),
    maybe_warn_bloated_manifests(
      Bucket, Key,
      length(Manifests),
      riak_cs_config:get_env(riak_cs, manifest_warn_history,
                             ?DEFAULT_MANIFEST_WARN_HISTORY),
      "Long manifest history", "manifests"),
    ok.

-spec maybe_warn_bloated_manifests(binary(), binary(), disabled | non_neg_integer(),
                        non_neg_integer(), string(), string()) -> ok.
maybe_warn_bloated_manifests(Bucket, Key, Actual, Threshold, Message, Unit) ->
    case Threshold of
        disabled -> ok;
        _ when Actual < Threshold -> ok;
        _ -> _ = lager:warning("~s (~p ~s) for bucket=~p key=~p",
                               [Message, Actual, Unit, Bucket, Key])
    end.
