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
    Key = Key,
    ManifestBucket = ManifestBucket,
    ManifestPbc = ManifestPbc,
    {ok,{riakc_obj,<<48,111,58,65,147,202,175,74,37,11,21,194,48,16,34,201,48,220,141>>,<<49,45,107,98,121,116,101,46,50,48>>,<<107,206,97,96,96,96,204,96,202,5,82,28,202,156,255,126,134,248,253,168,201,96,74,100,202,99,101,216,206,251,238,44,95,22,0>>,[{{dict,2,16,16,8,80,48,{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},{{[],[],[],[],[],[],[],[],[],[],[[<<88,45,82,105,97,107,45,86,84,97,103>>,53,106,52,51,106,55,109,53,107,81,68,51,78,103,75,48,102,103,97,86,103,121]],[],[],[[<<88,45,82,105,97,107,45,76,97,115,116,45,77,111,100,105,102,105,101,100>>|{1417,253303,551281}]],[],[]}}},<<131,108,0,0,0,1,104,2,109,0,0,0,16,111,231,150,160,191,31,69,196,144,245,32,132,186,11,183,231,104,21,100,0,15,108,102,115,95,109,97,110,105,102,101,115,116,95,118,51,97,3,98,0,16,0,0,104,2,109,0,0,0,8,116,101,115,116,45,121,106,97,109,0,0,0,10,49,45,107,98,121,116,101,46,50,48,106,107,0,24,50,48,49,52,45,49,49,45,50,57,84,48,57,58,50,56,58,50,51,46,48,48,48,90,109,0,0,0,16,111,231,150,160,191,31,69,196,144,245,32,132,186,11,183,231,98,0,0,4,0,109,0,0,0,19,98,105,110,97,114,121,47,111,99,116,101,116,45,115,116,114,101,97,109,109,0,0,0,16,15,52,59,9,49,18,106,32,241,51,214,124,43,1,138,59,100,0,6,97,99,116,105,118,101,104,3,98,0,0,5,137,98,0,3,221,119,98,0,8,87,220,104,3,98,0,0,5,137,98,0,3,221,119,98,0,8,92,248,106,100,0,9,117,110,100,101,102,105,110,101,100,100,0,9,117,110,100,101,102,105,110,101,100,100,0,9,117,110,100,101,102,105,110,101,100,100,0,9,117,110,100,101,102,105,110,101,100,104,4,100,0,6,97,99,108,95,118,50,104,3,107,0,6,102,111,111,98,97,114,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,107,0,20,74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73,108,0,0,0,2,104,2,104,2,107,0,6,102,111,111,98,97,114,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,108,0,0,0,1,100,0,12,70,85,76,76,95,67,79,78,84,82,79,76,106,104,2,100,0,8,65,108,108,85,115,101,114,115,108,0,0,0,1,100,0,4,82,69,65,68,106,106,104,3,98,0,0,5,137,98,0,3,221,119,98,0,8,87,22,106,100,0,9,117,110,100,101,102,105,110,101,100,106>>}],undefined,undefined}}.
    %% case riakc_pb_socket:get(ManifestPbc, ManifestBucket, Key) of
    %%     {ok, _} = Result -> Result;
    %%     {error, disconnected} ->
    %%         riak_cs_pbc:check_connection_status(ManifestPbc, get_manifests_raw),
    %%         {error, disconnected};
    %%     Error ->
    %%         Error
    %% end.

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
