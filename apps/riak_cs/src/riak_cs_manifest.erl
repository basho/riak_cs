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

-export([fetch/4,
         get_manifests/4,
         get_manifests_of_all_versions/3,
         manifests_from_riak_object/1,
         link_version/5,
         unlink_version/4,
         etag/1,
         etag_no_quotes/1,
         object_acl/1]).

-include("riak_cs.hrl").

-spec fetch(pid(), binary(), binary(), binary()) -> {ok, lfs_manifest()} | {error, term()}.
fetch(RcPid, Bucket, Key, ObjVsn) ->
    case get_manifests(RcPid, Bucket, Key, ObjVsn) of
        {ok, _, Manifests} ->
            rcs_common_manifest_utils:active_manifest(orddict:from_list(Manifests));
        Error ->
            Error
    end.

-spec get_manifests(riak_client(), binary(), binary(), binary()) ->
    {ok, riakc_obj:riakc_obj(), wrapped_manifest()} | {error, term()}.
get_manifests(RcPid, Bucket, Key, ObjVsn) ->
    case get_manifests_raw(RcPid, Bucket, Key, ObjVsn) of
        {ok, Object} ->
            Manifests = manifests_from_riak_object(Object),
            maybe_warn_bloated_manifests(Bucket, Key, Object, Manifests),
            _  = gc_deleted_while_writing_manifests(Object, Manifests, Bucket, RcPid),
            {ok, Object, Manifests};
        {error, _Reason} = Error ->
            Error
    end.


-spec get_manifests_of_all_versions(riak_client(), binary(), binary()) ->
          {ok, orddict:orddict(UUID::binary(), [{ObjVsn::binary(), wrapped_manifest()}])} | {error, term()}.
get_manifests_of_all_versions(RcPid, Bucket, Key) ->
    case get_manifests(RcPid, Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION) of
        {ok, _, PrimaryM} ->
            try
                {ok, orddict:map(fun(_, M) -> get_descendants(RcPid, Bucket, Key, M) end, PrimaryM)}
            catch
                throw:manifest_retrieval_error ->
                    {error, manifest_retrieval_error}
            end;
        ER ->
            ER
    end.

get_descendants(Rc, B, K, M) ->
    lists:reverse(
      get_descendants(Rc, B, K, M, [])).
get_descendants(_, _, _, ThisM = ?MANIFEST{object_version = ThisOV,
                                           next_object_version = eol}, Q) ->
    [{ThisOV, ThisM} | Q];
get_descendants(Rc, B, K, ThisM = ?MANIFEST{object_version = ThisOV,
                                            next_object_version = NextOV}, Q) ->
    case get_manifests(Rc, B, K, NextOV) of
        {ok, _, NextM} ->
            get_descendants(Rc, B, K, NextM, [{ThisOV, ThisM} | Q]);
        ER ->
            lager:warning("failed to get manifests for version ~s of ~s/~s (~p)", [NextOV, B, K, ER]),
            throw(manifest_retrieval_error)
    end.


-spec unlink_version(riak_client(), binary(), binary(), lfs_manifest()) -> ok.
unlink_version(RcPid, Bucket, Key, ErasedM) ->
    orddict:map(fun(_UUID, M) -> unlink_version2(RcPid, Bucket, Key, M) end, ErasedM),
    ok.

unlink_version2(RcPid, Bucket, Key, ?MANIFEST{next_object_version = NextOV,
                                              prev_object_version = PrevOV}) ->
    if PrevOV /= eol ->
            {ok, ManiPid1} = riak_cs_manifest_fsm:start_link(Bucket, Key, PrevOV, RcPid),
            {ok, _, PrevM} = get_manifests(RcPid, Bucket, Key, PrevOV),
            riak_cs_manifest_fsm:update_manifests(
              ManiPid1,
              orddict:map(fun(_, M) -> M?MANIFEST{next_object_version = NextOV} end, PrevM)),
            riak_cs_manifest_fsm:stop(ManiPid1);
       el/=se ->
            nop
    end,
    if NextOV /= eol ->
            {ok, ManiPid2} = riak_cs_manifest_fsm:start_link(Bucket, Key, NextOV, RcPid),
            {ok, _, NextM} = get_manifests(RcPid, Bucket, Key, NextOV),
            riak_cs_manifest_fsm:update_manifests(
              ManiPid2,
              orddict:map(fun(_, M) -> M?MANIFEST{next_object_version = PrevOV} end, NextM)),
            riak_cs_manifest_fsm:stop(ManiPid2);
       el/=se ->
            nop
    end,
    ok.

-spec link_version(nopid | riak_client(), binary(), binary(), lfs_manifest(), binary()) ->
          {sole | new | existing, lfs_manifest()}.
link_version(nopid, Bucket, Key, InsertedM, ObjVsn) ->
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    Res = link_version(RcPid, Bucket, Key, InsertedM, ObjVsn),
    riak_cs_riak_client:checkin(RcPid),
    Res;

link_version(RcPid, Bucket, Key, InsertedM, ObjVsn) ->
    case get_manifests_of_all_versions(RcPid, Bucket, Key) of
        {ok, UUVVMM} ->
            case find_version(ObjVsn, UUVVMM) of
                {found, _M} ->
                    %% found a matching version: don't bother
                    %% changing prev or next links. It will be resolved, later, I suppose?
                    {existing, InsertedM};
                not_found ->
                    %% insert the supplied manifest
                    {new, link_version2(InsertedM, ObjVsn, UUVVMM, RcPid)}
            end;
        {error, notfound} ->
            {sole, InsertedM?MANIFEST{object_version = ObjVsn}}
    end.

find_version(V, UUVVMM) ->
    orddict:fold(
      fun(_, _, {found, _} = Found) ->
              Found;
         (_UUID, VVMM, _) ->
              case lists:keyfind(V, 1, VVMM) of
                  false ->
                      continue_captain;
                  {_, M} ->
                      {found, M}
              end
      end,
      not_found,
      UUVVMM).

link_version2(M0 = ?MANIFEST{bkey = {Bucket, Key}}, ObjVsn, UUVVMM, RcPid) ->

    LastBefore = find_adjoining(ObjVsn, UUVVMM, {'<', fun(L) -> L end}),
    FirstAfter = find_adjoining(ObjVsn, UUVVMM, {'>', fun lists:reverse/1}),

    M1 =
        if FirstAfter /= eol ->
                {ok, MPid1} = riak_cs_manifest_fsm:start_link(Bucket, Key, FirstAfter, RcPid),
                riak_cs_manifest_fsm:update_manifests(
                  MPid1,
                  orddict:map(fun(_, M) -> M?MANIFEST{prev_object_version = ObjVsn} end, FirstAfter)),
                riak_cs_manifest_fsm:stop(MPid1),
                M0?MANIFEST{next_object_version = FirstAfter?MANIFEST.object_version};
           el/=se ->
                M0?MANIFEST{next_object_version = eol}
        end,

    M2 =
        if LastBefore /= eol ->
                {ok, MPid2} = riak_cs_manifest_fsm:start_link(Bucket, Key, LastBefore, RcPid),
                riak_cs_manifest_fsm:update_manifests(
                  MPid2,
                  orddict:map(fun(_, M) -> M?MANIFEST{next_object_version = ObjVsn} end, LastBefore)),
                riak_cs_manifest_fsm:stop(MPid2),
                M1?MANIFEST{prev_object_version = LastBefore?MANIFEST.object_version};
           el/=se ->
                M1?MANIFEST{prev_object_version = eol}
        end,

    M2?MANIFEST{object_version = ObjVsn}.

find_adjoining(V, UUVVMM, {Operand, Fun}) ->
    orddict:fold(
      fun(_UUID, VVMM, eol) ->
              case lists:search(fun({Vi, _}) -> Operand(V, Vi) end, Fun(VVMM)) of
                  false ->
                      eol;
                  {value, {_, M}} ->
                      M
              end;
         (_, _, Found) ->
              Found
      end,
      eol,
      UUVVMM).


-spec manifests_from_riak_object(riakc_obj:riakc_obj()) -> wrapped_manifest().
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
                          {_, V} = Content <- Contents,
                          not riak_cs_utils:has_tombstone(Content)],

    %% Upgrade the manifests to be the latest erlang
    %% record version
    Upgraded = rcs_common_manifest_utils:upgrade_wrapped_manifests(DecodedSiblings),

    %% resolve the siblings
    Resolved = rcs_common_manifest_resolution:resolve(Upgraded),

    %% prune old scheduled_delete manifests
    riak_cs_manifest_utils:prune(Resolved).

-spec etag(lfs_manifest()) -> string().
etag(?MANIFEST{content_md5 = {MD5, Suffix}}) ->
    riak_cs_utils:etag_from_binary(MD5, Suffix);
etag(?MANIFEST{content_md5 = MD5}) ->
    riak_cs_utils:etag_from_binary(MD5).

-spec etag_no_quotes(lfs_manifest()) -> string().
etag_no_quotes(?MANIFEST{content_md5 = ContentMD5}) ->
    riak_cs_utils:etag_from_binary_no_quotes(ContentMD5).

-spec object_acl(notfound | lfs_manifest()) -> undefined | acl().
object_acl(notfound) ->
    undefined;
object_acl(?MANIFEST{acl = Acl}) ->
    Acl.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% Retrieve the riak object at a bucket/key/version
get_manifests_raw(RcPid, Bucket, Key, Vsn) ->
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    ok = riak_cs_riak_client:set_bucket_name(RcPid, Bucket),
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    Timeout = riak_cs_config:get_manifest_timeout(),
    case riakc_pb_socket:get(ManifestPbc, ManifestBucket,
                             rcs_common_manifest:make_versioned_key(Key, Vsn), Timeout) of
        {ok, _} = Result ->
            Result;
        {error, disconnected} ->
            riak_cs_pbc:check_connection_status(ManifestPbc, get_manifests_raw),
            {error, disconnected};
        Error ->
            Error
    end.

gc_deleted_while_writing_manifests(Object, Manifests, Bucket, RcPid) ->
    UUIDs = rcs_common_manifest_utils:deleted_while_writing(Manifests),
    riak_cs_gc:gc_specific_manifests(UUIDs, Object, Bucket, RcPid).

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
