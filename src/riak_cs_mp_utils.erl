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

-module(riak_cs_mp_utils).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MIN_MP_PART_SIZE, (5*1024*1024)).

-define(PID(WrappedRcPid), get_riak_client_pid(WrappedRcPid)).

%% export Public API
-export([
         abort_multipart_upload/4, abort_multipart_upload/5,
         calc_multipart_2i_dict/3,
         clean_multipart_unused_parts/2,
         complete_multipart_upload/5, complete_multipart_upload/6,
         initiate_multipart_upload/5, initiate_multipart_upload/6,
         list_all_multipart_uploads/3,
         list_multipart_uploads/3, list_multipart_uploads/4,
         list_parts/5, list_parts/6,
         make_content_types_accepted/2,
         make_content_types_accepted/3,
         make_special_error/1,
         make_special_error/4,
         upload_part/6, upload_part/7,
         upload_part_1blob/2,
         upload_part_finished/7, upload_part_finished/8,
         is_multipart_manifest/1
        ]).
-export([get_mp_manifest/1]).

%%%===================================================================
%%% API
%%%===================================================================

calc_multipart_2i_dict(Ms, Bucket, _Key) when is_list(Ms) ->
    %% According to API Version 2006-03-01, page 139-140, bucket
    %% owners have some privileges for multipart uploads performed by
    %% other users, i.e, see those MP uploads via list multipart uploads,
    %% and cancel multipart upload.  We use two different 2I index entries
    %% to allow 2I to do the work of segregating multipart upload requests
    %% of bucket owner vs. non-bucket owner via two different 2I entries,
    %% one that includes the object owner and one that does not.
    L_2i = [
            case get_mp_manifest(M) of
                undefined ->
                    [];
                MpM when is_record(MpM, ?MULTIPART_MANIFEST_RECNAME) ->
                    [{make_2i_key(Bucket, MpM?MULTIPART_MANIFEST.owner), <<"1">>},
                     {make_2i_key(Bucket), <<"1">>}]
            end || M <- Ms,
                   M?MANIFEST.state == writing],
    {?MD_INDEX, lists:usort(lists:append(L_2i))}.

abort_multipart_upload(Bucket, Key, UploadId, Caller) ->
    abort_multipart_upload(Bucket, Key, UploadId, Caller, nopid).

abort_multipart_upload(Bucket, Key, UploadId, Caller, RcPidUnW) ->
    do_part_common(abort, Bucket, Key, UploadId, Caller, [], RcPidUnW).

clean_multipart_unused_parts(?MANIFEST{bkey=BKey, props=Props} = Manifest, RcPid) ->
    case get_mp_manifest(Manifest) of
        undefined ->
            same;
        MpM ->
            case {proplists:get_value(multipart_clean, Props, false),
                  MpM?MULTIPART_MANIFEST.cleanup_parts} of
                {false, []} ->
                    same;
                {false, PartsToDelete} ->
                    _ = try
                        {Bucket, Key} = BKey,
                        BagId = riak_cs_mb_helper:bag_id_from_manifest(Manifest),
                        ok = move_dead_parts_to_gc(Bucket, Key, BagId,
                                                   PartsToDelete, RcPid),
                        UpdManifest = Manifest?MANIFEST{props=[multipart_clean|Props]},
                        ok = update_manifest_with_confirmation(RcPid, UpdManifest)
                    catch X:Y ->
                            lager:debug("clean_multipart_unused_parts: "
                                        "bkey ~p: ~p ~p @ ~p\n",
                                        [BKey, X, Y, erlang:get_stacktrace()])
                    end,
                    %% Return same value to caller, regardless of ok/catch
                    updated;
                {true, _} ->
                    same
            end
    end.

complete_multipart_upload(Bucket, Key, UploadId, PartETags, Caller) ->
    complete_multipart_upload(Bucket, Key, UploadId, PartETags, Caller, nopid).

complete_multipart_upload(Bucket, Key, UploadId, PartETags, Caller, RcPidUnW) ->
    Extra = {PartETags},
    do_part_common(complete, Bucket, Key, UploadId, Caller, [{complete, Extra}],
                   RcPidUnW).

initiate_multipart_upload(Bucket, Key, ContentType, Owner, Opts) ->
    initiate_multipart_upload(Bucket, Key, ContentType, Owner, Opts, nopid).

initiate_multipart_upload(Bucket, Key, ContentType, {_,_,_} = Owner,
                          Opts, RcPidUnW) ->
    write_new_manifest(new_manifest(Bucket, Key, ContentType, Owner, Opts),
                       Opts, RcPidUnW).

make_content_types_accepted(RD, Ctx) ->
    make_content_types_accepted(RD, Ctx, unused_callback).

make_content_types_accepted(RD, Ctx, Callback) ->
    make_content_types_accepted(wrq:get_req_header("Content-Type", RD),
                                RD,
                                Ctx,
                                Callback).

make_content_types_accepted(CT, RD, Ctx, Callback)
  when CT =:= undefined;
       CT =:= [] ->
    make_content_types_accepted("application/octet-stream", RD, Ctx, Callback);
make_content_types_accepted(CT, RD, Ctx=#context{local_context=LocalCtx0}, Callback) ->
    %% This was shamelessly ripped out of
    %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
    {Media, _Params} = mochiweb_util:parse_header(CT),
    case string:tokens(Media, "/") of
        [_Type, _Subtype] ->
            %% accept whatever the user says
            LocalCtx = LocalCtx0#key_context{putctype=Media},
            {[{Media, Callback}], RD, Ctx#context{local_context=LocalCtx}};
        _ ->
            {[],
             wrq:set_resp_header(
               "Content-Type",
               "text/plain",
               wrq:set_resp_body(
                 ["\"", Media, "\""
                  " is not a valid media type"
                  " for the Content-type header.\n"],
                 RD)),
             Ctx}
    end.

make_special_error(Error) ->
    make_special_error(Error, Error, "request-id", "host-id").

make_special_error(Code, Message, RequestId, HostId) ->
    XmlDoc = {'Error',
              [
               {'Code', [Code]},
               {'Message', [Message]},
               {'RequestId', [RequestId]},
               {'HostId', [HostId]}
              ]
             },
    riak_cs_xml:to_xml([XmlDoc]).


list_multipart_uploads(Bucket, Caller, Opts) ->
    list_multipart_uploads(Bucket, Caller, Opts, nopid).

list_multipart_uploads(Bucket, {_Display, _Canon, CallerKeyId} = Caller,
                       Opts, RcPidUnW) ->
    case wrap_riak_client(RcPidUnW) of
        {ok, RcPid} ->
            try
                BucketOwnerP = is_caller_bucket_owner(?PID(RcPid),
                                                      Bucket, CallerKeyId),
                Key2i = case BucketOwnerP of
                            true ->
                                make_2i_key(Bucket); % caller = bucket owner
                            false ->
                                make_2i_key(Bucket, Caller)
                        end,
                list_multipart_uploads_with_2ikey(Bucket, Opts, ?PID(RcPid), Key2i)
            catch error:{badmatch, {m_icbo, _}} ->
                    {error, access_denied}
            after
                wrap_close_riak_client(RcPid)
            end;
        Else ->
            Else
    end.

list_all_multipart_uploads(Bucket, Opts, RcPid) ->
    list_multipart_uploads_with_2ikey(Bucket, Opts,
                                      RcPid,
                                      make_2i_key(Bucket)).

list_multipart_uploads_with_2ikey(Bucket, Opts, RcPid, Key2i) ->
    HashBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    Timeout = riak_cs_config:get_index_list_multipart_uploads_timeout(),
    case riakc_pb_socket:get_index_eq(ManifestPbc, HashBucket,
                                      Key2i, <<"1">>, [{timeout, Timeout}]) of
        {ok, ?INDEX_RESULTS{keys=Names}} ->
            {ok, list_multipart_uploads2(Bucket, RcPid,
                                         Names, Opts)};
        Else2 ->
            Else2
    end.

list_parts(Bucket, Key, UploadId, Caller, Opts) ->
    list_parts(Bucket, Key, UploadId, Caller, Opts, nopid).

list_parts(Bucket, Key, UploadId, Caller, Opts, RcPidUnW) ->
    Extra = {Opts},
    do_part_common(list, Bucket, Key, UploadId, Caller, [{list, Extra}], RcPidUnW).

%% @doc
-spec new_manifest(binary(), binary(), binary(), acl_owner(), list()) -> lfs_manifest().
new_manifest(Bucket, Key, ContentType, {_, _, _} = Owner, Opts) ->
    UUID = druuid:v4(),
    %% TODO: add object metadata here, e.g. content-disposition et al.
    MetaData = case proplists:get_value(meta_data, Opts) of
                   undefined -> [];
                   AsIsHdrs  -> AsIsHdrs
               end,
    M = riak_cs_lfs_utils:new_manifest(Bucket,
                                       Key,
                                       UUID,
                                       0,
                                       ContentType,
                                       %% we won't know the md5 of a multipart
                                       undefined,
                                       MetaData,
                                       riak_cs_lfs_utils:block_size(),
                                       %% ACL: needs Riak client pid, so we wait
                                       no_acl_yet,
                                       [],
                                       %% Cluster ID and Bag ID are added later
                                       undefined,
                                       undefined),
    MpM = ?MULTIPART_MANIFEST{upload_id = UUID,
                              owner = Owner},
    M?MANIFEST{props = replace_mp_manifest(MpM, M?MANIFEST.props)}.

upload_part(Bucket, Key, UploadId, PartNumber, Size, Caller) ->
    upload_part(Bucket, Key, UploadId, PartNumber, Size, Caller, nopid).

upload_part(Bucket, Key, UploadId, PartNumber, Size, Caller, RcPidUnW) ->
    Extra = {Bucket, Key, UploadId, Caller, PartNumber, Size},
    do_part_common(upload_part, Bucket, Key, UploadId, Caller,
                   [{upload_part, Extra}], RcPidUnW).

upload_part_1blob(PutPid, Blob) ->
    ok = riak_cs_put_fsm:augment_data(PutPid, Blob),
    {ok, M} = riak_cs_put_fsm:finalize(PutPid, undefined),
    {ok, M?MANIFEST.content_md5}.

%% Once upon a time, in a naive land far away, I thought that it would
%% be sufficient to use each part's UUID as the ETag when the part
%% upload was finished, and thus the clietn would use that UUID to
%% complete the uploaded object.  However, 's3cmd' want to use the
%% ETag of each uploaded part to be the MD5(part content) and will
%% issue a warning if that checksum expectation isn't met.  So, now we
%% must thread the MD5 value through upload_part_finished and update
%% the ?MULTIPART_MANIFEST in a mergeable way.  {sigh}

upload_part_finished(Bucket, Key, UploadId, _PartNumber, PartUUID, MD5, Caller) ->
    upload_part_finished(Bucket, Key, UploadId, _PartNumber, PartUUID, MD5,
                         Caller, nopid).

upload_part_finished(Bucket, Key, UploadId, _PartNumber, PartUUID, MD5,
                     Caller, RcPidUnW) ->
    Extra = {PartUUID, MD5},
    do_part_common(upload_part_finished, Bucket, Key, UploadId,
                   Caller, [{upload_part_finished, Extra}], RcPidUnW).

write_new_manifest(?MANIFEST{bkey={Bucket, Key}, uuid=UUID}=M, Opts, RcPidUnW) ->
    MpM = get_mp_manifest(M),
    Owner = MpM?MULTIPART_MANIFEST.owner,
    case wrap_riak_client(RcPidUnW) of
        {ok, RcPid} ->
            try
                Acl = case proplists:get_value(acl, Opts) of
                          undefined ->
                              riak_cs_acl_utils:canned_acl("private", Owner, undefined);
                          AnAcl ->
                              AnAcl
                      end,
                BagId = riak_cs_mb_helper:choose_bag_id(block, {Bucket, Key, UUID}),
                M2 = riak_cs_lfs_utils:set_bag_id(BagId, M),
                ClusterId = riak_cs_mb_helper:cluster_id(BagId),
                M3 = M2?MANIFEST{acl = Acl,
                                 cluster_id=ClusterId,
                                 write_start_time=os:timestamp()},
                {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key,
                                                                ?PID(RcPid)),
                try
                    ok = riak_cs_manifest_fsm:add_new_manifest(ManiPid, M3),
                    {ok, M3?MANIFEST.uuid}
                after
                    ok = riak_cs_manifest_fsm:stop(ManiPid)
                end
            after
                wrap_close_riak_client(RcPid)
            end;
        Else ->
            Else
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_part_common(Op, Bucket, Key, UploadId, {_,_,CallerKeyId} = _Caller, Props, RcPidUnW) ->
    case wrap_riak_client(RcPidUnW) of
        {ok, RcPid} ->
            try
                case riak_cs_manifest:get_manifests(?PID(RcPid), Bucket, Key) of
                    {ok, Obj, Manifests} ->
                        case find_manifest_with_uploadid(UploadId, Manifests) of
                            false ->
                                {error, notfound};
                            M when M?MANIFEST.state == writing ->
                                MpM = get_mp_manifest(M),
                                {_, _, MpMOwner} = MpM?MULTIPART_MANIFEST.owner,
                                case CallerKeyId == MpMOwner of
                                    true ->
                                        do_part_common2(Op, ?PID(RcPid),
                                                        M, Obj, MpM, Props);
                                    false ->
                                        {error, access_denied}
                                end;
                            _ ->
                                {error, notfound}
                        end;
                    Else2 ->
                        Else2
                end
            catch error:{badmatch, {m_icbo, _}} ->
                    {error, access_denied};
                  error:{badmatch, {m_umwc, _}} ->
                    {error, riak_unavailable}
            after
                wrap_close_riak_client(RcPid)
            end;
        Else ->
            Else
    end.

do_part_common2(abort, RcPid, M, Obj, _Mpm, _Props) ->
        {Bucket, Key} = M?MANIFEST.bkey,
        case riak_cs_gc:gc_specific_manifests(
               [M?MANIFEST.uuid], Obj, Bucket, Key, RcPid) of
            {ok, _NewObj} ->
                ok;
            Else3 ->
                Else3
        end;
do_part_common2(complete, RcPid,
                ?MANIFEST{uuid = _UUID, props = MProps} = Manifest,
                _Obj, MpM, Props) ->
    %% The content_md5 is used by WM to create the ETags header.
    %% However/fortunately/sigh-of-relief, Amazon's S3 doesn't use
    %% the file contents for ETag for a completeted multipart
    %% upload.
    %%
    %% However, if we add the hypen suffix here, e.g., "-1", then
    %% the WM etags doodad will simply convert that suffix to
    %% extra hex digits "2d31" instead.  So, hrm, what to do here.
    %%
    %% https://forums.aws.amazon.com/thread.jspa?messageID=203436&#203436
    %% BogoMD5 = iolist_to_binary([UUID, "-1"]),
    {PartETags} = proplists:get_value(complete, Props),
    try
        {Bucket, Key} = Manifest?MANIFEST.bkey,
        {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RcPid),
        try
            {Bytes, OverAllMD5, PartsToKeep, PartsToDelete} = comb_parts(MpM, PartETags),
            true = enforce_part_size(PartsToKeep),
            NewMpM = MpM?MULTIPART_MANIFEST{parts = PartsToKeep,
                                            done_parts = [],
                                            cleanup_parts = PartsToDelete},
            %% If [] = PartsToDelete, then we only need to update
            %% the manifest once.
            MProps2 = case PartsToDelete of
                          [] ->
                              [multipart_clean] ++
                                  replace_mp_manifest(NewMpM, MProps);
                          _ ->
                              replace_mp_manifest(NewMpM, MProps)
                      end,
            ContentMD5 = {OverAllMD5, "-" ++ integer_to_list(ordsets:size(PartsToKeep))},
            NewManifest = Manifest?MANIFEST{state = active,
                                            content_length = Bytes,
                                            content_md5 = ContentMD5,
                                            props = MProps2},
            ok = riak_cs_manifest_fsm:add_new_manifest(ManiPid, NewManifest),
            case PartsToDelete of
                [] ->
                    {ok, NewManifest};
                _ ->
                    %% Create fake S3 object manifests for this part,
                    %% then pass them to the GC monster for immediate
                    %% deletion.
                    BagId = riak_cs_mb_helper:bag_id_from_manifest(NewManifest),
                    ok = move_dead_parts_to_gc(Bucket, Key, BagId,
                                               PartsToDelete, RcPid),
                    MProps3 = [multipart_clean|MProps2],
                    New2Manifest = NewManifest?MANIFEST{props = MProps3},
                    ok = riak_cs_manifest_fsm:update_manifest(
                           ManiPid, New2Manifest),
                    {ok, New2Manifest}
            end
        after
            ok = riak_cs_manifest_fsm:stop(ManiPid)
        end
    catch error:{badmatch, {m_umwc, _}} ->
            {error, riak_unavailable};
          throw:bad_etag ->
            {error, bad_etag};
          throw:bad_etag_order ->
            {error, bad_etag_order};
          throw:entity_too_small ->
            {error, entity_too_small}
    end;
do_part_common2(list, _RcPid, _M, _Obj, MpM, Props) ->
    {_Opts} = proplists:get_value(list, Props),
    ?MULTIPART_MANIFEST{parts = Parts, done_parts = DoneParts0} = MpM,
    DoneParts = orddict:from_list(ordsets:to_list(DoneParts0)),
    ETagPs = lists:foldl(fun(P, Acc) ->
                                 case orddict:find(P?PART_MANIFEST.part_id,
                                                   DoneParts) of
                                     error ->
                                         Acc;
                                     {ok, ETag} ->
                                         [{ETag, P}|Acc]
                                 end
                         end, [], Parts),
    Ds = [?PART_DESCR{part_number = P?PART_MANIFEST.part_number,
                      %% TODO: technically, start_time /= last_modified
                      last_modified = riak_cs_wm_utils:iso_8601_datetime(calendar:now_to_local_time(P?PART_MANIFEST.start_time)),
                      etag = ETag,
                      size = P?PART_MANIFEST.content_length} ||
             {ETag, P} <- ETagPs],
    {ok, Ds};
do_part_common2(upload_part, RcPid, M, _Obj, MpM, Props) ->
    {Bucket, Key, _UploadId, _Caller, PartNumber, Size} =
        proplists:get_value(upload_part, Props),
    BlockSize = riak_cs_lfs_utils:block_size(),
    BagId = riak_cs_mb_helper:bag_id_from_manifest(M),
    {ok, PutPid} = riak_cs_put_fsm:start_link(
                     {Bucket, Key, Size, <<"x-riak/multipart-part">>,
                      orddict:new(), BlockSize, M?MANIFEST.acl,
                      infinity, self(), RcPid},
                     false, BagId),
    try
        ?MANIFEST{content_length = ContentLength,
                  props = MProps} = M,
        ?MULTIPART_MANIFEST{parts = Parts} = MpM,
        PartUUID = riak_cs_put_fsm:get_uuid(PutPid),
        PM = ?PART_MANIFEST{bucket = Bucket,
                            key = Key,
                            start_time = os:timestamp(),
                            part_number = PartNumber,
                            part_id = PartUUID,
                            content_length = Size,
                            block_size = BlockSize},
        NewMpM = MpM?MULTIPART_MANIFEST{parts = ordsets:add_element(PM, Parts)},
        NewM = M?MANIFEST{content_length = ContentLength + Size,
                          props = replace_mp_manifest(NewMpM, MProps)},

        ok = update_manifest_with_confirmation(RcPid, NewM),
        {upload_part_ready, PartUUID, PutPid}
    catch error:{badmatch, {m_umwc, _}} ->
            riak_cs_put_fsm:force_stop(PutPid),
            {error, riak_unavailable}
    end;
do_part_common2(upload_part_finished, RcPid, M, _Obj, MpM, Props) ->
    {PartUUID, MD5} = proplists:get_value(upload_part_finished, Props),
    try
        ?MULTIPART_MANIFEST{parts = Parts, done_parts = DoneParts} = MpM,
        DoneUUIDs = ordsets:from_list([UUID || {UUID, _ETag} <- DoneParts]),
        case {lists:keyfind(PartUUID, ?PART_MANIFEST.part_id,
                            ordsets:to_list(Parts)),
              ordsets:is_element(PartUUID, DoneUUIDs)} of
            {false, _} ->
                {error, notfound};
            {_, true} ->
                {error, notfound};
            {PM, false} when is_record(PM, ?PART_MANIFEST_RECNAME) ->
                ?MANIFEST{props = MProps} = M,
                NewMpM = MpM?MULTIPART_MANIFEST{
                               done_parts = ordsets:add_element({PartUUID, MD5},
                                                                DoneParts)},
                NewM = M?MANIFEST{props = replace_mp_manifest(NewMpM, MProps)},
                ok = update_manifest_with_confirmation(RcPid, NewM)
        end
    catch error:{badmatch, {m_umwc, _}} ->
            {error, riak_unavailable}
    end.

update_manifest_with_confirmation(RcPid, Manifest) ->
    {Bucket, Key} = Manifest?MANIFEST.bkey,
    {m_umwc, {ok, ManiPid}} = {m_umwc,
                               riak_cs_manifest_fsm:start_link(Bucket, Key,
                                                               RcPid)},
    try
        ok = riak_cs_manifest_fsm:update_manifest_with_confirmation(ManiPid,
                                                                    Manifest)
    after
        ok = riak_cs_manifest_fsm:stop(ManiPid)
    end.

-spec make_2i_key(binary()) -> binary().
make_2i_key(Bucket) ->
    make_2i_key2(Bucket, "").

-spec make_2i_key(binary(), acl_owner()) -> binary().
make_2i_key(Bucket, {_, _, OwnerStr}) ->
    make_2i_key2(Bucket, OwnerStr).

-spec make_2i_key2(binary(), string()) -> binary().
make_2i_key2(Bucket, OwnerStr) ->
    iolist_to_binary(["rcs@", OwnerStr, "@", Bucket, "_bin"]).

list_multipart_uploads2(Bucket, RcPid, Names, Opts) ->
    FilterFun =
        fun(K, Acc) ->
                filter_uploads_list(Bucket, K, Opts, RcPid, Acc)
        end,
    {Manifests, Prefixes} = lists:foldl(FilterFun, {[], ordsets:new()}, Names),
    {lists:sort(Manifests), ordsets:to_list(Prefixes)}.

filter_uploads_list(Bucket, Key, Opts, RcPid, Acc) ->
    multipart_manifests_for_key(Bucket, Key, Opts, Acc, RcPid).

parameter_filter(M, Acc, _, _, KeyMarker, _)
  when M?MULTIPART_DESCR.key =< KeyMarker->
    Acc;
parameter_filter(M, Acc, _, _, KeyMarker, UploadIdMarker)
  when M?MULTIPART_DESCR.key =< KeyMarker andalso
       M?MULTIPART_DESCR.upload_id =< UploadIdMarker ->
    Acc;
parameter_filter(M, {Manifests, Prefixes}, undefined, undefined, _, _) ->
    {[M | Manifests], Prefixes};
parameter_filter(M, Acc, undefined, Delimiter, _, _) ->
    Group = extract_group(M?MULTIPART_DESCR.key, Delimiter),
    update_keys_and_prefixes(Acc, M, <<>>, 0, Group);
parameter_filter(M, {Manifests, Prefixes}, Prefix, undefined, _, _) ->
    PrefixLen = byte_size(Prefix),
    case M?MULTIPART_DESCR.key of
        << Prefix:PrefixLen/binary, _/binary >> ->
            {[M | Manifests], Prefixes};
        _ ->
            {Manifests, Prefixes}
    end;
parameter_filter(M, {Manifests, Prefixes}=Acc, Prefix, Delimiter, _, _) ->
    PrefixLen = byte_size(Prefix),
    case M?MULTIPART_DESCR.key of
        << Prefix:PrefixLen/binary, Rest/binary >> ->
            Group = extract_group(Rest, Delimiter),
            update_keys_and_prefixes(Acc, M, Prefix, PrefixLen, Group);
        _ ->
            {Manifests, Prefixes}
    end.

extract_group(Key, Delimiter) ->
    case binary:match(Key, [Delimiter]) of
        nomatch ->
            nomatch;
        {Pos, Len} ->
            binary:part(Key, {0, Pos+Len})
    end.

update_keys_and_prefixes({Keys, Prefixes}, Key, _, _, nomatch) ->
    {[Key | Keys], Prefixes};
update_keys_and_prefixes({Keys, Prefixes}, _, Prefix, PrefixLen, Group) ->
    NewPrefix = << Prefix:PrefixLen/binary, Group/binary >>,
    {Keys, ordsets:add_element(NewPrefix, Prefixes)}.

multipart_manifests_for_key(Bucket, Key, Opts, Acc, RcPid) ->
    ParameterFilter = build_parameter_filter(Opts),
    Manifests = handle_get_manifests_result(
                  riak_cs_manifest:get_manifests(RcPid, Bucket, Key)),
    lists:foldl(ParameterFilter, Acc, Manifests).

build_parameter_filter(Opts) ->
    Prefix = proplists:get_value(prefix, Opts),
    Delimiter = proplists:get_value(delimiter, Opts),
    KeyMarker = proplists:get_value(key_marker, Opts),
    UploadIdMarker = base64url:decode(
                           proplists:get_value(upload_id_marker, Opts)),
    build_parameter_filter(Prefix, Delimiter, KeyMarker, UploadIdMarker).

build_parameter_filter(Prefix, Delimiter, KeyMarker, UploadIdMarker) ->
    fun(Key, Acc) ->
            parameter_filter(Key, Acc, Prefix, Delimiter, KeyMarker, UploadIdMarker)
    end.

handle_get_manifests_result({ok, _Obj, Manifests}) ->
   [multipart_description(M)
                 || {_, M} <- Manifests,
                    M?MANIFEST.state == writing,
                    is_multipart_manifest(M)];
handle_get_manifests_result(_) ->
    [].

-spec is_multipart_manifest(?MANIFEST{}) -> boolean().
is_multipart_manifest(?MANIFEST{props=Props}) ->
    case proplists:get_value(multipart, Props) of
        undefined ->
            false;
        _ ->
             true
    end.

-spec multipart_description(?MANIFEST{}) -> ?MULTIPART_DESCR{}.
multipart_description(Manifest) ->
    MpM =  proplists:get_value(multipart, Manifest?MANIFEST.props),
    ?MULTIPART_DESCR{
       key = element(2, Manifest?MANIFEST.bkey),
       upload_id = Manifest?MANIFEST.uuid,
       owner_key_id = element(3, MpM?MULTIPART_MANIFEST.owner),
       owner_display = element(1, MpM?MULTIPART_MANIFEST.owner),
       initiated = Manifest?MANIFEST.created}.

%% @doc Will cause error:{badmatch, {m_ibco, _}} if CallerKeyId does not exist

is_caller_bucket_owner(RcPid, Bucket, CallerKeyId) ->
    {m_icbo, {ok, {C, _}}} = {m_icbo, riak_cs_user:get_user(CallerKeyId, RcPid)},
    Buckets = [iolist_to_binary(B?RCS_BUCKET.name) ||
                  B <- riak_cs_bucket:get_buckets(C)],
    lists:member(Bucket, Buckets).

find_manifest_with_uploadid(UploadId, Manifests) ->
    case lists:keyfind(UploadId, 1, Manifests) of
        false ->
            false;
        {UploadId, M} ->
            M
    end.

%% @doc In #885 (https://github.com/basho/riak_cs/issues/855) it
%% happened to be revealed that ETag is generated as
%%
%% > ETag = MD5(Sum(p \in numberParts, MD5(PartBytes(p))) + "-" + numberParts
%%
%% by an Amazon support guy, Hubert.
%% https://forums.aws.amazon.com/thread.jspa?messageID=456442
-spec comb_parts(multipart_manifest(), list({non_neg_integer(), binary()})) ->
                        {KeepBytes::non_neg_integer(),
                         OverAllMD5::binary(),
                         PartsToKeep::list(),
                         PartsToDelete::list()}.
comb_parts(MpM, PartETags) ->
    Done = orddict:from_list(ordsets:to_list(MpM?MULTIPART_MANIFEST.done_parts)),
    %% TODO: Strictly speaking, this implementation could have
    %%       problems with MD5 hash collisions.  I'd *really* wanted
    %%       to avoid using MD5 hash used as the part ETags (see the
    %%       "Once upon a time" comment for upload_part_finished()
    %%       above).  According to AWS S3 docs, we're supposed to take
    %%       the newest part that has this {PartNum, ETag} pair.
    Parts0 = ordsets:to_list(MpM?MULTIPART_MANIFEST.parts),
    FindOrSet = fun(Key, Dict) -> case orddict:find(Key, Dict) of
                                      {ok, Value} -> Value;
                                      error       -> <<>>
                                  end
                end,
    Parts = [P?PART_MANIFEST{content_md5 = FindOrSet(P?PART_MANIFEST.part_id, Done)} ||
                P <- Parts0],
    All = dict:from_list(
            [{{PM?PART_MANIFEST.part_number, PM?PART_MANIFEST.content_md5}, PM} ||
                PM <- Parts]),
    Keep0 = dict:new(),
    Delete0 = dict:new(),
    {_, Keep, _Delete, _, KeepBytes, KeepPMs, MD5Context} =
        lists:foldl(fun comb_parts_fold/2,
                    {All, Keep0, Delete0, 0, 0, [], riak_cs_utils:md5_init()}, PartETags),
    ToDelete = [PM || {_, PM} <-
                          dict:to_list(
                            dict:filter(fun(K, _V) ->
                                             not dict:is_key(K, Keep) end,
                                        All))],
    {KeepBytes, riak_cs_utils:md5_final(MD5Context), lists:reverse(KeepPMs), ToDelete}.

comb_parts_fold({PartNum, _ETag} = _K,
                {_All, _Keep, _Delete, LastPartNum, _Bytes, _KeepPMs, _})
  when PartNum =< LastPartNum orelse PartNum < 1 ->
    throw(bad_etag_order);
comb_parts_fold({PartNum, ETag} = K,
                {All, Keep, Delete, _LastPartNum, Bytes, KeepPMs, MD5Context}) ->
    case {dict:find(K, All), dict:is_key(K, Keep)} of
        {{ok, PM}, false} ->
            {All, dict:store(K, true, Keep), Delete, PartNum,
             Bytes + PM?PART_MANIFEST.content_length, [PM|KeepPMs],
             riak_cs_utils:md5_update(MD5Context, ETag)};
        _X ->
            throw(bad_etag)
    end.

move_dead_parts_to_gc(Bucket, Key, BagId, PartsToDelete, RcPid) ->
    PartDelMs = [{P_UUID,
                  riak_cs_lfs_utils:new_manifest(
                    Bucket,
                    Key,
                    P_UUID,
                    ContentLength,
                    <<"x-delete/now">>,
                    undefined,
                    [],
                    P_BlockSize,
                    no_acl_yet,
                    [],
                    undefined,
                    BagId)} ||
                    ?PART_MANIFEST{part_id=P_UUID,
                                   content_length=ContentLength,
                                   block_size=P_BlockSize} <- PartsToDelete],
    ok = riak_cs_gc:move_manifests_to_gc_bucket(PartDelMs, RcPid).

enforce_part_size(PartsToKeep) ->
    case riak_cs_config:enforce_multipart_part_size() of
        true ->
            eval_part_sizes([P?PART_MANIFEST.content_length || P <- PartsToKeep]);
        false ->
            true
    end.

eval_part_sizes([]) ->
    true;
eval_part_sizes([_]) ->
    true;
eval_part_sizes(L) ->
    case lists:min(lists:sublist(L, length(L)-1)) of
        X when X < ?MIN_MP_PART_SIZE ->
            throw(entity_too_small);
        _ ->
            true
    end.

%% The intent of the wrap_* functions is to make this module's code
%% flexible enough to support two methods of operation:
%%
%% 1. Allocate its own Riak client pids (as originally written)
%% 2. Use a Riak client pid passed in by caller (later to interface with WM)
%%
%% If we're to allocate our own Riak client pids, we use the atom 'nopid'.

wrap_riak_client(nopid) ->
    case riak_cs_riak_client:checkout() of
        {ok, RcPid} ->
            {ok, {local_pid, RcPid}};
        Else ->
            Else
    end;
wrap_riak_client(RcPid) ->
    {ok, {remote_pid, RcPid}}.

wrap_close_riak_client({local_pid, RcPid}) ->
    riak_cs_riak_client:checkin(RcPid);
wrap_close_riak_client({remote_pid, _RcPid}) ->
    ok.

get_riak_client_pid({local_pid, RcPid}) ->
    RcPid;
get_riak_client_pid({remote_pid, RcPid}) ->
    RcPid.

-spec get_mp_manifest(lfs_manifest()) -> multipart_manifest() | 'undefined'.
get_mp_manifest(?MANIFEST{props = Props}) when is_list(Props) ->
    %% TODO: When the version number of the multipart_manifest_v1 changes
    %%       to version v2 and beyond, this might be a good place to add
    %%       a record conversion function to handle older versions of
    %%       the multipart record?
    proplists:get_value(multipart, Props, undefined);
get_mp_manifest(_) ->
    undefined.

replace_mp_manifest(MpM, Props) ->
    [{multipart, MpM}|proplists:delete(multipart, Props)].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

eval_part_sizes_test() ->
    true = eval_part_sizes_wrapper([]),
    true = eval_part_sizes_wrapper([999888777]),
    true = eval_part_sizes_wrapper([777]),
    false = eval_part_sizes_wrapper([1,51048576,51048576,51048576,436276]),
    false = eval_part_sizes_wrapper([51048576,1,51048576,51048576,436276]),
    false = eval_part_sizes_wrapper([51048576,1,51048576,51048576,436276]),
    false = eval_part_sizes_wrapper([1,51048576,51048576,51048576]),
    false = eval_part_sizes_wrapper([51048576,1,51048576,51048576]),
    true  = eval_part_sizes_wrapper([51048576,51048576,51048576,1]),
    ok.

eval_part_sizes_wrapper(L) ->
    try
        eval_part_sizes(L)
    catch
        throw:entity_too_small ->
            false
    end.

comb_parts_test() ->
    Num = 5,
    GoodETags = [{X, <<(X+$0):8>>} || X <- lists:seq(1, Num)],
    GoodDones = [{ETag, ETag} || {_, ETag} <- GoodETags],
    PMs = [?PART_MANIFEST{part_number = X, part_id = Y, content_length = X} ||
              {X, Y} <- GoodETags],
    BadETags = [{X, <<(X+$0):8>>} || X <- lists:seq(Num + 1, Num + 1 + Num)],
    MpM1 = ?MULTIPART_MANIFEST{parts = ordsets:from_list(PMs),
                               done_parts = ordsets:from_list(GoodDones)},
    try
        _ = comb_parts(MpM1, GoodETags ++ BadETags),
        throw(test_failed)
    catch
        throw:bad_etag ->
            ok
    end,
    try
        _ = comb_parts(MpM1, [lists:last(GoodETags)|tl(GoodETags)]),
        throw(test_failed)
    catch
        throw:bad_etag_order ->
            ok
    end,

    MD51 = riak_cs_utils:md5_final(lists:foldl(fun({_, ETag}, MD5Context) ->
                                                       riak_cs_utils:md5_update(MD5Context, ETag)
                                               end,
                                               riak_cs_utils:md5_init(),
                                               GoodETags)),
    MD52 = riak_cs_utils:md5_final(lists:foldl(fun({_, ETag}, MD5Context) ->
                                                       riak_cs_utils:md5_update(MD5Context, ETag)
                                               end,
                                               riak_cs_utils:md5_init(),
                                               tl(GoodETags))),

    {15, MD51, Keep1, []} = comb_parts(MpM1, GoodETags),
    5 = length(Keep1),
    Keep1 = lists:usort(Keep1),

    {14, MD52, Keep2, [PM2]} = comb_parts(MpM1, tl(GoodETags)),
    4 = length(Keep2),
    Keep2 = lists:usort(Keep2),
    1 = PM2?PART_MANIFEST.part_number,

    ok.

-ifdef(EQC).

eval_part_sizes_eqc_test()                                                              ->
    true = eqc:quickcheck(eqc:numtests(500, prop_part_sizes())).

prop_part_sizes()                                                                       ->
    Min = ?MIN_MP_PART_SIZE,
    Min_1 = Min - 1,
    MinMinus100 = Min - 100,
    MinPlus100 = Min + 100,
    ?FORALL({L, Last, Either},
            {list(choose(Min, MinPlus100)), choose(0, Min_1), choose(MinMinus100, MinPlus100)},
            true == eval_part_sizes_wrapper(L ++ [Last]) andalso
            false == eval_part_sizes_wrapper(L ++ [Min_1] ++ L ++ [Either])
           ).

-endif. % EQC

-endif.
