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

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MIN_MP_PART_SIZE, (5*1024*1024)).

-define(PID(WrappedPid), get_riakc_pid(WrappedPid)).

%% export Public API
-export([
         abort_multipart_upload/4, abort_multipart_upload/5,
         calc_multipart_2i_dict/3,
         clean_multipart_unused_parts/2,
         complete_multipart_upload/5, complete_multipart_upload/6,
         initiate_multipart_upload/5, initiate_multipart_upload/6,
         list_multipart_uploads/3, list_multipart_uploads/4,
         list_parts/5, list_parts/6,
         make_content_types_accepted/2,
         make_content_types_accepted/3,
         make_special_error/1,
         make_special_error/4,
         new_manifest/5,
         upload_part/6, upload_part/7,
         upload_part_1blob/2,
         upload_part_finished/7, upload_part_finished/8,
         user_rec_to_3tuple/1,
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

abort_multipart_upload(Bucket, Key, UploadId, Caller, RiakcPidUnW) ->
    do_part_common(abort, Bucket, Key, UploadId, Caller, [], RiakcPidUnW).

clean_multipart_unused_parts(?MANIFEST{bkey=BKey, props=Props} = Manifest,
                             RiakcPid) ->
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
                        ok = move_dead_parts_to_gc(Bucket, Key, PartsToDelete,
                                                   RiakcPid),
                        UpdManifest = Manifest?MANIFEST{props=[multipart_clean|Props]},
                        ok = update_manifest_with_confirmation(RiakcPid, UpdManifest)
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

complete_multipart_upload(Bucket, Key, UploadId, PartETags, Caller, RiakcPidUnW) ->
    Extra = {PartETags},
    do_part_common(complete, Bucket, Key, UploadId, Caller, [{complete, Extra}],
                   RiakcPidUnW).

initiate_multipart_upload(Bucket, Key, ContentType, Owner, Opts) ->
    initiate_multipart_upload(Bucket, Key, ContentType, Owner, Opts, nopid).

initiate_multipart_upload(Bucket, Key, ContentType, {_,_,_} = Owner,
                          Opts, RiakcPidUnW) ->
    write_new_manifest(new_manifest(Bucket, Key, ContentType, Owner, Opts),
                       Opts, RiakcPidUnW).

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
    riak_cs_xml:export_xml([XmlDoc]).

list_multipart_uploads(Bucket, Caller, Opts) ->
    list_multipart_uploads(Bucket, Caller, Opts, nopid).

list_multipart_uploads(Bucket, {_Display, _Canon, CallerKeyId} = Caller,
                       Opts, RiakcPidUnW) ->
    case wrap_riak_connection(RiakcPidUnW) of
        {ok, RiakcPid} ->
            try
                BucketOwnerP = is_caller_bucket_owner(?PID(RiakcPid),
                                                      Bucket, CallerKeyId),
                Key2i = case BucketOwnerP of
                            true ->
                                make_2i_key(Bucket); % caller = bucket owner
                            false ->
                                make_2i_key(Bucket, Caller)
                        end,
                HashBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
                case riakc_pb_socket:get_index(?PID(RiakcPid), HashBucket,
                                               Key2i, <<"1">>) of
                    {ok, Names} ->
                        MyCaller = case BucketOwnerP of
                                       true -> owner;
                                       _    -> CallerKeyId
                                   end,
                        {ok, list_multipart_uploads2(Bucket, ?PID(RiakcPid),
                                                     Names, Opts, MyCaller)};
                    Else2 ->
                        Else2
                end
            catch error:{badmatch, {m_icbo, _}} ->
                    {error, access_denied}
            after
                wrap_close_riak_connection(RiakcPid)
            end;
        Else ->
            Else
    end.

list_parts(Bucket, Key, UploadId, Caller, Opts) ->
    list_parts(Bucket, Key, UploadId, Caller, Opts, nopid).

list_parts(Bucket, Key, UploadId, Caller, Opts, RiakcPidUnW) ->
    Extra = {Opts},
    do_part_common(list, Bucket, Key, UploadId, Caller, [{list, Extra}],
                   RiakcPidUnW).

%% @doc
-spec new_manifest(binary(), binary(), binary(), acl_owner(), list()) -> lfs_manifest().
new_manifest(Bucket, Key, ContentType, {_, _, _} = Owner, Opts) ->
    UUID = druuid:v4(),
    %% TODO: add object metadata here, e.g. content-disposition et al.
    %% TODO: add cluster_id ... which means calling new_manifest/11 not /9.
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
                                       no_acl_yet),
    MpM = ?MULTIPART_MANIFEST{upload_id = UUID,
                              owner = Owner},
    M?MANIFEST{props = replace_mp_manifest(MpM, M?MANIFEST.props)}.

upload_part(Bucket, Key, UploadId, PartNumber, Size, Caller) ->
    upload_part(Bucket, Key, UploadId, PartNumber, Size, Caller, nopid).

upload_part(Bucket, Key, UploadId, PartNumber, Size, Caller, RiakcPidUnW) ->
    Extra = {Bucket, Key, UploadId, Caller, PartNumber, Size},
    do_part_common(upload_part, Bucket, Key, UploadId, Caller,
                   [{upload_part, Extra}], RiakcPidUnW).

upload_part_1blob(PutPid, Blob) ->
    ok = riak_cs_put_fsm:augment_data(PutPid, Blob),
    {ok, M} = riak_cs_put_fsm:finalize(PutPid),
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
                     Caller, RiakcPidUnW) ->
    Extra = {PartUUID, MD5},
    do_part_common(upload_part_finished, Bucket, Key, UploadId,
                   Caller, [{upload_part_finished, Extra}], RiakcPidUnW).

user_rec_to_3tuple(U) ->
    %% acl_owner3: {display name, canonical id, key id}
    {U?RCS_USER.display_name, U?RCS_USER.canonical_id,
     U?RCS_USER.key_id}.

write_new_manifest(M, Opts, RiakcPidUnW) ->
    MpM = get_mp_manifest(M),
    Owner = MpM?MULTIPART_MANIFEST.owner,
    case wrap_riak_connection(RiakcPidUnW) of
        {ok, RiakcPid} ->
            try
                Acl = case proplists:get_value(acl, Opts) of
                          undefined ->
                              % 4th arg, pid(), unused but honor the contract
                              riak_cs_acl_utils:canned_acl("private", Owner, undefined);
                          AnAcl ->
                              AnAcl
                      end,
                ClusterId = riak_cs_config:cluster_id(?PID(RiakcPid)),
                M2 = M?MANIFEST{acl = Acl,
                                cluster_id = ClusterId,
                                write_start_time=os:timestamp()},
                {Bucket, Key} = M?MANIFEST.bkey,
                {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key,
                                                                ?PID(RiakcPid)),
                try
                    ok = riak_cs_manifest_fsm:add_new_manifest(ManiPid, M2),
                    {ok, M2?MANIFEST.uuid}
                after
                    ok = riak_cs_manifest_fsm:stop(ManiPid)
                end
            after
                wrap_close_riak_connection(RiakcPid)
            end;
        Else ->
            Else
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_part_common(Op, Bucket, Key, UploadId, {_,_,CallerKeyId} = _Caller, Props, RiakcPidUnW) ->
    case wrap_riak_connection(RiakcPidUnW) of
        {ok, RiakcPid} ->
            try
                case riak_cs_utils:get_manifests(?PID(RiakcPid), Bucket, Key) of
                    {ok, Obj, Manifests} ->
                        case find_manifest_with_uploadid(UploadId, Manifests) of
                            false ->
                                {error, notfound};
                            M when M?MANIFEST.state == writing ->
                                MpM = get_mp_manifest(M),
                                {_, _, MpMOwner} = MpM?MULTIPART_MANIFEST.owner,
                                case CallerKeyId == MpMOwner of
                                    true ->
                                        do_part_common2(Op, ?PID(RiakcPid), M,
                                                        Obj, MpM, Props);
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
                wrap_close_riak_connection(RiakcPid)
            end;
        Else ->
            Else
    end.

do_part_common2(abort, RiakcPid, M, Obj, _Mpm, _Props) ->
        {Bucket, Key} = M?MANIFEST.bkey,
        case riak_cs_gc:gc_specific_manifests(
               [M?MANIFEST.uuid], Obj, Bucket, Key, RiakcPid) of
            {ok, _NewObj} ->
                ok;
            Else3 ->
                Else3
        end;
do_part_common2(complete, RiakcPid,
                ?MANIFEST{uuid = UUID, props = MProps} = Manifest,
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
        {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RiakcPid),
        try
                {Bytes, PartsToKeep, PartsToDelete} = comb_parts(MpM, PartETags),
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
                NewManifest = Manifest?MANIFEST{state = active,
                                                content_length = Bytes,
                                                content_md5 = {UUID, "-" ++ integer_to_list(ordsets:size(PartsToKeep))},
                                                props = MProps2},
                ok = riak_cs_manifest_fsm:add_new_manifest(ManiPid, NewManifest),
                case PartsToDelete of
                    [] ->
                        ok;
                    _ ->
                        %% Create fake S3 object manifests for this part,
                        %% then pass them to the GC monster for immediate
                        %% deletion.
                        ok = move_dead_parts_to_gc(Bucket, Key, PartsToDelete, RiakcPid),
                        MProps3 = [multipart_clean|MProps2],
                        New2Manifest = NewManifest?MANIFEST{props = MProps3},
                        ok = riak_cs_manifest_fsm:update_manifest(
                               ManiPid, New2Manifest)
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
do_part_common2(list, _RiakcPid, _M, _Obj, MpM, Props) ->
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
do_part_common2(upload_part, RiakcPid, M, _Obj, MpM, Props) ->
    {Bucket, Key, _UploadId, _Caller, PartNumber, Size} =
        proplists:get_value(upload_part, Props),
    BlockSize = riak_cs_lfs_utils:block_size(),
    {ok, PutPid} = riak_cs_put_fsm:start_link(
                     {Bucket, Key, Size, <<"x-riak/multipart-part">>,
                      orddict:new(), BlockSize, M?MANIFEST.acl,
                      infinity, self(), RiakcPid},
                     false),
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
        ok = update_manifest_with_confirmation(RiakcPid, NewM),
        {upload_part_ready, PartUUID, PutPid}
    catch error:{badmatch, {m_umwc, _}} ->
            riak_cs_put_fsm:force_stop(PutPid),
            {error, riak_unavailable}
    end;
do_part_common2(upload_part_finished, RiakcPid, M, _Obj, MpM, Props) ->
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
                ok = update_manifest_with_confirmation(RiakcPid, NewM)
        end
    catch error:{badmatch, {m_umwc, _}} ->
            {error, riak_unavailable}
    end.

update_manifest_with_confirmation(RiakcPid, Manifest) ->
    {Bucket, Key} = Manifest?MANIFEST.bkey,
    {m_umwc, {ok, ManiPid}} = {m_umwc,
                               riak_cs_manifest_fsm:start_link(Bucket, Key,
                                                               RiakcPid)},
    try
        ok = riak_cs_manifest_fsm:update_manifest_with_confirmation(ManiPid,
                                                                    Manifest)
    after
        ok = riak_cs_manifest_fsm:stop(ManiPid)
    end.

make_2i_key(Bucket) ->
    make_2i_key2(Bucket, "").

make_2i_key(Bucket, {_, _, OwnerStr}) ->
    make_2i_key2(Bucket, OwnerStr);
make_2i_key(Bucket, undefined) ->
    %% I can't figure this one out:
    %%     riak_cs_mp_utils.erl:516: Guard test is_list(OwnerStr::'undefined') can never succeed
    %% or, if I try to work around:
    %%     riak_cs_mp_utils.erl:529: The pattern <Bucket, OwnerStr> can never match since previous clauses completely covered the type <_,'undefined' | {_,_,_}>
    %%
    %% If I use "typer" to infer types for this func, the 'undefined' atom
    %% is not inferred.  'undefined' isn't part of the valid type for
    %% ?MULTIPART_MANIFEST.owner.  {sigh}
    _ = try
            really_does_not_exist = get(really_does_not_exist)
        catch _:_ ->
                lager:error("~s:make_2i_key: error @ ~p\n",
                            [?MODULE, erlang:get_stacktrace()])
        end,
    iolist_to_binary(["rcs@undefined@", Bucket, "_bin"]);
make_2i_key(Bucket, OwnerStr) when is_list(OwnerStr) ->
    make_2i_key2(Bucket, OwnerStr).

make_2i_key2(Bucket, OwnerStr) ->
    iolist_to_binary(["rcs@", OwnerStr, "@", Bucket, "_bin"]).

list_multipart_uploads2(Bucket, RiakcPid, Names, Opts, _CallerKeyId) ->
    FilterFun =
        fun(K, Acc) ->
                filter_uploads_list(Bucket, K, Opts, RiakcPid, Acc)
        end,
    {Manifests, Prefixes} = lists:foldl(FilterFun, {[], ordsets:new()}, Names),
    {lists:sort(Manifests), ordsets:to_list(Prefixes)}.

filter_uploads_list(Bucket, Key, Opts, RiakcPid, Acc) ->
    multipart_manifests_for_key(Bucket, Key, Opts, Acc, RiakcPid).

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

multipart_manifests_for_key(Bucket, Key, Opts, Acc, RiakcPid) ->
    ParameterFilter = build_parameter_filter(Opts),
    Manifests = handle_get_manifests_result(
                  riak_cs_utils:get_manifests(RiakcPid, Bucket, Key)),
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

is_caller_bucket_owner(RiakcPid, Bucket, CallerKeyId) ->
    {m_icbo, {ok, {C, _}}} = {m_icbo, riak_cs_utils:get_user(CallerKeyId,
                                                             RiakcPid)},
    Buckets = [iolist_to_binary(B?RCS_BUCKET.name) ||
                  B <- riak_cs_utils:get_buckets(C)],
    lists:member(Bucket, Buckets).

find_manifest_with_uploadid(UploadId, Manifests) ->
    case lists:keyfind(UploadId, 1, Manifests) of
        false ->
            false;
        {UploadId, M} ->
            M
    end.

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
    {_, Keep, _Delete, _, KeepBytes, KeepPMs} =
        lists:foldl(fun comb_parts_fold/2,
                    {All, Keep0, Delete0, 0, 0, []}, PartETags),
    ToDelete = [PM || {_, PM} <-
                          dict:to_list(
                            dict:filter(fun(K, _V) ->
                                             not dict:is_key(K, Keep) end,
                                        All))],
    {KeepBytes, lists:reverse(KeepPMs), ToDelete}.

comb_parts_fold({PartNum, _ETag} = _K,
                {_All, _Keep, _Delete, LastPartNum, _Bytes, _KeepPMs})
  when PartNum =< LastPartNum orelse PartNum < 1 ->
    throw(bad_etag_order);
comb_parts_fold({PartNum, _ETag} = K,
                {All, Keep, Delete, _LastPartNum, Bytes, KeepPMs}) ->
    case {dict:find(K, All), dict:is_key(K, Keep)} of
        {{ok, PM}, false} ->
            {All, dict:store(K, true, Keep), Delete, PartNum,
             Bytes + PM?PART_MANIFEST.content_length, [PM|KeepPMs]};
        _X ->
            throw(bad_etag)
    end.

move_dead_parts_to_gc(Bucket, Key, PartsToDelete, RiakcPid) ->
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
                    no_acl_yet)} ||
                    ?PART_MANIFEST{part_id=P_UUID,
                                   content_length=ContentLength,
                                   block_size=P_BlockSize} <- PartsToDelete],
    ok = riak_cs_gc:move_manifests_to_gc_bucket(PartDelMs, RiakcPid, false).

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

wrap_riak_connection(nopid) ->
    case riak_cs_utils:riak_connection() of
        {ok, RiakcPid} ->
            {ok, {local_pid, RiakcPid}};
        Else ->
            Else
    end;
wrap_riak_connection(RiakcPid) ->
    {ok, {remote_pid, RiakcPid}}.

wrap_close_riak_connection({local_pid, RiakcPid}) ->
    riak_cs_utils:close_riak_connection(RiakcPid);
wrap_close_riak_connection({remote_pid, _RiakcPid}) ->
    ok.

get_riakc_pid({local_pid, RiakcPid}) ->
    RiakcPid;
get_riakc_pid({remote_pid, RiakcPid}) ->
    RiakcPid.

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

-ifdef(EQC).

eval_part_sizes_eqc_test() ->
    true = eqc:quickcheck(eqc:numtests(500, prop_part_sizes())).

prop_part_sizes() ->
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

%% The test_0() and test_1() commands can be used while Riak CS is running.
%% In fact, it's preferred (?) to do it that way because then all of the
%% Poolboy infrastructure is already set up.  However, then there's the
%% problem of getting the EUnit-compiled version of this module!
%% So, there are a couple of work-arounds:
%%
%% 1. If Riak CS is running via "make stage" or "make rel", do at the console:
%%       code:add_patha("../../.eunit").
%%       l(riak_cs_mp_utils).
%%       code:which(riak_cs_mp_utils).    % To verify EUnit version is loaded
%%
%% 2. Run without Riak CS:
%%       erl -pz .eunit ebin deps/*/ebin
%%    ... and then...
%%       riak_cs_mp_utils:test_setup().   % Scott's preferred environment
%%    or
%%       riak_cs_mp_utils:test_setup(RiakIpAddress, RiakPbPortNumber).
%%
%% Finally, to run the tests:
%%       ok = riak_cs_mp_utils:test_0().
%%       ok = riak_cs_mp_utils:test_1().

test_setup() ->
    test_setup("127.0.0.1", 8081).

test_setup(RiakIp, RiakPbPort) ->
    PoolList = [{request_pool,{128,0}},{bucket_list_pool,{5,0}}],
    WorkerStop = fun(Worker) -> riak_cs_riakc_pool_worker:stop(Worker) end,
    PoolSpecs = [{Name,
                  {poolboy, start_link, [[{name, {local, Name}},
                                          {worker_module, riak_cs_riakc_pool_worker},
                                          {size, Workers},
                                          {max_overflow, Overflow},
                                          {stop_fun, WorkerStop}]]},
                  permanent, 5000, worker, [poolboy]}
                 || {Name, {Workers, Overflow}} <- PoolList],
    application:set_env(riak_cs, riak_ip, RiakIp),
    application:set_env(riak_cs, riak_pb_port, RiakPbPort),
    [{ok, _}, {ok, _}] =
        [erlang:apply(M, F, A) || {_, {M, F, A}, _, _, _, _} <- PoolSpecs],
    application:start(folsom),
    riak_cs_stats:start_link(),
    ok.

test_0() ->
    test_cleanup_users(),
    test_cleanup_data(),
    test_create_users(),

    ID1 = test_initiate(test_user1()),
    ID2 = test_initiate(test_user2()),
    _ID1b = test_initiate(test_user1()),

    {ok, {X1, _}} = test_list_uploadids(test_user1(), []),
    3 = length(X1),
    {ok, {X2, _}} = test_list_uploadids(test_user2(), []),
    1 = length(X2),
    {error, access_denied} = test_list_uploadids(test_userNONE(), []),

    {error, access_denied} = test_abort(ID1, test_user2()),
    {error,notfound} = test_abort(<<"no such upload_id">>, test_user2()),
    ok = test_abort(ID1, test_user1()),
    {error, notfound} = test_abort(ID1, test_user1()),

    {error, access_denied} = test_complete(ID2, [], test_user1()),
    {error,notfound} = test_complete(<<"no such upload_id">>, [], test_user2()),
    ok = test_complete(ID2, [], test_user2()),
    {error, notfound} = test_complete(ID2, [], test_user2()),

    {ok, {X3, _}} = test_list_uploadids(test_user1(), []),
    1 = length(X3),
    {ok, {X4, _}} = test_list_uploadids(test_user2(), []),
    0 = length(X4),

    ok.

test_1() ->
    test_cleanup_users(),
    test_cleanup_data(),
    test_create_users(),

    ID1 = test_initiate(test_user1()),
    Bytes = 50,
    {ok, _PartID1, MD51} = test_upload_part(ID1, 1, <<42:(8*Bytes)>>, test_user1()),
    {ok, _PartID4, MD54} = test_upload_part(ID1, 4, <<43:(8*Bytes)>>, test_user1()),
    {ok, _PartID9, MD59} = test_upload_part(ID1, 9, <<44:(8*Bytes)>>, test_user1()),
    ok = test_complete(ID1, [{1, MD51}, {4, MD54}, {9, MD59}], test_user1()).

test_initiate(User) ->
    {ok, ID} = initiate_multipart_upload(
                 test_bucket1(), test_key1(), <<"text/plain">>, User, []),
    ID.

test_abort(UploadId, User) ->
    abort_multipart_upload(test_bucket1(), test_key1(), UploadId, User).

test_complete(UploadId, PartETags, User) ->
    complete_multipart_upload(test_bucket1(), test_key1(), UploadId, PartETags, User).

test_list_uploadids(User, Opts) ->
    list_multipart_uploads(test_bucket1(), User, Opts).

test_upload_part(UploadId, PartNumber, Blob, User) ->
    Size = byte_size(Blob),
    {upload_part_ready, PartUUID, PutPid} =
        upload_part(test_bucket1(), test_key1(), UploadId, PartNumber, Size, User),
    {ok, MD5} = upload_part_1blob(PutPid, Blob),
    {error, notfound} =
        upload_part_finished(<<"no-such-bucket">>, test_key1(), UploadId,
                             PartNumber, PartUUID, MD5, User),
    {error, notfound} =
        upload_part_finished(test_bucket1(), <<"no-such-key">>, UploadId,
                             PartNumber, PartUUID, MD5, User),
    {U1, U2, U3} = User,
    NoSuchUser = {U1 ++ "foo", U2 ++ "foo", U3 ++ "foo"},
    {error, access_denied} =
        upload_part_finished(test_bucket1(), test_key1(), UploadId,
                             PartNumber, PartUUID, MD5, NoSuchUser),
    {error, notfound} =
        upload_part_finished(test_bucket1(), test_key1(), <<"no-such-upload-id">>,
                             PartNumber, PartUUID, MD5, User),
    {error, notfound} =
         upload_part_finished(test_bucket1(), test_key1(), UploadId,
                              PartNumber, <<"no-such-part-id">>, MD5, User),
    ok = upload_part_finished(test_bucket1(), test_key1(), UploadId,
                              PartNumber, PartUUID, MD5, User),
    {error, notfound} =
         upload_part_finished(test_bucket1(), test_key1(), UploadId,
                              PartNumber, PartUUID, MD5, User),
    {ok, PartUUID, MD5}.

test_comb_parts() ->
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

    {15, Keep1, []} = comb_parts(MpM1, GoodETags),
    5 = length(Keep1),
    Keep1 = lists:usort(Keep1),

    {14, Keep2, [PM2]} = comb_parts(MpM1, tl(GoodETags)),
    4 = length(Keep2),
    Keep2 = lists:usort(Keep2),
    1 = PM2?PART_MANIFEST.part_number,

    ok.

test_create_users() ->
    %% info for test_user1()
    %% NOTE: This user has a "test" bucket in its buckets list,
    %%       therefore test_user1() is the owner of the "test" bucket.
    ok = test_put(<<"moss.users">>, <<"J2IP6WGUQ_FNGIAN9AFI">>, <<131,104,9,100,0,11,114,99,115,95,117,115,101,114,95,118,50,107,0,7,102,111,111,32,98,97,114,107,0,6,102,111,111,98,97,114,107,0,18,102,111,111,98,97,114,64,101,120,97,109,112,108,101,46,99,111,109,107,0,20,74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73,107,0,40,109,98,66,45,49,86,65,67,78,115,114,78,48,121,76,65,85,83,112,67,70,109,88,78,78,66,112,65,67,51,88,48,108,80,109,73,78,65,61,61,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,108,0,0,0,1,104,6,100,0,14,109,111,115,115,95,98,117,99,107,101,116,95,118,49,107,0,4,116,101,115,116,100,0,7,99,114,101,97,116,101,100,107,0,24,50,48,49,50,45,49,50,45,48,56,84,48,48,58,51,53,58,49,57,46,48,48,48,90,104,3,98,0,0,5,74,98,0,14,36,199,98,0,6,176,191,100,0,9,117,110,100,101,102,105,110,101,100,106,100,0,7,101,110,97,98,108,101,100>>),
    %% info for test_user2()
    ok = test_put(<<"moss.users">>, <<"LAHU4GBJIRQD55BJNET7">>, <<131,104,9,100,0,11,114,99,115,95,117,115,101,114,95,118,50,107,0,8,102,111,111,32,98,97,114,50,107,0,7,102,111,111,98,97,114,50,107,0,19,102,111,111,98,97,114,50,64,101,120,97,109,112,108,101,46,99,111,109,107,0,20,76,65,72,85,52,71,66,74,73,82,81,68,53,53,66,74,78,69,84,55,107,0,40,121,104,73,48,56,73,122,50,71,112,55,72,100,103,85,70,50,101,103,85,49,83,99,82,53,97,72,50,49,85,116,87,110,87,110,99,69,103,61,61,107,0,64,51,50,57,99,51,51,50,98,57,101,102,102,52,57,56,57,57,99,50,99,54,101,53,49,56,53,100,101,55,102,100,57,55,99,100,99,54,100,54,52,54,99,53,53,100,51,101,56,52,101,102,49,57,48,48,54,99,55,52,54,99,51,54,56,106,100,0,7,101,110,97,98,108,101,100>>),
    ok.

test_bucket1() ->
    <<"test">>.

test_key1() ->
    <<"mp0">>.

test_cleanup_data() ->
    _ = test_delete(test_hash_objects_bucket(test_bucket1()), test_key1()),
    ok.

test_cleanup_users() ->
    _ = test_delete(<<"moss.users">>, list_to_binary(element(3, test_user1()))),
    _ = test_delete(<<"moss.users">>, list_to_binary(element(3, test_user2()))),
    ok.

test_hash_objects_bucket(Bucket) ->
    riak_cs_utils:to_bucket_name(objects, Bucket).

test_delete(Bucket, Key) ->
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    Res = riakc_pb_socket:delete(RiakcPid, Bucket, Key),
    riak_cs_utils:close_riak_connection(RiakcPid),
    Res.

test_put(Bucket, Key, Value) ->
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    Res = riakc_pb_socket:put(RiakcPid, riakc_obj:new(Bucket, Key, Value)),
    riak_cs_utils:close_riak_connection(RiakcPid),
    Res.

test_user1() ->
    {"foobar", "18983ba0e16e18a2b103ca16b84fad93d12a2fbed1c88048931fb91b0b844ad3", "J2IP6WGUQ_FNGIAN9AFI"}.

test_user1_secret() ->
    "mbB-1VACNsrN0yLAUSpCFmXNNBpAC3X0lPmINA==".

test_user2() ->
    {"foobar2", "329c332b9eff49899c2c6e5185de7fd97cdc6d646c55d3e84ef19006c746c368", "LAHU4GBJIRQD55BJNET7"}.

test_user2_secret() ->
    "yhI08Iz2Gp7HdgUF2egU1ScR5aH21UtWnWncEg==".

test_userNONE() ->
    {"bar", "bar", "bar"}.

-endif.
