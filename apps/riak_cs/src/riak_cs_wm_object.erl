%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_wm_object).

-export([init/1,
         stats_prefix/0,
         authorize/2,
         content_types_provided/2,
         generate_etag/2,
         last_modified/2,
         produce_body/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         valid_entity_length/2
        ]).

-ignore_xref([init/1,
              stats_prefix/0,
              authorize/2,
              content_types_provided/2,
              generate_etag/2,
              last_modified/2,
              produce_body/2,
              allowed_methods/0,
              malformed_request/2,
              content_types_accepted/2,
              accept_body/2,
              delete_resource/2,
              valid_entity_length/2
             ]).

-include("riak_cs.hrl").
-include("riak_cs_web.hrl").
-include_lib("webmachine/include/wm_reqstate.hrl").
-include_lib("kernel/include/logger.hrl").

-spec init(#rcs_web_context{}) -> {ok, #rcs_web_context{}}.
init(Ctx) ->
    {ok, Ctx#rcs_web_context{local_context = #key_context{}}}.

-spec stats_prefix() -> object.
stats_prefix() -> object.

-spec malformed_request(#wm_reqdata{}, #rcs_web_context{}) -> {false, #wm_reqdata{}, #rcs_web_context{}}.
malformed_request(RD, #rcs_web_context{response_module=ResponseMod} = Ctx) ->
    case riak_cs_wm_utils:extract_key(RD, Ctx) of
        {error, Reason} ->
            ResponseMod:api_error(Reason, RD, Ctx);
        {ok, ContextWithKey} ->
            case riak_cs_wm_utils:has_canned_acl_and_header_grant(RD) of
                true ->
                    ResponseMod:api_error(canned_acl_and_header_grant,
                                          RD, ContextWithKey);
                false ->
                    case riak_cs_copy_object:malformed_request(RD) of
                        {true, Reason} ->
                            ResponseMod:api_error(Reason, RD, ContextWithKey);
                        false ->
                            {false, RD, ContextWithKey}
                    end
            end
    end.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
-spec authorize(#wm_reqdata{}, #rcs_web_context{}) ->
    {boolean() | {halt, term()}, #wm_reqdata{}, #rcs_web_context{}}.
authorize(RD, Ctx0 = #rcs_web_context{local_context = LocalCtx0,
                                      riak_client = RcPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RcPid),
    Ctx = Ctx0#rcs_web_context{requested_perm = RequestedAccess,
                               local_context = LocalCtx},
    authorize(RD, Ctx,
              LocalCtx#key_context.bucket_object,
              Method, LocalCtx#key_context.manifest).

authorize(RD, Ctx, notfound = _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_bucket);
authorize(RD, Ctx, _BucketObj, 'GET', notfound = _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_key);
authorize(RD, Ctx, _BucketObj, 'HEAD', notfound = _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_key);
authorize(RD, Ctx, _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:object_access_authorize_helper(object, true, RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% TODO: POST
    ['HEAD', 'GET', 'DELETE', 'PUT'].

-spec valid_entity_length(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
valid_entity_length(RD, Ctx) ->
    MaxLen = riak_cs_lfs_utils:max_content_len(),
    case riak_cs_wm_utils:valid_entity_length(MaxLen, RD, Ctx) of
        {true, NewRD, NewCtx} ->
            check_0length_metadata_update(riak_cs_wm_utils:content_length(RD),
                                          NewRD, NewCtx);
        Other ->
            Other
    end.

-spec content_types_provided(#wm_reqdata{}, #rcs_web_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_provided(RD, Ctx = #rcs_web_context{local_context = LocalCtx,
                                                  riak_client = RcPid}) ->
    Mfst = LocalCtx#key_context.manifest,
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            UpdLocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx, RcPid),
            ContentType = binary_to_list(Mfst?MANIFEST.content_type),
            case ContentType of
                _ ->
                    UpdCtx = Ctx#rcs_web_context{local_context = UpdLocalCtx},
                    {[{ContentType, produce_body}], RD, UpdCtx}
            end;
       true ->
            %% TODO this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", produce_body}], RD, Ctx}
    end.

-spec generate_etag(#wm_reqdata{}, #rcs_web_context{}) -> {string(), #wm_reqdata{}, #rcs_web_context{}}.
generate_etag(RD, Ctx = #rcs_web_context{local_context = LocalCtx}) ->
    Mfst = LocalCtx#key_context.manifest,
    ETag = riak_cs_manifest:etag_no_quotes(Mfst),
    {ETag, RD, Ctx}.

-spec last_modified(#wm_reqdata{}, #rcs_web_context{}) -> {calendar:datetime(), #wm_reqdata{}, #rcs_web_context{}}.
last_modified(RD, Ctx = #rcs_web_context{local_context
                                         = #key_context{manifest
                                                        = ?MANIFEST{write_start_time = WST}}}) ->
    ErlDate = calendar:system_time_to_local_time(WST, millisecond),
    {ErlDate, RD, Ctx}.

-spec produce_body(#wm_reqdata{}, #rcs_web_context{}) ->
          {{known_length_stream, non_neg_integer(), {<<>>, function()}}, #wm_reqdata{}, #rcs_web_context{}}.
produce_body(RD, Ctx = #rcs_web_context{local_context = LocalCtx,
                                        response_module = ResponseMod}) ->
    #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst} = LocalCtx,
    ResourceLength = Mfst?MANIFEST.content_length,
    case parse_range(RD, ResourceLength) of
        invalid_range ->
            %% HTTP/1.1 416 Requested Range Not Satisfiable
            riak_cs_get_fsm:stop(GetFsmPid),
            ResponseMod:api_error(
              invalid_range,
              %% RD#wm_reqdata{resp_range=ignore_request}, Ctx);
              RD, Ctx);
        {RangeIndexes, RespRange} ->
            produce_body(RD, Ctx, RangeIndexes, RespRange)
    end.

produce_body(RD, Ctx = #rcs_web_context{rc_pool = RcPool,
                                        riak_client = RcPid,
                                        local_context = LocalCtx,
                                        start_time = StartTime},
             {Start, End}, RespRange) ->
    #key_context{get_fsm_pid = GetFsmPid,
                 manifest = ?MANIFEST{write_start_time = Created,
                                      content_length = ResourceLength,
                                      metadata = Metadata} = Mfst} = LocalCtx,
    Method = wrq:method(RD),
    LastModified = webmachine_util:rfc1123_date(
                     calendar:system_time_to_local_time(Created, millisecond)),
    ETag = riak_cs_manifest:etag(Mfst),
    NewRQ1 = lists:foldl(fun({K, V}, Rq) -> wrq:set_resp_header(K, V, Rq) end,
                         RD,
                         [{"ETag",  ETag},
                          {"Last-Modified", LastModified}
                         ] ++ Metadata),
    NewRQ2 = wrq:set_resp_range(RespRange, NewRQ1),
    NoBody = Method =:= 'HEAD' orelse ResourceLength =:= 0,
    {NewCtx, StreamBody} =
        case NoBody of
            true ->
                riak_cs_get_fsm:stop(GetFsmPid),
                {Ctx, fun() -> {<<>>, done} end};
            false ->
                riak_cs_get_fsm:continue(GetFsmPid, {Start, End}),
                %% Streaming by `known_length_stream' and `StreamBody' function
                %% will be handled *after* WM's `finish_request' callback complets.
                %% Use `no_stats` to avoid auto stats update by `riak_cs_wm_common'.
                {Ctx#rcs_web_context{auto_rc_close = false,
                                     stats_key = no_stats},
                 {<<>>, fun() ->
                                riak_cs_wm_utils:streaming_get(
                                  RcPool, RcPid, GetFsmPid, StartTime)
                        end}}
        end,
    {{known_length_stream, ResourceLength, StreamBody}, NewRQ2, NewCtx}.

parse_range(RD, ResourceLength) ->
    case wrq:get_req_header("range", RD) of
        undefined ->
            {{0, ResourceLength - 1}, ignore_request};
        RawRange ->
            case webmachine_util:parse_range(RawRange, ResourceLength) of
                [] ->
                    invalid_range;
                [SingleRange] ->
                    {SingleRange, follow_request};
                _MultipleRanges ->
                    %% S3 responds full resource without a Content-Range header
                    {{0, ResourceLength - 1}, ignore_request}
            end
    end.

%% @doc Callback for deleting an object.
-spec delete_resource(#wm_reqdata{}, #rcs_web_context{}) -> {true, #wm_reqdata{}, #rcs_web_context{}}.
delete_resource(RD, Ctx = #rcs_web_context{local_context = LocalCtx,
                                           riak_client = RcPid}) ->
    #key_context{bucket = Bucket,
                 key = Key,
                 obj_vsn = ObjVsn,
                 get_fsm_pid = GetFsmPid} = LocalCtx,
    BFile_str = bfile_str(Bucket, Key, ObjVsn),
    UserName = riak_cs_wm_utils:extract_name(Ctx#rcs_web_context.user),
    riak_cs_get_fsm:stop(GetFsmPid),
    DeleteObjectResponse = riak_cs_utils:delete_object(Bucket, Key, ObjVsn, RcPid),
    handle_delete_object(DeleteObjectResponse, UserName, BFile_str, RD, Ctx).

%% @private
handle_delete_object({error, Error}, _UserName, _BFile_str, RD, Ctx) ->
    logger:error("delete object failed with reason: ~p", [Error]),
    {false, RD, Ctx};
handle_delete_object({ok, _UUIDsMarkedforDelete}, _UserName, _BFile_str, RD, Ctx) ->
    {true, RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #rcs_web_context{}) -> {[{string(), atom()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_accepted(RD, Ctx) ->
    content_types_accepted(wrq:get_req_header("Content-Type", RD), RD, Ctx).

-spec content_types_accepted(undefined | string(), #wm_reqdata{}, #rcs_web_context{}) ->
          {[{string(), atom()}], #wm_reqdata{}, #rcs_web_context{}}.
content_types_accepted(CT, RD, Ctx)
  when CT =:= undefined;
       CT =:= [] ->
    content_types_accepted("application/octet-stream", RD, Ctx);
content_types_accepted(CT, RD, Ctx = #rcs_web_context{local_context = LocalCtx0}) ->
    %% This was shamelessly ripped out of
    %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
    {Media, _Params} = mochiweb_util:parse_header(CT),
    case string:tokens(Media, "/") of
        [_Type, _Subtype] ->
            %% accept whatever the user says
            LocalCtx = LocalCtx0#key_context{putctype=Media},
            {[{Media, add_acl_to_context_then_accept}], RD, Ctx#rcs_web_context{local_context = LocalCtx}};
        _ ->
            %% TODO:
            %% Maybe we should have caught
            %% this in malformed_request?
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

-spec accept_body(#wm_reqdata{}, #rcs_web_context{}) ->
          {{halt, integer()}, #wm_reqdata{}, #rcs_web_context{}}.
accept_body(RD, Ctx = #rcs_web_context{riak_client=RcPid,
                                       local_context=LocalCtx,
                                       response_module=ResponseMod})
  when LocalCtx#key_context.update_metadata == true ->
    %% zero-body put copy - just updating metadata
    #key_context{bucket = Bucket, key = Key, obj_vsn = ObjVsn,
                 manifest = Mfst} = LocalCtx,
    Acl = Mfst?MANIFEST.acl,
    NewAcl = Acl?ACL{creation_time = os:system_time(millisecond)},
    {ContentType, Metadata} = riak_cs_copy_object:new_metadata(Mfst, RD),
    case riak_cs_utils:set_object_acl(Bucket, Key, ObjVsn,
                                      Mfst?MANIFEST{metadata=Metadata, content_type=ContentType}, NewAcl,
                                      RcPid) of
        ok ->
            ETag = riak_cs_manifest:etag(Mfst),
            RD2 = wrq:set_resp_header("ETag", ETag, RD),
            ResponseMod:copy_object_response(Mfst, RD2, Ctx);
        {error, Err} ->
            ResponseMod:api_error(Err, RD, Ctx)
    end;
accept_body(RD, Ctx = #rcs_web_context{response_module = ResponseMod}) ->
    case riak_cs_copy_object:get_copy_source(RD) of
        undefined ->
            handle_normal_put(RD, Ctx);
        {error, _} = Err ->
            ResponseMod:api_error(Err, RD, Ctx);
        {SrcBucket, SrcKey, SrcObjVsn} ->
            handle_copy_put(RD, Ctx#rcs_web_context{stats_key = [object, put_copy]},
                            SrcBucket, SrcKey, SrcObjVsn)
    end.

handle_normal_put(RD, Ctx0) ->
    #rcs_web_context{local_context = LocalCtx,
                     acl = ACL,
                     riak_client = RcPid} = Ctx0,
    #key_context{bucket = Bucket,
                 key = Key,
                 obj_vsn = SuppliedVsn,
                 putctype = ContentType,
                 size = Size,
                 get_fsm_pid = GetFsmPid} = LocalCtx,
    EventualVsn = determine_object_version(SuppliedVsn, Bucket, Key, RcPid),
    Ctx1 = Ctx0#rcs_web_context{local_context = LocalCtx#key_context{obj_vsn = EventualVsn}},

    riak_cs_get_fsm:stop(GetFsmPid),
    Metadata = riak_cs_wm_utils:extract_user_metadata(RD),
    BlockSize = riak_cs_lfs_utils:block_size(),

    Args = [{Bucket, Key, EventualVsn, Size, list_to_binary(ContentType),
             Metadata, BlockSize, ACL, timer:seconds(60), self(), RcPid}],
    {ok, Pid} = riak_cs_put_fsm_sup:start_put_fsm(node(), Args),
    try
        accept_streambody(RD, Ctx1, Pid,
                          wrq:stream_req_body(RD, riak_cs_lfs_utils:block_size()))
    catch
        Type:Error:ST ->
            %% Want to catch mochiweb_socket:recv() returns {error,
            %% einval} or disconnected stuff, any errors prevents this
            %% manifests from being uploaded anymore
            ?LOG_DEBUG("PUT FSM force_stop after ~p:~p. Stacktrace: ~p", [Type, Error, ST]),
            _ = riak_cs_put_fsm:force_stop(Pid),
            error({Type, Error})
    end.

determine_object_version(Vsn0, Bucket, Key, RcPid) ->
    case {Vsn0, riak_cs_bucket:get_bucket_versioning(Bucket, RcPid)} of
        {?LFS_DEFAULT_OBJECT_VERSION, {ok, #bucket_versioning{status = enabled}}} ->
            Vsn1 = list_to_binary(riak_cs_utils:binary_to_hexlist(uuid:get_v4())),
            logger:info("bucket \"~s\" has object versioning enabled,"
                        " autogenerating version ~p for key ~p", [Bucket, Vsn1, Key]),
            Vsn1;
        {_, {ok, #bucket_versioning{status = enabled}}} ->
            logger:info("bucket \"~s\" has object versioning enabled"
                        " but using ~p as supplied in request for key ~p", [Bucket, Vsn0, Key]),
            Vsn0;
        {?LFS_DEFAULT_OBJECT_VERSION, {ok, #bucket_versioning{status = suspended}}} ->
            Vsn0;
        {Vsn3, {ok, #bucket_versioning{status = suspended}}} ->
            logger:warning("ignoring object version ~p in request for key ~p in bucket \"~s\""
                           " as the bucket has object versioning suspended", [Vsn3, Key, Bucket]),
            Vsn0
    end.

%% @doc the head is PUT copy path
handle_copy_put(RD, Ctx, SrcBucket, SrcKey, SrcObjVsn) ->
    #rcs_web_context{local_context = LocalCtx,
                     response_module = ResponseMod,
                     acl = Acl,
                     riak_client = RcPid} = Ctx,
    %% manifest is always notfound|undefined here
    #key_context{bucket = Bucket, key = Key, obj_vsn = ObjVsn,
                 get_fsm_pid = GetFsmPid} = LocalCtx,

    {ok, ReadRcPid} = riak_cs_riak_client:checkout(),
    try

        %% You'll also need permission to access source object, but RD and
        %% Ctx is of target object. Then access permission to source
        %% object has to be checked here. First of all, get manifest.
        case riak_cs_manifest:fetch(ReadRcPid, SrcBucket, SrcKey, SrcObjVsn) of
            {ok, SrcManifest} ->

                EntityTooLarge = SrcManifest?MANIFEST.content_length > riak_cs_lfs_utils:max_content_len(),

                case riak_cs_copy_object:test_condition_and_permission(ReadRcPid, SrcManifest, RD, Ctx) of

                    {false, _, _} when EntityTooLarge ->
                        ResponseMod:api_error(entity_too_large, RD, Ctx);

                    {false, _, _} ->

                        %% start copying
                        ?LOG_DEBUG("copying! > ~s/~s/~s => ~s/~s/~s via ~p",
                                   [SrcBucket, SrcKey, SrcObjVsn, Bucket, Key, ObjVsn, ReadRcPid]),

                        {ContentType, Metadata} =
                            riak_cs_copy_object:new_metadata(SrcManifest, RD),
                        NewAcl = Acl?ACL{creation_time = os:system_time(millisecond)},
                        {ok, PutFsmPid} = riak_cs_copy_object:start_put_fsm(
                                            Bucket, Key, ObjVsn, SrcManifest?MANIFEST.content_length,
                                            ContentType, Metadata, NewAcl, RcPid),

                        %% Prepare for connection loss or client close
                        FDWatcher = riak_cs_copy_object:connection_checker((RD#wm_reqdata.wm_state)#wm_reqstate.socket),

                        %% This ain't fail because all permission and 404
                        %% possibility has been already checked.
                        {ok, DstManifest} = riak_cs_copy_object:copy(PutFsmPid, SrcManifest, ReadRcPid, FDWatcher),
                        ETag = riak_cs_manifest:etag(DstManifest),
                        RD2 = wrq:set_resp_header("ETag", ETag, RD),
                        ResponseMod:copy_object_response(DstManifest, RD2,
                                                         Ctx#rcs_web_context{local_context = LocalCtx});
                    {true, _RD, _OtherCtx} ->
                        %% access to source object not authorized
                        %% TODO: check the return value / http status
                        ResponseMod:api_error(copy_source_access_denied, RD, Ctx);
                    {{halt, 403}, _RD, _OtherCtx} ->
                        %% access to source object not authorized either, but
                        %% in different return value
                        ResponseMod:api_error(copy_source_access_denied, RD, Ctx);
                    {Result, _, _} = Error ->
                        ?LOG_DEBUG("~p on ~s ~s", [Result, SrcBucket, SrcKey]),
                        Error

                end;
            {error, notfound} ->
                ResponseMod:api_error(no_copy_source_key, RD, Ctx);
            {error, no_active_manifest} ->
                ResponseMod:api_error(no_copy_source_key, RD, Ctx);
            {error, Err} ->
                ResponseMod:api_error(Err, RD, Ctx)
        end
    after
        riak_cs_get_fsm:stop(GetFsmPid),
        riak_cs_riak_client:checkin(ReadRcPid)
    end.

-spec accept_streambody(#wm_reqdata{}, #rcs_web_context{}, pid(), term()) -> {{halt, integer()}, #wm_reqdata{}, #rcs_web_context{}}.
accept_streambody(RD,
                  Ctx = #rcs_web_context{local_context = #key_context{size = 0}},
                  Pid,
                  {_Data, _Next}) ->
    finalize_request(RD, Ctx, Pid);
accept_streambody(RD, Ctx, Pid, {Data, Next}) ->
    riak_cs_put_fsm:augment_data(Pid, Data),
    if is_function(Next) ->
            accept_streambody(RD, Ctx, Pid, Next());
       Next =:= done ->
            finalize_request(RD, Ctx, Pid)
    end.

%% TODO:
%% We need to do some checking to make sure the bucket exists
%% for the user who is doing this PUT
-spec finalize_request(#wm_reqdata{}, #rcs_web_context{}, pid()) -> {{halt, 200}, #wm_reqdata{}, #rcs_web_context{}}.
finalize_request(RD,
                 Ctx = #rcs_web_context{local_context = LocalCtx,
                                        response_module = ResponseMod},
                 Pid) ->
    #key_context{obj_vsn = Vsn,
                 size = S} = LocalCtx,
    ContentMD5 = case wrq:get_req_header("content-md5", RD) of
                     undefined -> undefined;
                     A -> list_to_binary(A)
                 end,
    Response =
        case riak_cs_put_fsm:finalize(Pid, ContentMD5) of
            {ok, Manifest} ->
                ETag = riak_cs_manifest:etag(Manifest),
                %% TODO: probably want something that counts actual bytes uploaded
                %% instead, to record partial/aborted uploads
                AccessRD = riak_cs_access_log_handler:set_bytes_in(S, RD),
                {{halt, 200}, wrq:set_resp_headers([{"ETag", ETag},
                                                    {"x-amz-version-id", Vsn}], AccessRD), Ctx};
            {error, invalid_digest} ->
                ResponseMod:invalid_digest_response(ContentMD5, RD, Ctx);
            {error, Reason} ->
                ResponseMod:api_error(Reason, RD, Ctx)
        end,
    Response.

check_0length_metadata_update(Length, RD, Ctx = #rcs_web_context{local_context = LocalCtx}) ->
    %% The authorize() callback has already been called, which means
    %% that ensure_doc() has been called, so the local context
    %% manifest is up-to-date: the object exists or it doesn't.
    case (not is_atom(LocalCtx#key_context.manifest) andalso
          zero_length_metadata_update_p(Length, RD)) of
        false ->
            UpdLocalCtx = LocalCtx#key_context{size = Length},
            {true, RD, Ctx#rcs_web_context{local_context = UpdLocalCtx}};
        true ->
            UpdLocalCtx = LocalCtx#key_context{size = Length,
                                               update_metadata = true},
            {true, RD, Ctx#rcs_web_context{stats_key = [object, put_copy],
                                           local_context = UpdLocalCtx}}
    end.

zero_length_metadata_update_p(0, RD) ->
    OrigPath = wrq:get_req_header("x-rcs-rewrite-path", RD),
    case wrq:get_req_header("x-amz-copy-source", RD) of
        undefined ->
            false;
        [$/ | _] = Path ->
            Path == OrigPath;
        Path ->
            %% boto (version 2.7.0) does NOT prepend "/"
            [$/ | Path] == OrigPath
    end;
zero_length_metadata_update_p(_, _) ->
    false.

bfile_str(B, K, ?LFS_DEFAULT_OBJECT_VERSION) ->
    [B, $,, K];
bfile_str(B, K, V) ->
    [B, $,, K, $,, V].
