%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_wm_object_upload_part).

-export([init/1,
         stats_prefix/0,
         authorize/2,
         content_types_provided/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         post_is_create/2,
         process_post/2,
         valid_entity_length/2,
         delete_resource/2,
         accept_body/2,
         to_xml/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("webmachine/include/wm_reqstate.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    %% {ok, Ctx#context{local_context=#key_context{}}}.
    {ok, Ctx#context{local_context=#key_context{}}}.

-spec stats_prefix() -> multipart_upload.
stats_prefix() -> multipart_upload.

-spec malformed_request(#wm_reqdata{}, #context{}) ->
                               {false, #wm_reqdata{}, #context{}} | {{halt, pos_integer()}, #wm_reqdata{}, #context{}}.
malformed_request(RD,  #context{response_module=ResponseMod} = Ctx) ->
    Method = wrq:method(RD),
    case Method == 'PUT' andalso not valid_part_number(RD) of
        %% For multipart upload part,
        true ->
            ResponseMod:api_error(invalid_part_number, RD, Ctx);
        false ->
            case riak_cs_wm_utils:extract_key(RD, Ctx) of
                {error, Reason} ->
                    ResponseMod:api_error(Reason, RD, Ctx);
                {ok, ContextWithKey} ->
                    {false, RD, ContextWithKey}
            end
    end.

valid_part_number(RD)  ->
    case riak_cs_utils:safe_list_to_integer(wrq:get_qs_value("partNumber", RD)) of
        {ok, PartNumber} ->
            1 =< PartNumber andalso PartNumber =< ?DEFAULT_MAX_PART_NUMBER;
        _ ->
            false
    end.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
-spec authorize(#wm_reqdata{}, #context{}) ->
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx0=#context{local_context=LocalCtx0, riak_client=RcPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RcPid),
    Ctx = Ctx0#context{requested_perm=RequestedAccess, local_context=LocalCtx},
    authorize(RD, Ctx,
              LocalCtx#key_context.bucket_object,
              Method, LocalCtx#key_context.manifest).

authorize(RD, Ctx, notfound = _BucketObj, _Method, _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_bucket);
authorize(RD, Ctx, _BucketObj, 'HEAD', notfound = _Manifest) ->
    riak_cs_wm_utils:respond_api_error(RD, Ctx, no_such_key);
authorize(RD, Ctx, _BucketObj, 'GET', notfound = _Manifest) ->
    %% List Parts
    %% Object ownership will be checked by riak_cs_mp_utils:do_part_common(),
    %% so we give blanket permission here - Never check ACL but Policy.
    riak_cs_wm_utils:object_access_authorize_helper(object_part, true, true, RD, Ctx);
authorize(RD, Ctx, _BucketObj, _Method, _Manifest) ->
    %% Initiate/Complete/Abort multipart
    riak_cs_wm_utils:object_access_authorize_helper(object_part, true, false, RD, Ctx).

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'POST', 'PUT', 'DELETE'].

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

process_post(RD, Ctx=#context{local_context=LocalCtx, riak_client=RcPid}) ->
    #key_context{bucket=Bucket, key=Key} = LocalCtx,
    User = riak_cs_user:to_3tuple(Ctx#context.user),
    UploadId64 = re:replace(wrq:path(RD), ".*/uploads/", "", [{return, binary}]),
    Body = binary_to_list(wrq:req_body(RD)),
    case {parse_body(Body), catch base64url:decode(UploadId64)} of
        {bad, _} ->
            {{halt,477}, RD, Ctx};
        %% RCS-156 (gh #1100) return a 400 Bad Request, Malformed XML error
        %% when there is a multipart upload without any parts in the upload
        %% complete message
        {[], _UploadId} ->
            riak_cs_s3_response:api_error(malformed_xml, RD, Ctx);
        {PartETags, UploadId} ->
            case riak_cs_mp_utils:complete_multipart_upload(
                   Bucket, list_to_binary(Key), UploadId, PartETags, User,
                   RcPid) of
                {ok, NewManifest} ->
                    ETag = riak_cs_manifest:etag(NewManifest),
                    _ = lager:debug("checksum of all parts checksum: ~p", [ETag]),
                    XmlDoc = {'CompleteMultipartUploadResult',
                              [{'xmlns', "http://s3.amazonaws.com/doc/2006-03-01/"}],
                              [
                               {'Location', [response_location(Bucket, Key)]},
                               {'Bucket', [Bucket]},
                               {'Key', [Key]},
                               {'ETag', [ETag]}
                              ]
                             },
                    XmlBody = riak_cs_xml:to_xml([XmlDoc]),
                    RD2 = wrq:set_resp_body(XmlBody, RD),
                    {true, RD2, Ctx};
                {error, notfound} ->
                    riak_cs_s3_response:no_such_upload_response(UploadId, RD, Ctx);
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end
    end.

response_location(Bucket, Key) ->
    lists:append(["http://",
        binary:bin_to_list(Bucket), ".", riak_cs_config:root_host(), "/", Key]).
    
-spec valid_entity_length(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
valid_entity_length(RD, Ctx) ->
    MaxLen = riak_cs_lfs_utils:max_content_len(),
    riak_cs_wm_utils:valid_entity_length(MaxLen, RD, Ctx).

-spec delete_resource(#wm_reqdata{}, #context{}) ->
                             {boolean() | {'halt', term()}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{local_context=LocalCtx,
                                 riak_client=RcPid}) ->
    ReqUploadId = wrq:path_info('uploadId', RD),
    case (catch base64url:decode(ReqUploadId)) of
        {'EXIT', _Reason} ->
            riak_cs_s3_response:no_such_upload_response({raw, ReqUploadId}, RD, Ctx);
        UploadId ->
            #key_context{bucket=Bucket, key=KeyStr} = LocalCtx,
            Key = list_to_binary(KeyStr),
            User = riak_cs_user:to_3tuple(Ctx#context.user),
            case riak_cs_mp_utils:abort_multipart_upload(Bucket, Key, UploadId,
                                                         User, RcPid) of
                ok ->
                    {true, RD, Ctx};
                {error, notfound} ->
                    riak_cs_s3_response:no_such_upload_response(UploadId, RD, Ctx);
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end
    end.

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD, Ctx=#context{}) ->
    Method = wrq:method(RD),
    if Method == 'GET' ->
            {[{?XML_TYPE, to_xml}], RD, Ctx};
       Method == 'POST' ->
            {[{?XML_TYPE, unused_callback1}], RD, Ctx};
       Method == 'PUT' ->
            {[{?XML_TYPE, doesnt_matter_see_content_types_accepted}], RD, Ctx};
       true ->
            %% this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", unused_callback2}], RD, Ctx}
    end.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    %% For multipart upload part,
    %% e.g., PUT /ObjectName?partNumber=PartNumber&uploadId=UploadId
    riak_cs_mp_utils:make_content_types_accepted(RD, Ctx, accept_body).

parse_body(Body0) ->
    try
        Body = re:replace(Body0, "&quot;", "", [global, {return, list}]),
        {ok, ParsedData} = riak_cs_xml:scan(Body),
        #xmlElement{name='CompleteMultipartUpload'} = ParsedData,
        Nums = [list_to_integer(T#xmlText.value) ||
                   T <- xmerl_xpath:string("//CompleteMultipartUpload/Part/PartNumber/text()", ParsedData)],
        ETags = [riak_cs_utils:hexlist_to_binary(string:strip(T#xmlText.value, both, $")) ||
                   T <- xmerl_xpath:string("//CompleteMultipartUpload/Part/ETag/text()", ParsedData)],
        lists:zip(Nums, ETags)
    catch _:_ ->
            bad
    end.

-spec accept_body(#wm_reqdata{}, #context{}) ->
                         {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, #context{local_context=LocalCtx0} = Ctx0) ->
    catch riak_cs_get_fsm:stop(LocalCtx0#key_context.get_fsm_pid),

    try
        {t, {ok, UploadId}} =
            {t, riak_cs_utils:safe_base64url_decode(
                  re:replace(wrq:path(RD), ".*/uploads/", "", [{return, binary}]))},
        {t, {ok, PartNumber}} =
            {t, riak_cs_utils:safe_list_to_integer(wrq:get_qs_value("partNumber", RD))},
        LocalCtx = LocalCtx0#key_context{upload_id=UploadId,
                                         part_number=PartNumber},
        Ctx = Ctx0#context{local_context=LocalCtx},
        validate_copy_header(RD, Ctx)
    catch
        error:{badmatch, {t, _}} ->
            {{halt, 400}, RD, Ctx0}
    end.

validate_copy_header(RD, #context{response_module=ResponseMod,
                                  local_context=LocalCtx} = Ctx) ->
    case riak_cs_copy_object:get_copy_source(RD) of
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx);
        undefined ->
            validate_part_size(RD, Ctx, LocalCtx#key_context.size,
                               undefined, undefined);
        {SrcBucket, SrcKey} ->
            {ok, ReadRcPid} = riak_cs_riak_client:checkout(),
            try
                case riak_cs_manifest:fetch(ReadRcPid, SrcBucket, SrcKey) of
                    {error, notfound} ->
                        ResponseMod:api_error(no_copy_source_key, RD, Ctx);
                    {ok, SrcManifest} ->
                        {Start,End} = riak_cs_copy_object:copy_range(RD, SrcManifest),
                        validate_part_size(RD,
                                           Ctx#context{stats_key=[multipart_upload, put_copy]},
                                           End - Start + 1,
                                           SrcManifest, ReadRcPid)
                end
            after
                riak_cs_riak_client:checkin(ReadRcPid)
            end
    end.

validate_part_size(RD, #context{response_module=ResponseMod} = Ctx,
                   ExactSize, SrcManifest, ReadRcPid) ->
    case ExactSize =< riak_cs_lfs_utils:max_content_len() of
        false ->
            ResponseMod:api_error(entity_too_large, RD, Ctx);
        true ->
            prepare_part_upload(RD, Ctx, ExactSize, SrcManifest, ReadRcPid)
    end.

prepare_part_upload(RD, #context{riak_client=RcPid,
                                 local_context=LocalCtx0} = Ctx0,
             ExactSize, SrcManifest, ReadRcPid) ->
    #key_context{bucket=DstBucket, key=Key,
                upload_id=UploadId, part_number=PartNumber} = LocalCtx0,
    Caller = riak_cs_user:to_3tuple(Ctx0#context.user),
    case riak_cs_mp_utils:upload_part(DstBucket, Key, UploadId, PartNumber,
                                      ExactSize, Caller, RcPid) of
        {error, notfound} ->
            riak_cs_s3_response:no_such_upload_response(UploadId, RD, Ctx0);
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx0);
        {upload_part_ready, PartUUID, PutPid} ->
            LocalCtx = LocalCtx0#key_context{part_uuid=PartUUID},
            Ctx = Ctx0#context{local_context=LocalCtx},
            case SrcManifest of
                undefined ->
                    BlockSize = riak_cs_lfs_utils:block_size(),
                    %% No badmatch errow by machiweb_socket:recv()
                    %% will be catched here because writing state
                    %% parts can be collected in Multipart Abort API.
                    accept_streambody(RD, Ctx, PutPid,
                                      wrq:stream_req_body(RD, BlockSize));
                _ ->
                    maybe_copy_part(PutPid, SrcManifest, ReadRcPid, RD, Ctx)
            end
    end.

-spec accept_streambody(#wm_reqdata{}, #context{}, pid(), term()) -> {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_streambody(RD,
                  Ctx=#context{local_context=_LocalCtx=#key_context{size=0}},
                  Pid,
                  {_Data, _Next}) ->
    finalize_request(RD, Ctx, Pid);
accept_streambody(RD,
                  Ctx=#context{local_context=LocalCtx,
                                       user=User},
                  Pid,
                  {Data, Next}) ->
    #key_context{bucket=Bucket,
                 key=Key} = LocalCtx,
    BFile_str = [Bucket, $,, Key],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"accept_streambody">>, [size(Data)], [UserName, BFile_str]),
    riak_cs_put_fsm:augment_data(Pid, Data),
    if is_function(Next) ->
            accept_streambody(RD, Ctx, Pid, Next());
       Next =:= done ->
            finalize_request(RD, Ctx, Pid)
    end.

to_xml(RD, Ctx=#context{local_context=LocalCtx,
                        riak_client=RcPid}) ->
    #key_context{bucket=Bucket, key=Key} = LocalCtx,
    UploadId = base64url:decode(re:replace(wrq:path(RD), ".*/uploads/",
                                           "", [{return, binary}])),
    {UserDisplay, _Canon, UserKeyId} = User =
        riak_cs_user:to_3tuple(Ctx#context.user),
    case riak_cs_mp_utils:list_parts(Bucket, Key, UploadId, User, [], RcPid) of
        {ok, Ps} ->
            Us = [{'Part',
                   [
                    {'PartNumber', [P?PART_DESCR.part_number]},
                    {'LastModified', [P?PART_DESCR.last_modified]},
                    {'ETag', [riak_cs_utils:etag_from_binary(P?PART_DESCR.etag)]},
                    {'Size', [P?PART_DESCR.size]}
                   ]
                  } || P <- lists:sort(Ps)],
            PartNumbers = [P?PART_DESCR.part_number || P <- Ps],
            MaxPartNumber = case PartNumbers of
                                [] -> 1;
                                _  -> lists:max(PartNumbers)
                            end,
            XmlDoc = {'ListPartsResult',
                      [{'xmlns', "http://s3.amazonaws.com/doc/2006-03-01/"}],
                      [
                       {'Bucket', [Bucket]},
                       {'Key', [Key]},
                       {'UploadId', [base64url:encode(UploadId)]},
                       {'Initiator',    % TODO: replace with ARN data?
                        [{'ID', [UserKeyId]},
                         {'DisplayName', [UserDisplay]}
                        ]},
                       {'Owner',
                        [{'ID', [UserKeyId]},
                         {'DisplayName', [UserDisplay]}
                        ]},
                       {'StorageClass', ["STANDARD"]}, % TODO
                       {'PartNumberMarker', [1]},    % TODO
                       {'NextPartNumberMarker', [MaxPartNumber]}, % TODO
                       {'MaxParts', [length(Us)]}, % TODO
                       {'IsTruncated', ["false"]}   % TODO
                      ] ++ Us
                     },
            Body = riak_cs_xml:to_xml([XmlDoc]),
            {Body, RD, Ctx};
        {error, notfound} ->
            riak_cs_s3_response:no_such_upload_response(UploadId, RD, Ctx);
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.


finalize_request(RD, Ctx=#context{local_context=LocalCtx,
                                  response_module=ResponseMod,
                                  riak_client=RcPid}, PutPid) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 upload_id=UploadId,
                 part_number=PartNumber,
                 part_uuid=PartUUID} = LocalCtx,
    Caller = riak_cs_user:to_3tuple(Ctx#context.user),
    ContentMD5 = wrq:get_req_header("content-md5", RD),
    case riak_cs_put_fsm:finalize(PutPid, ContentMD5) of
        {ok, M} ->
            case riak_cs_mp_utils:upload_part_finished(
                   Bucket, Key, UploadId, PartNumber, PartUUID,
                   M?MANIFEST.content_md5, Caller, RcPid) of
                ok ->
                    ETag = riak_cs_manifest:etag(M),
                    RD2 = wrq:set_resp_header("ETag", ETag, RD),
                    {{halt, 200}, RD2, Ctx};
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end;
        {error, invalid_digest} ->
            ResponseMod:invalid_digest_response(ContentMD5, RD, Ctx);
        {error, Reason1} ->
            riak_cs_s3_response:api_error(Reason1, RD, Ctx)
    end.

-spec maybe_copy_part(pid(), lfs_manifest(), riak_client(),
                      #wm_reqdata{}, #context{}) ->
                             {{halt, integer()}, #wm_reqdata{}, #context{}}.
maybe_copy_part(PutPid,
                ?MANIFEST{bkey={SrcBucket, SrcKey}} = SrcManifest,
                ReadRcPid,
                RD, #context{riak_client=RcPid,
                             local_context=LocalCtx,
                             user=User} = Ctx) ->
    #key_context{bucket=DstBucket, key=Key,
                 upload_id=UploadId,
                 part_number=PartNumber,
                 part_uuid=PartUUID} = LocalCtx,
                    DstKey = list_to_binary(Key),
    Caller = riak_cs_user:to_3tuple(User),

    case riak_cs_copy_object:test_condition_and_permission(ReadRcPid, SrcManifest, RD, Ctx) of
        {false, _, _} ->
            _ = lager:debug("Start copying! > ~s ~s => ~s ~s via ~p",
                            [SrcBucket, SrcKey, DstBucket, DstKey, ReadRcPid]),

            %% Prepare for connection loss or client close
            FDWatcher = riak_cs_copy_object:connection_checker((RD#wm_reqdata.wm_state)#wm_reqstate.socket),

            Range = riak_cs_copy_object:copy_range(RD, SrcManifest),
            %% This ain't fail because all permission and 404
            %% possibility has been already checked.
            case riak_cs_copy_object:copy(PutPid, SrcManifest, ReadRcPid, FDWatcher, Range) of
                {ok, DstManifest} ->
                    case riak_cs_mp_utils:upload_part_finished(
                           DstBucket, DstKey, UploadId, PartNumber, PartUUID,
                           DstManifest?MANIFEST.content_md5, Caller, RcPid) of
                        ok ->
                            ETag = riak_cs_manifest:etag(DstManifest),
                            RD2 = wrq:set_resp_header("ETag", ETag, RD),
                            riak_cs_s3_response:copy_part_response(DstManifest, RD2, Ctx);

                        {error, Reason0} ->
                            riak_cs_s3_response:api_error(Reason0, RD, Ctx)
                    end;
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end;

        {true, _RD, _OtherCtx} ->
            %% access to source object not authorized
            %% TODO: check the return value
            _ = lager:debug("access to source object denied (~s, ~s)", [SrcBucket, SrcKey]),
            {{halt, 403}, RD, Ctx};
        Error ->
            _ = lager:debug("unknown error: ~p", [Error]),
            %% ResponseMod:api_error(Error, RD, Ctx#context{local_context=LocalCtx})
            Error
    end.
