%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_object_upload_part).

-export([init/1,
         authorize/2,
         content_types_provided/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         post_is_create/2,
         process_post/2,
         multiple_choices/2,
         valid_entity_length/2,
         delete_resource/2,
         accept_body/2,
         to_xml/2,
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    %% {ok, Ctx#context{local_context=#key_context{}}}.
    {ok, Ctx#context{local_context=#key_context{}}}.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {false, #wm_reqdata{}, #context{}}.
malformed_request(RD,Ctx=#context{local_context=LocalCtx0}) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    %% need to unquote twice since we re-urlencode the string during rewrite in
    %% order to trick webmachine dispatching
    %% NOTE: Bucket::binary(), *but* Key::string()
    Key = mochiweb_util:unquote(mochiweb_util:unquote(wrq:path_info(object, RD))),
    LocalCtx = LocalCtx0#key_context{bucket=Bucket, key=Key},
    {false, RD, Ctx#context{local_context=LocalCtx}}.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
-spec authorize(#wm_reqdata{}, #context{}) ->
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx0=#context{local_context=LocalCtx0, riakc_pid=RiakPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RiakPid),
    Ctx = Ctx0#context{requested_perm=RequestedAccess,local_context=LocalCtx},
    check_permission(Method, RD, Ctx, LocalCtx#key_context.manifest).

%% @doc Final step of {@link forbidden/2}: Authentication succeeded,
%% now perform ACL check to verify access permission.
-spec check_permission(atom(), #wm_reqdata{}, #context{}, lfs_manifest() | notfound) ->
                              {boolean() | {halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
check_permission(_, RD, Ctx=#context{requested_perm=RequestedAccess,
                                     local_context=LocalCtx}, Mfst) ->
    #key_context{bucket=Bucket} = LocalCtx,
    RiakPid = Ctx#context.riakc_pid,
    case Ctx#context.user of
        undefined ->
            User = CanonicalId = undefined;
        User ->
            CanonicalId = User?RCS_USER.canonical_id
    end,
    case Mfst of
        notfound ->
            case wrq:method(RD) of
                'GET' ->
                    %% Object ownership will be checked by
                    %% riak_cs_mp_utils:do_part_common(), so we give blanket
                    %% permission here.
                    ObjectAcl = skip;
                _ ->
                    ObjectAcl = undefined
                end;
        _ ->
            ObjectAcl = Mfst?MANIFEST.acl
    end,
    case if ObjectAcl == skip -> true;
            true              -> riak_cs_acl:object_access(Bucket,
                                                           ObjectAcl,
                                                           RequestedAccess,
                                                           CanonicalId,
                                                           RiakPid)
         end of
        true ->
            %% actor is the owner
            AccessRD = riak_cs_access_log_handler:set_user(User, RD),
            UserStr = User?RCS_USER.canonical_id,
            UpdLocalCtx = LocalCtx#key_context{owner=UserStr},
            {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}};
        {true, OwnerId} ->
            %% bill the owner, not the actor
            AccessRD = riak_cs_access_log_handler:set_user(OwnerId, RD),
            UpdLocalCtx = LocalCtx#key_context{owner=OwnerId},
            {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}};
        false ->
            %% ACL check failed, deny access
            riak_cs_wm_utils:deny_access(RD, Ctx)
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'POST', 'PUT', 'DELETE'].

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

process_post(RD, Ctx=#context{local_context=LocalCtx,
                              riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket, key=Key} = LocalCtx,
    User = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
    UploadId64 = re:replace(wrq:path(RD), ".*/uploads/", "", [{return, binary}]),
    Body = binary_to_list(wrq:req_body(RD)),
    case {parse_body(Body), catch base64url:decode(UploadId64)} of
        {bad, _} ->
            {{halt,477}, RD, Ctx};
        {PartETags, UploadId} ->
            case riak_cs_mp_utils:complete_multipart_upload(
                   Bucket, list_to_binary(Key), UploadId, PartETags, User, RiakcPid) of
                ok ->
                    XmlDoc = {'CompleteMultipartUploadResult',
                              [{'xmlns', "http://s3.amazonaws.com/doc/2006-03-01/"}],
                              [
                               {'Location', [lists:append(["http://", binary_to_list(Bucket), ".s3.amazonaws.com/", Key])]},
                               {'Bucket', [binary_to_list(Bucket)]},
                               {'Key', [Key]},
                               {'ETag', [binary_to_list(UploadId64)]}
                              ]
                             },
                    XmlBody = riak_cs_s3_response:export_xml([XmlDoc]),
                    RD2 = wrq:set_resp_body(XmlBody, RD),
                    {true, RD2, Ctx};
                {error, notfound} ->
                    XErr = riak_cs_mp_utils:make_special_error("NoSuchUpload"),
                    RD2 = wrq:set_resp_body(XErr, RD),
                    {{halt, 404}, RD2, Ctx};
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end
    end.

multiple_choices(RD, Ctx) ->
    {false, RD, Ctx}.

-spec valid_entity_length(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
valid_entity_length(RD, Ctx=#context{local_context=LocalCtx}) ->
    case wrq:method(RD) of
        'PUT' ->
            case catch(
                   list_to_integer(
                     wrq:get_req_header("Content-Length", RD))) of
                Length when is_integer(Length) ->
                    case Length =< riak_cs_lfs_utils:max_content_len() of
                        false ->
                            riak_cs_s3_response:api_error(
                              entity_too_large, RD, Ctx);
                        true ->
                            UpdLocalCtx = LocalCtx#key_context{size=Length},
                            {true, RD, Ctx#context{local_context=UpdLocalCtx}}
                    end;
                _ ->
                    {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.

-spec delete_resource(#wm_reqdata{}, #context{}) ->
                             {boolean() | {'halt', term()}, #wm_reqdata{}, #context{}}.
delete_resource(RD, Ctx=#context{local_context=LocalCtx,
                                 riakc_pid=RiakcPid}) ->
    case (catch base64url:decode(wrq:path_info('uploadId', RD))) of
        {'EXIT', _Reason} ->
            {{halt, 404}, RD, Ctx};
        UploadId ->
            #key_context{bucket=Bucket, key=KeyStr} = LocalCtx,
            Key = list_to_binary(KeyStr),
            User = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
            case riak_cs_mp_utils:abort_multipart_upload(Bucket, Key, UploadId,
                                                         User, RiakcPid) of
                ok ->
                    {true, RD, Ctx};
                {error, notfound} ->
                    {{halt, 404}, RD, Ctx};
                {error, Reason} ->
                    riak_cs_s3_response:api_error(Reason, RD, Ctx)
            end
    end.

finish_request(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx}.

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

parse_body(Body) ->
    try
        {ParsedData, _Rest} = xmerl_scan:string(Body, []),
        #xmlElement{name='CompleteMultipartUpload'} = ParsedData,
        Nums = [list_to_integer(T#xmlText.value) ||
                   T <- xmerl_xpath:string("//CompleteMultipartUpload/Part/PartNumber/text()", ParsedData)],
        ETags = [riak_cs_utils:hexlist_to_binary(string:strip(T#xmlText.value, both, $")) ||
                   T <- xmerl_xpath:string("//CompleteMultipartUpload/Part/ETag/text()", ParsedData)],
        lists:zip(Nums, ETags)
    catch _:_ ->
            bad
    end.

-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx0=#context{local_context=LocalCtx0,
                              riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 size=Size,
                 get_fsm_pid=GetFsmPid} = LocalCtx0,
    catch riak_cs_get_fsm:stop(GetFsmPid),
    BlockSize = riak_cs_lfs_utils:block_size(),
    Caller = riak_cs_mp_utils:user_rec_to_3tuple(Ctx0#context.user),
    try
        {t, {ok, UploadId}} =
            {t, safe_base64url_decode(re:replace(wrq:path(RD), ".*/uploads/", "", [{return, binary}]))},
        {t, {ok, PartNumber}} =
            {t, safe_list_to_integer(wrq:get_qs_value("partNumber", RD))},
        {t3, {ok, ContentMD5}} = case wrq:get_req_header("Content-MD5", RD) of
                                     undefined -> {t3, {ok, undefined}};
                                     MD5Enc    -> {t3, safe_base64_decode(MD5Enc)}
                                 end,
        case riak_cs_mp_utils:upload_part(Bucket, Key, UploadId, PartNumber,
                                          Size, Caller, RiakcPid) of
            {upload_part_ready, PartUUID, PutPid} ->
                LocalCtx = LocalCtx0#key_context{upload_id=UploadId,
                                                 part_number=PartNumber,
                                                 part_uuid=PartUUID,
                                                 content_md5=ContentMD5},
                Ctx = Ctx0#context{local_context=LocalCtx},
                accept_streambody(RD, Ctx, PutPid,
                                  wrq:stream_req_body(RD, BlockSize));
            {error, notfound} ->
                XErr = riak_cs_mp_utils:make_special_error("NoSuchUpload"),
                RD2 = wrq:set_resp_body(XErr, RD),
                {{halt, 404}, RD2, Ctx0};
            {error, Reason} ->
                riak_cs_s3_response:api_error(Reason, RD, Ctx0)
        end
    catch
        error:{badmatch, {t, _}} ->
            {{halt, 400}, RD, Ctx0};
        error:{badmatch, {t3, _}} ->
            XErrT3 = riak_cs_mp_utils:make_special_error("InvalidDigest"),
            RDT3 = wrq:set_resp_body(XErrT3, RD),
            {{halt, 400}, RDT3, Ctx0}
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
                        riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket, key=Key} = LocalCtx,
    UploadId = base64url:decode(re:replace(wrq:path(RD), ".*/uploads/",
                                           "", [{return, binary}])),
    {UserDisplay, _Canon, UserKeyId} = User =
        riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
    case riak_cs_mp_utils:list_parts(Bucket, Key, UploadId, User, [], RiakcPid) of
        {ok, Ps} ->
            Us = [{'Part',
                   [
                    {'PartNumber', [integer_to_list(P?PART_DESCR.part_number)]},
                    {'LastModified', [P?PART_DESCR.last_modified]},
                    {'ETag', [riak_cs_utils:etag_from_binary(P?PART_DESCR.etag)]},
                    {'Size', [integer_to_list(P?PART_DESCR.size)]}
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
                       {'Bucket', [binary_to_list(Bucket)]},
                       {'Key', [Key]},
                       {'UploadId', [binary_to_list(base64url:encode(UploadId))]},
                       {'Initiator',    % TODO: replace with ARN data?
                        [{'ID', [UserKeyId]},
                         {'DisplayName', [UserDisplay]}
                        ]},
                       {'Owner',
                        [{'ID', [UserKeyId]},
                         {'DisplayName', [UserDisplay]}
                        ]},
                       {'StorageClass', ["STANDARD"]}, % TODO
                       {'PartNumberMarker', ["1"]},    % TODO
                       {'NextPartNumberMarker', [integer_to_list(MaxPartNumber)]}, % TODO
                       {'MaxParts', [integer_to_list(length(Us))]}, % TODO
                       {'IsTruncated', ["false"]}   % TODO
                      ] ++ Us
                     },
            Body = riak_cs_s3_response:export_xml([XmlDoc]),
            {Body, RD, Ctx};
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.


finalize_request(RD, Ctx=#context{local_context=LocalCtx,
                                  riakc_pid=RiakcPid}, PutPid) ->
    {ok, M} = riak_cs_put_fsm:finalize(PutPid),
    #key_context{bucket=Bucket,
                 key=Key,
                 upload_id=UploadId,
                 part_number=PartNumber,
                 part_uuid=PartUUID,
                 content_md5=ContentMD5} = LocalCtx,
    Caller = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
    case riak_cs_mp_utils:upload_part_finished(
           Bucket, Key, UploadId, PartNumber, PartUUID,
           M?MANIFEST.content_md5, Caller, RiakcPid) of
        ok ->
            if ContentMD5 == undefined orelse
               ContentMD5 == M?MANIFEST.content_md5 ->
                    RD2 = wrq:set_resp_header("ETag", riak_cs_utils:etag_from_binary(M?MANIFEST.content_md5), RD),
                    {{halt, 200}, RD2, Ctx};
               true ->
                    XErr = riak_cs_mp_utils:make_special_error("BadDigest"),
                    RD2 = wrq:set_resp_body(XErr, RD),
                    {{halt, 400}, RD2, Ctx}
            end;
        {error, Reason} ->
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end.

safe_base64_decode(Str) ->
    try
        X = base64:decode(Str),
        {ok, X}
    catch _:_ ->
            bad
    end.

safe_base64url_decode(Str) ->
    try
        X = base64url:decode(Str),
        {ok, X}
    catch _:_ ->
            bad
    end.
safe_list_to_integer(Str) ->
    try
        X = list_to_integer(Str),
        {ok, X}
    catch _:_ ->
            bad
    end.
