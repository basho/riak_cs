%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_objects_upload_complete).

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
         finish_request/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {{trace, "/tmp/wm"}, Ctx#context{local_context=#key_context{}}}.
    %% {ok, Ctx#context{local_context=#key_context{}}}.

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
check_permission(_, RD, Ctx=#context{requested_perm=RequestedAccess,local_context=LocalCtx}, Mfst) ->
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
            ObjectAcl = undefined;
        _ ->
            ObjectAcl = Mfst?MANIFEST.acl
    end,
    case riak_cs_acl:object_access(Bucket,
                                   ObjectAcl,
                                   RequestedAccess,
                                   CanonicalId,
                                   RiakPid) of
        true ->
            %% actor is the owner
            AccessRD = riak_cs_access_logger:set_user(User, RD),
            UserStr = User?RCS_USER.canonical_id,
            UpdLocalCtx = LocalCtx#key_context{owner=UserStr},
            {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}};
        {true, OwnerId} ->
            %% bill the owner, not the actor
            AccessRD = riak_cs_access_logger:set_user(OwnerId, RD),
            UpdLocalCtx = LocalCtx#key_context{owner=OwnerId},
            {false, AccessRD, Ctx#context{local_context=UpdLocalCtx}};
        false ->
            %% ACL check failed, deny access
            riak_cs_wm_utils:deny_access(RD, Ctx)
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['POST'].

post_is_create(RD, Ctx) ->
    {false, RD, Ctx}.

%% TODO: Use the RiakcPid in our Ctx and thread it through initiate_mult....
process_post(RD, Ctx=#context{local_context=LocalCtx}) ->
    #key_context{bucket=Bucket, key=Key} = LocalCtx,
    User = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
    UploadId64 = re:replace(wrq:path(RD), ".*/uploads/", "", [{return, binary}]),
    Body = binary_to_list(wrq:req_body(RD)),
    case {parse_body(Body), catch base64url:decode(UploadId64)} of
        {bad, _} ->
            {{halt,477}, RD, Ctx};
        {PartETags, UploadId} ->
            %% TODO: double-check this cut-and-paste'd TODO list.....
            %% TODO: pass in x-amz-acl?
            %% TODO: pass in additional x-amz-meta-* headers?
            %% TODO: pass in Content-​Disposition?
            %% TODO: pass in Content-​Encoding?
            %% TODO: pass in Expires?
            %% TODO: pass in x-amz-server-side​-encryption?
            %% TODO: pass in x-amz-storage-​class?
            %% TODO: pass in x-amz-server-side​-encryption?
            %% TODO: pass in x-amz-grant-* headers?
            case riak_cs_mp_utils:complete_multipart_upload(
                   Bucket, list_to_binary(Key), UploadId, PartETags, User) of
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
                    XErr = riak_cs_wm_utils:make_special_error("NoSuchUpload"),
                    RD2 = wrq:set_resp_body(XErr, RD),
                    {{halt, 404}, RD2, Ctx};
                {error, bad_etag} ->
                    XErr = riak_cs_wm_utils:make_special_error("InvalidPart"),
                    RD2 = wrq:set_resp_body(XErr, RD),
                    {{halt, 400}, RD2, Ctx};
                {error, Reason} ->
io:format("~p LINE ~p ~p\n", [?MODULE, ?LINE, Reason]),
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
%% TODO: Use the RiakcPid in our Ctx and thread it through abort_mult....
delete_resource(RD, Ctx=#context{local_context=LocalCtx,
                                 riakc_pid=_RiakPid}) ->
    case (catch base64url:decode(wrq:path_info('uploadId', RD))) of
        {'EXIT', _Reason} ->
            {{halt, 404}, RD, Ctx};
        UploadId ->
            #key_context{bucket=Bucket, key=KeyStr} = LocalCtx,
            Key = list_to_binary(KeyStr),
            User = riak_cs_mp_utils:user_rec_to_3tuple(Ctx#context.user),
            case riak_cs_mp_utils:abort_multipart_upload(Bucket, Key,
                                                         UploadId, User) of
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
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'POST' ->
            {[{?XML_TYPE, unused_callback}], RD, Ctx};
       true ->
            %% TODO this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", unused_callback}], RD, Ctx}
    end.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx=#context{local_context=LocalCtx0}) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            DefaultCType = "application/octet-stream",
            LocalCtx = LocalCtx0#key_context{putctype=DefaultCType},
            {[{DefaultCType, unused_callback}],
             RD,
             Ctx#context{local_context=LocalCtx}};
        %% This was shamelessly ripped out of
        %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            case string:tokens(Media, "/") of
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    LocalCtx = LocalCtx0#key_context{putctype=Media},
                    {[{Media, unused_callback}], RD, Ctx#context{local_context=LocalCtx}};
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
            end
    end.

parse_body(Body) ->
    try
        {ParsedData, _Rest} = xmerl_scan:string(Body, []),
        #xmlElement{name='CompleteMultipartUpload'} = ParsedData,
        Nums = [list_to_integer(T#xmlText.value) ||
                   T <- xmerl_xpath:string("//CompleteMultipartUpload/Part/PartNumber/text()", ParsedData)],
        ETags = [list_to_binary(string:strip(T#xmlText.value, both, $")) ||
                   T <- xmerl_xpath:string("//CompleteMultipartUpload/Part/ETag/text()", ParsedData)],
        lists:zip(Nums, ETags)
    catch _:_ ->
            bad
    end.
