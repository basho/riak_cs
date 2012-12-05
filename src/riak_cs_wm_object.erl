%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_object).

-export([init/1,
         authorize/2,
         content_types_provided/2,
         produce_body/2,
         allowed_methods/0,
         malformed_request/2,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         valid_entity_length/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{}}}.

malformed_request(RD,Ctx=#context{local_context=LocalCtx0}) ->    
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    %% need to unquote twice since we re-urlencode the string during rewrite in 
    %% order to trick webmachine dispatching
    Key = mochiweb_util:unquote(mochiweb_util:unquote(wrq:path_info(object, RD))),
    LocalCtx = LocalCtx0#key_context{bucket=Bucket, key=Key},
    {false, RD, Ctx#context{local_context=LocalCtx}}.

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
authorize(RD, Ctx0=#context{local_context=LocalCtx0, riakc_pid=RiakPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RiakPid),
    Ctx = Ctx0#context{requested_perm=RequestedAccess,local_context=LocalCtx},
    check_permission(Method, RD, Ctx, LocalCtx#key_context.manifest).

%% @doc Final step of {@link forbidden/2}: Authentication succeeded,
%% now perform ACL check to verify access permission.
check_permission('GET', RD, Ctx, notfound) ->
    {{halt, 404}, riak_cs_access_logger:set_user(Ctx#context.user, RD), Ctx};
check_permission('HEAD', RD, Ctx, notfound) ->
    {{halt, 404}, riak_cs_access_logger:set_user(Ctx#context.user, RD), Ctx};
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
    %% TODO: POST
    ['HEAD', 'GET', 'DELETE', 'PUT'].

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

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx=#context{local_context=LocalCtx,
                                        riakc_pid=RiakcPid}) ->
    Mfst = LocalCtx#key_context.manifest,
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            UpdLocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx, RiakcPid),
            ContentType = binary_to_list(Mfst?MANIFEST.content_type),
            case ContentType of
                _ ->
                    UpdCtx = Ctx#context{local_context=UpdLocalCtx},
                    {[{ContentType, produce_body}], RD, UpdCtx}
            end;
       true ->
            %% TODO this shouldn't ever be called, it's just to
            %% appease webmachine
            {[{"text/plain", produce_body}], RD, Ctx}
    end.

-spec produce_body(term(), term()) -> {iolist()|binary(), term(), term()}.
produce_body(RD, Ctx=#context{local_context=LocalCtx,
                              start_time=StartTime,
                              user=User}) ->
    #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst} = LocalCtx,
    {Bucket, File} = Mfst?MANIFEST.bkey,
    BFile_str = [Bucket, $,, File],
    UserName = riak_cs_wm_utils:extract_name(User),
    Method = wrq:method(RD),
    Func = case Method of 
               'HEAD' -> <<"object_head">>;
               _ -> <<"object_get">>
           end,
    riak_cs_dtrace:dt_object_entry(?MODULE, Func, [], [UserName, BFile_str]),
    ContentLength = Mfst?MANIFEST.content_length,
    ContentMd5 = Mfst?MANIFEST.content_md5,
    LastModified = riak_cs_wm_utils:to_rfc_1123(Mfst?MANIFEST.created),
    ETag = "\"" ++ riak_cs_utils:binary_to_hexlist(ContentMd5) ++ "\"",
    NewRQ = lists:foldl(fun({K, V}, Rq) -> wrq:set_resp_header(K, V, Rq) end,
                        RD,
                        [{"ETag",  ETag},
                         {"Last-Modified", LastModified}
                        ] ++  Mfst?MANIFEST.metadata),
    case Method == 'HEAD'
        orelse
    ContentLength == 0 of
        true ->
            riak_cs_get_fsm:stop(GetFsmPid),
            StreamFun = fun() -> {<<>>, done} end;
        false ->
            riak_cs_get_fsm:continue(GetFsmPid),
            StreamFun = fun() -> riak_cs_wm_utils:streaming_get(
                                   GetFsmPid, StartTime, UserName, BFile_str)
                        end
    end,
    if Method == 'HEAD' ->
            riak_cs_dtrace:dt_object_return(?MODULE, <<"object_head">>, 
                                               [], [UserName, BFile_str]),
            ok = riak_cs_stats:update_with_start(object_head, StartTime);
       true ->
            ok
    end,
    {{known_length_stream, ContentLength, {<<>>, StreamFun}}, NewRQ, Ctx}.

%% @doc Callback for deleting an object.
-spec delete_resource(term(), term()) -> {true, term(), #key_context{}}.
delete_resource(RD, Ctx=#context{local_context=LocalCtx,
                                 riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 get_fsm_pid=GetFsmPid} = LocalCtx,
    BFile_str = [Bucket, $,, Key],
    UserName = riak_cs_wm_utils:extract_name(Ctx#context.user),  
    riak_cs_dtrace:dt_object_entry(?MODULE, <<"object_delete">>, 
                                      [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    BinKey = list_to_binary(Key),
    DeleteObjectResponse = riak_cs_utils:delete_object(Bucket, BinKey, RiakcPid),
    handle_delete_object(DeleteObjectResponse, UserName, BFile_str, RD, Ctx).

%% @private
handle_delete_object({error, Error}, UserName, BFile_str, RD, Ctx) ->
    lager:error("delete object failed with reason: ", [Error]),
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_delete">>, [0], [UserName, BFile_str]),
    {false, RD, Ctx};
handle_delete_object({ok, _UUIDsMarkedforDelete}, UserName, BFile_str, RD, Ctx) ->
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_delete">>, [1], [UserName, BFile_str]),
    {true, RD, Ctx}.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx=#context{local_context=LocalCtx0}) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            DefaultCType = "application/octet-stream",
            LocalCtx = LocalCtx0#key_context{putctype=DefaultCType},
            {[{DefaultCType, accept_body}],
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
                    {[{Media, accept_body}], RD, Ctx#context{local_context=LocalCtx}};
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

accept_body(RD, Ctx=#context{local_context=LocalCtx,
                             user=User,
                             riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 putctype=ContentType,
                 size=Size,
                 get_fsm_pid=GetFsmPid,
                 owner=Owner} = LocalCtx,
    BFile_str = [Bucket, $,, Key],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_object_entry(?MODULE, <<"object_put">>, 
                                      [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    Metadata = riak_cs_wm_utils:extract_user_metadata(RD),
    BlockSize = riak_cs_lfs_utils:block_size(),
    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_cs_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?RCS_USER.display_name,
             User?RCS_USER.canonical_id,
             User?RCS_USER.key_id},
            Owner,
            RiakcPid),
    Args = [{Bucket, list_to_binary(Key), Size, list_to_binary(ContentType),
             Metadata, BlockSize, ACL, timer:seconds(60), self(), RiakcPid}],
    {ok, Pid} = riak_cs_put_fsm_sup:start_put_fsm(node(), Args),
    accept_streambody(RD, Ctx, Pid, wrq:stream_req_body(RD, riak_cs_lfs_utils:block_size())).

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

%% TODO:
%% We need to do some checking to make sure
%% the bucket exists for the user who is doing
%% this PUT
finalize_request(RD,
                 Ctx=#context{local_context=LocalCtx,
                              start_time=StartTime,
                              user=User},
                 Pid) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 size=S} = LocalCtx,
    BFile_str = [Bucket, $,, Key],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finalize_request">>, [S], [UserName, BFile_str]),
    %% TODO: probably want something that counts actual bytes uploaded
    %% instead, to record partial/aborted uploads
    AccessRD = riak_cs_access_logger:set_bytes_in(S, RD),

    {ok, Manifest} = riak_cs_put_fsm:finalize(Pid),
    ETag = "\"" ++ riak_cs_utils:binary_to_hexlist(Manifest?MANIFEST.content_md5) ++ "\"",
    ok = riak_cs_stats:update_with_start(object_put, StartTime),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finalize_request">>, [S], [UserName, BFile_str]),
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_put">>, [S], [UserName, BFile_str]),
    {{halt, 200}, wrq:set_resp_header("ETag",  ETag, AccessRD), Ctx}.
