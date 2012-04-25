%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_key).

-export([init/1,
         service_available/2,
         forbidden/2,
         content_types_provided/2,
         produce_body/2,
         allowed_methods/2,
         content_types_accepted/2,
         accept_body/2,
         delete_resource/2,
         valid_entity_length/2,
         finish_request/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #key_context{context=#context{auth_bypass=AuthBypass,
                                       start_time=now()}}}.

-spec extract_paths(term(), term()) -> term().
extract_paths(RD, Ctx) ->
    Bucket = list_to_binary(wrq:path_info(bucket, RD)),
    case wrq:path_tokens(RD) of
        [] ->
            Key = undefined;
        KeyTokens ->
            Key = mochiweb_util:unquote(string:join(KeyTokens, "/"))
    end,
    Ctx#key_context{bucket=Bucket, key=Key}.

-spec service_available(term(), term()) -> {true, term(), term()}.
service_available(RD, Ctx) ->
    case riak_moss_wm_utils:service_available(RD, Ctx) of
        {true, ServiceRD, ServiceCtx} ->
            %% this fills in the bucket and key
            %% part of the context so they are
            %% available in the rest of the
            %% chain
            NewCtx = extract_paths(ServiceRD, ServiceCtx),
            {true, ServiceRD, NewCtx};
        {false, _, _} ->
            {false, RD, Ctx}
    end.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, #key_context{context=ICtx}=Ctx) ->
    Next = fun(NewRD, NewCtx) ->
                   get_access_and_manifest(NewRD, Ctx#key_context{context=NewCtx})
           end,
    riak_moss_wm_utils:find_and_auth_user(RD, ICtx, Next).

%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
get_access_and_manifest(RD, Ctx=#key_context{context=InnerCtx}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_moss_acl_utils:requested_access(Method,
                                             wrq:req_qs(RD)),
    NewInnerCtx = InnerCtx#context{requested_perm=RequestedAccess},
    NewCtx = riak_moss_wm_utils:ensure_doc(Ctx#key_context{context=NewInnerCtx}),
    check_permission(Method, RD, NewCtx).

%% @doc Final step of {@link forbidden/2}: Authentication succeeded,
%% now perform ACL check to verify access permission.
check_permission('GET', RD, Ctx=#key_context{manifest=notfound}) ->
    {{halt, 404}, maybe_log_user(RD, Ctx), Ctx};
check_permission('HEAD', RD, Ctx=#key_context{manifest=notfound}) ->
    {{halt, 404}, maybe_log_user(RD, Ctx), Ctx};
check_permission(Method, RD, Ctx=#key_context{bucket=Bucket,
                                       context=InnerCtx,
                                       manifest=Mfst}) ->
    RiakPid = InnerCtx#context.riakc_pid,
    RequestedAccess =
        riak_moss_acl_utils:requested_access(Method,
                                             wrq:req_qs(RD)),
    case InnerCtx#context.user of
        undefined ->
            User = CanonicalId = undefined;
        User ->
            CanonicalId = User?MOSS_USER.canonical_id
    end,
    case Mfst of
        notfound ->
            ObjectAcl = undefined;
        _ ->
            ObjectAcl = Mfst#lfs_manifest_v2.acl
    end,
    case riak_moss_acl:object_access(Bucket,
                                     ObjectAcl,
                                     RequestedAccess,
                                     CanonicalId,
                                     RiakPid) of
        true ->
            %% actor is the owner
            AccessRD = riak_moss_access_logger:set_user(User, RD),
            {false, AccessRD, Ctx#key_context{owner=User}};
        {true, OwnerId} ->
            %% bill the owner, not the actor
            AccessRD = riak_moss_access_logger:set_user(OwnerId, RD),
            {false, AccessRD, Ctx#key_context{owner=OwnerId}};
        false ->
            %% ACL check failed, deny access
            riak_moss_wm_utils:deny_access(RD, Ctx)
    end.

%% @doc Only set the user for the access logger to catch if there is a
%% user to catch.
maybe_log_user(RD, #key_context{context=Context}) ->
    case Context#context.user of
        undefined ->
            RD;
        User ->
            riak_moss_access_logger:set_user(User, RD)
    end.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods(term(), term()) -> {[atom()], term(), term()}.
allowed_methods(RD, Ctx) ->
    %% TODO: POST
    {['HEAD', 'GET', 'DELETE', 'PUT'], RD, Ctx}.

valid_entity_length(RD, Ctx) ->
    case wrq:method(RD) of
        'PUT' ->
            case catch(
                   list_to_integer(
                     wrq:get_req_header("Content-Length", RD))) of
                Length when is_integer(Length) ->
                    case Length =< riak_moss_lfs_utils:max_content_len() of
                        false ->
                            riak_moss_s3_response:api_error(
                              entity_too_large, RD, Ctx);
                        true ->
                            {true, RD, Ctx#key_context{size=Length}}
                    end;
                _ ->
                    {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.

-spec content_types_provided(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_provided(RD, Ctx=#key_context{manifest=Mfst}) ->
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            DocCtx = riak_moss_wm_utils:ensure_doc(Ctx),
            ContentType = binary_to_list(Mfst#lfs_manifest_v2.content_type),
            case ContentType of
                undefined ->
                    {[{"application/octet-stream", produce_body}], RD, DocCtx};
                _ ->
                    {[{ContentType, produce_body}], RD, DocCtx}
            end;
       true ->
            %% TODO
            %% this shouldn't ever be
            %% called, it's just to appease
            %% webmachine
            {[{"text/plain", produce_body}], RD, Ctx}
    end.

-spec produce_body(term(), term()) -> {iolist()|binary(), term(), term()}.
produce_body(RD, #key_context{get_fsm_pid=GetFsmPid,
                              manifest=Mfst,
                              context=#context{start_time=StartTime,
                                               requested_perm='READ_ACP'}}=KeyCtx) ->
    riak_moss_get_fsm:stop(GetFsmPid),
    riak_cs_stats:update(object_get_acl,
                         timer:now_diff(os:timestamp(), StartTime)),
    Acl = Mfst#lfs_manifest_v2.acl,
    case Acl of
        undefined ->
            {riak_moss_acl_utils:empty_acl_xml(), RD, KeyCtx};
        _ ->
            {riak_moss_acl_utils:acl_to_xml(Acl), RD, KeyCtx}
    end;
produce_body(RD, #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst,
                              context=#context{start_time=StartTime}}=Ctx) ->
    ContentLength = Mfst#lfs_manifest_v2.content_length,
    ContentMd5 = Mfst#lfs_manifest_v2.content_md5,
    LastModified = riak_moss_wm_utils:to_rfc_1123(Mfst#lfs_manifest_v2.created),
    ETag = "\"" ++ riak_moss_utils:binary_to_hexlist(ContentMd5) ++ "\"",
    NewRQ = lists:foldl(fun({K, V}, Rq) -> wrq:set_resp_header(K, V, Rq) end,
                        RD,
                        [{"ETag",  ETag},
                         {"Last-Modified", LastModified}
                        ]),
    Method = wrq:method(RD),
    case Method == 'HEAD'
        orelse
    ContentLength == 0 of
        true ->
            riak_moss_get_fsm:stop(GetFsmPid),
            StreamFun = fun() -> {<<>>, done} end;
        false ->
            riak_moss_get_fsm:continue(GetFsmPid),
            StreamFun = fun() -> riak_moss_wm_utils:streaming_get(GetFsmPid,
                                                                  StartTime) end
    end,
    if Method == 'HEAD' ->
            riak_cs_stats:update(object_head,
                                 timer:now_diff(os:timestamp(), StartTime));
       true ->
            ok
    end,
    {{known_length_stream, ContentLength, {<<>>, StreamFun}}, NewRQ, Ctx}.

%% @doc Callback for deleting an object.
-spec delete_resource(term(), term()) -> boolean().
delete_resource(RD, Ctx=#key_context{bucket=Bucket,
                                     key=Key,
                                     get_fsm_pid=GetFsmPid,
                                     context=InnerCtx}) ->
    riak_moss_get_fsm:stop(GetFsmPid),
    BinKey = list_to_binary(Key),
    #context{riakc_pid=RiakPid} = InnerCtx,
    riak_moss_delete_marker:delete(Bucket, BinKey, RiakPid),
    {true, RD, Ctx}.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            {[{"application/octet-stream", accept_body}], RD, Ctx};
        %% This was shamelessly ripped out of
        %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
        CType ->
            {Media, _Params} = mochiweb_util:parse_header(CType),
            case string:tokens(Media, "/") of
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    {[{Media, accept_body}], RD, Ctx#key_context{putctype=Media}};
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

-spec accept_body(term(), term()) ->
    {true, term(), term()}.
accept_body(RD, Ctx=#key_context{bucket=Bucket,
                                 key=Key,
                                 manifest=Mfst,
                                 owner=Owner,
                                 get_fsm_pid=GetFsmPid,
                                 context=#context{user=User,
                                                  riakc_pid=RiakPid,
                                                  requested_perm='WRITE_ACP'}}) ->


    riak_moss_get_fsm:stop(GetFsmPid),
    Body = binary_to_list(wrq:req_body(RD)),
    case Body of
        [] ->
            %% Check for `x-amz-acl' header to support
            %% the use of a canned ACL.
            Acl = riak_moss_acl_utils:canned_acl(
                    wrq:get_req_header("x-amz-acl", RD),
                    {User?MOSS_USER.display_name,
                     User?MOSS_USER.canonical_id,
                     User?MOSS_USER.key_id},
                    Owner,
                    RiakPid);
        _ ->
            Acl = riak_moss_acl_utils:acl_from_xml(Body,
                                                   User?MOSS_USER.key_id,
                                                   RiakPid)
    end,
    %% Write new ACL to active manifest
    case riak_moss_utils:set_object_acl(Bucket, Key, Mfst, Acl, RiakPid) of
        ok ->
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end;
accept_body(RD, Ctx=#key_context{bucket=Bucket,
                                 key=Key,
                                 putctype=ContentType,
                                 size=Size,
                                 get_fsm_pid=GetFsmPid,
                                 owner=Owner,
                                 context=#context{user=User,
                                                  riakc_pid=RiakPid}}) ->
    riak_moss_get_fsm:stop(GetFsmPid),
    %% TODO:
    %% the Metadata
    %% should be pulled out of the
    %% headers
    Metadata = orddict:new(),
    BlockSize = riak_moss_lfs_utils:block_size(),
    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_moss_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?MOSS_USER.display_name,
             User?MOSS_USER.canonical_id,
             User?MOSS_USER.key_id},
            Owner,
            RiakPid),
    Args = [Bucket, list_to_binary(Key), Size, list_to_binary(ContentType),
        Metadata, BlockSize, ACL, timer:seconds(60), self(), RiakPid],
    {ok, Pid} = riak_moss_put_fsm_sup:start_put_fsm(node(), Args),
    accept_streambody(RD, Ctx, Pid, wrq:stream_req_body(RD, riak_moss_lfs_utils:block_size())).

accept_streambody(RD, Ctx=#key_context{size=0}, Pid, {_Data, _Next}) ->
    finalize_request(RD, Ctx, Pid);
accept_streambody(RD, Ctx=#key_context{}, Pid, {Data, Next}) ->
    riak_moss_put_fsm:augment_data(Pid, Data),
    if is_function(Next) ->
            accept_streambody(RD, Ctx, Pid, Next());
       Next =:= done ->
            finalize_request(RD, Ctx, Pid)
    end.

%% TODO:
%% We need to do some checking to make sure
%% the bucket exists for the user who is doing
%% this PUT
finalize_request(RD, #key_context{size=S,context=#context{start_time=StartTime}}=Ctx, Pid) ->
    %% TODO: probably want something that counts actual bytes uploaded
    %% instead, to record partial/aborted uploads
    AccessRD = riak_moss_access_logger:set_bytes_in(S, RD),

    {ok, Manifest} = riak_moss_put_fsm:finalize(Pid),
    ETag = "\"" ++ riak_moss_utils:binary_to_hexlist(Manifest#lfs_manifest_v2.content_md5) ++ "\"",
    riak_cs_stats:update(object_put,
                         timer:now_diff(os:timestamp(), StartTime)),
    {true, wrq:set_resp_header("ETag",  ETag, AccessRD), Ctx}.

finish_request(RD, KeyCtx=#key_context{context=InnerCtx}) ->
    #context{riakc_pid=RiakPid} = InnerCtx,
    case RiakPid of
        undefined ->
            {true, RD, KeyCtx};
        _ ->
            riak_moss_utils:close_riak_connection(RiakPid),
            UpdInnerCtx = InnerCtx#context{riakc_pid=undefined},
            {true, RD, KeyCtx#key_context{context=UpdInnerCtx}}
    end.
