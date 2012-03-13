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
         valid_entity_length/2]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #key_context{context=#context{auth_bypass=AuthBypass}}}.

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
                   check_permission(NewRD, Ctx#key_context{context=NewCtx})
           end,
    riak_moss_wm_utils:find_and_auth_user(RD, ICtx, Next).

%% @doc Compare the permission requested with the permission granted,
%% and allow or deny access.  Returns a result suitable for directly
%% returning from the {@link forbidden/2} webmachine export.
check_permission(RD, #key_context{bucket=Bucket,
                                  key=Key,
                                  context=ICtx}=Ctx) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_moss_acl_utils:requested_access(Method, wrq:req_qs(RD)),
    BinKey = list_to_binary(Key),
    Granted = case ICtx#context.user of
                  undefined ->
                      riak_moss_acl:anonymous_object_access(Bucket,
                                                            BinKey,
                                                            RequestedAccess);
                  User ->
                      riak_moss_acl:object_access(Bucket,
                                                  BinKey,
                                                  RequestedAccess,
                                                  User?MOSS_USER.canonical_id)
              end,

    NewICtx = ICtx#context{requested_perm=RequestedAccess},
    NewCtx = Ctx#key_context{context=NewICtx},
    case Granted of
        {true, _Owner} ->
            %% TODO: keep this pathway, even though redundant now - it
            %% will be tracked differently in the access-log branch
            forbidden(Method, RD, NewCtx);
        true ->
            forbidden(Method, RD, NewCtx);
        false ->
            riak_moss_wm_utils:deny_access(RD, NewCtx)
    end.

%% @TODO Further attention needed here.
%% @doc Final step of {@link forbidden/2}: after the permissions have
%% been confirmed, ensure that the document metadata is available
%% during GET & HEAD processing.
%% @doc Authentication succeeded, now perform ACL check to verify
%% access permission.
forbidden(_Method, _RD, Ctx=#key_context{manifest=undefined}) ->
    %% With object ACLs we need the manifest for all
    %% methods so just get it here.
    NewCtx = riak_moss_wm_utils:ensure_doc(Ctx),
    forbidden(_Method, _RD, NewCtx);
forbidden('GET', RD, Ctx=#key_context{manifest=notfound}) ->
    {{halt, 404}, RD, Ctx};
forbidden('HEAD', RD, Ctx=#key_context{manifest=notfound}) ->
    {{halt, 404}, RD, Ctx};
forbidden(Method, RD, Ctx=#key_context{bucket=Bucket,
                                       context=InnerCtx,
                                       manifest=Mfst}) ->
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
                                     CanonicalId) of
        true ->
            {false, RD, Ctx#key_context{owner=User}};
        {true, OwnerId} ->
            case riak_moss_utils:get_user_by_index(?ID_INDEX,
                                                   list_to_binary(OwnerId)) of
                {ok, {Owner, _}} ->
                    {false, RD, Ctx#key_context{owner=Owner}};
                {error, _} ->
                    %% @TODO Decide if this should return 403 instead
                    riak_moss_s3_response:api_error(object_owner_unavailable, RD, Ctx)
            end;
        false ->
            %% ACL check failed, deny access
            riak_moss_s3_response:api_error(access_denied, RD, Ctx)
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
                              context=#context{requested_perm='READ_ACP'}}=KeyCtx) ->
    riak_moss_get_fsm:stop(GetFsmPid),
    Acl = Mfst#lfs_manifest_v2.acl,
    case Acl of
        undefined ->
            {riak_moss_acl_utils:empty_acl_xml(), RD, KeyCtx};
        _ ->
            {riak_moss_acl_utils:acl_to_xml(Acl), RD, KeyCtx}
    end;
produce_body(RD, #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst}=Ctx) ->
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
            StreamFun = fun() -> riak_moss_wm_utils:streaming_get(GetFsmPid) end
    end,
    {{known_length_stream, ContentLength, {<<>>, StreamFun}}, NewRQ, Ctx}.

%% @doc Callback for deleting an object.
-spec delete_resource(term(), term()) -> boolean().
delete_resource(RD, Ctx=#key_context{bucket=Bucket, key=Key}) ->
    BinKey = list_to_binary(Key),
    riak_moss_delete_marker:delete(Bucket, BinKey),
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
                                 context=#context{requested_perm='WRITE_ACP'}}) ->

    Body = binary_to_list(wrq:req_body(RD)),
    Acl = riak_moss_acl_utils:acl_from_xml(Body),
    %% Write new ACL to active manifest
    case riak_moss_utils:set_object_acl(Bucket, Key, Mfst, Acl) of
        ok ->
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            riak_moss_s3_response:api_error(Reason, RD, Ctx)
    end;
accept_body(RD, Ctx=#key_context{bucket=Bucket,
                                 key=Key,
                                 putctype=ContentType,
                                 size=Size,
                                 owner=Owner,
                                 context=#context{user=User}}) ->
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
             User?MOSS_USER.canonical_id},
            {Owner?MOSS_USER.display_name,
             Owner?MOSS_USER.canonical_id}),
    Args = [Bucket, list_to_binary(Key), Size, list_to_binary(ContentType),
        Metadata, BlockSize, ACL, timer:seconds(60), self()],
    {ok, Pid} = riak_moss_put_fsm_sup:start_put_fsm(node(), Args),
    accept_streambody(RD, Ctx, Pid, wrq:stream_req_body(RD, riak_moss_lfs_utils:block_size())).

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
finalize_request(RD, Ctx, Pid) ->
    {ok, Manifest} = riak_moss_put_fsm:finalize(Pid),
    ETag = "\"" ++ riak_moss_utils:binary_to_hexlist(Manifest#lfs_manifest_v2.content_md5) ++ "\"",
    {true, wrq:set_resp_header("ETag",  ETag, RD), Ctx}.
