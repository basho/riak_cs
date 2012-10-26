%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_key).

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
-export([dt_return_object/3]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Config) ->
    dt_entry(<<"init">>),
    %% Check if authentication is disabled and
    %% set that in the context.
    AuthBypass = proplists:get_value(auth_bypass, Config),
    {ok, #key_context{context=#context{auth_bypass=AuthBypass,
                                       start_time=os:timestamp()}}}.

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
    dt_entry(<<"service_available">>),
    case riak_cs_wm_utils:service_available(RD, Ctx) of
        {true, ServiceRD, ServiceCtx} ->
            %% this fills in the bucket and key
            %% part of the context so they are
            %% available in the rest of the
            %% chain
            NewCtx = extract_paths(ServiceRD, ServiceCtx),
            dt_return(<<"service_available">>, [1], []),
            {true, ServiceRD, NewCtx};
        {false, _, _} ->
            dt_return(<<"service_available">>, [0], []),
            {false, RD, Ctx}
    end.

%% @doc Check to see if the user is
%%      authenticated. Normally with HTTP
%%      we'd use the `authorized` callback,
%%      but this is how S3 does things.
forbidden(RD, #key_context{context=ICtx}=Ctx) ->
    dt_entry(<<"forbidden">>),
    Next = fun(NewRD, NewICtx) ->
                   get_access_and_manifest(NewRD, Ctx#key_context{context=NewICtx})
           end,
    Conv2KeyCtx = fun(NewICtx) -> Ctx#key_context{context=NewICtx} end,
    case riak_cs_wm_utils:find_and_auth_user(RD, ICtx, Next, Conv2KeyCtx, true) of
        {false, _RD2, Ctx2} = FalseRet ->
            dt_return(<<"forbidden">>, [], [extract_name((Ctx2#key_context.context)#context.user), <<"false">>]),
            FalseRet;
        {Rsn, _RD2, Ctx2} = Ret ->
            Reason = case Rsn of
                         {halt, Code} -> Code;
                         _            -> -1
                     end,
            dt_return(<<"forbidden">>, [Reason], [extract_name((Ctx2#key_context.context)#context.user), <<"true">>]),
            Ret
    end.


%% @doc Get the type of access requested and the manifest with the
%% object ACL and compare the permission requested with the permission
%% granted, and allow or deny access. Returns a result suitable for
%% directly returning from the {@link forbidden/2} webmachine export.
get_access_and_manifest(RD, Ctx=#key_context{context=InnerCtx}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method,
                                             wrq:req_qs(RD)),
    NewInnerCtx = InnerCtx#context{requested_perm=RequestedAccess},
    NewCtx = riak_cs_wm_utils:ensure_doc(Ctx#key_context{context=NewInnerCtx}),
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
        riak_cs_acl_utils:requested_access(Method,
                                             wrq:req_qs(RD)),
    case InnerCtx#context.user of
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
            {false, AccessRD, Ctx#key_context{owner=UserStr}};
        {true, OwnerId} ->
            %% bill the owner, not the actor
            AccessRD = riak_cs_access_logger:set_user(OwnerId, RD),
            {false, AccessRD, Ctx#key_context{owner=OwnerId}};
        false ->
            %% ACL check failed, deny access
            riak_cs_wm_utils:deny_access(RD, Ctx)
    end.

%% @doc Only set the user for the access logger to catch if there is a
%% user to catch.
maybe_log_user(RD, #key_context{context=Context}) ->
    case Context#context.user of
        undefined ->
            RD;
        User ->
            riak_cs_access_logger:set_user(User, RD)
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
                    case Length =< riak_cs_lfs_utils:max_content_len() of
                        false ->
                            riak_cs_s3_response:api_error(
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
    dt_entry(<<"content_types_provided">>),
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            DocCtx = riak_cs_wm_utils:ensure_doc(Ctx),
            ContentType = binary_to_list(Mfst?MANIFEST.content_type),
            case ContentType of
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
                                               user=User,
                                               requested_perm='READ_ACP'}}=KeyCtx) ->
    {Bucket, File} = Mfst?MANIFEST.bkey,
    BFile_str = [Bucket, $,, File],
    UserName = extract_name(User),
    dt_entry(<<"produce_body">>, [], [UserName, BFile_str]),
    dt_entry_object(<<"get_acl">>, [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    ok = riak_cs_stats:update_with_start(object_get_acl, StartTime),
    Acl = Mfst?MANIFEST.acl,
    case Acl of
        undefined ->
            dt_return(<<"produce_body">>, [-1], [UserName, BFile_str]),
            dt_return_object(<<"get_acl">>, [-1], [UserName, BFile_str]),
            {riak_cs_acl_utils:empty_acl_xml(), RD, KeyCtx};
        _ ->
            dt_return(<<"produce_body">>, [-2], [UserName, BFile_str]),
            dt_return_object(<<"get_acl">>, [-2], [UserName, BFile_str]),
            {riak_cs_acl_utils:acl_to_xml(Acl), RD, KeyCtx}
    end;
produce_body(RD, #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst,
                              context=#context{start_time=StartTime,
                                               user=User}}=Ctx) ->
    {Bucket, File} = Mfst?MANIFEST.bkey,
    BFile_str = [Bucket, $,, File],
    UserName = extract_name(User),
    dt_entry(<<"produce_body">>, [], [UserName, BFile_str]),
    dt_entry_object(<<"file_get">>, [], [UserName, BFile_str]),
    ContentLength = Mfst?MANIFEST.content_length,
    ContentMd5 = Mfst?MANIFEST.content_md5,
    LastModified = riak_cs_wm_utils:to_rfc_1123(Mfst?MANIFEST.created),
    ETag = "\"" ++ riak_cs_utils:binary_to_hexlist(ContentMd5) ++ "\"",
    NewRQ = lists:foldl(fun({K, V}, Rq) -> wrq:set_resp_header(K, V, Rq) end,
                        RD,
                        [{"ETag",  ETag},
                         {"Last-Modified", LastModified}
                        ] ++  Mfst?MANIFEST.metadata),
    Method = wrq:method(RD),
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
            dt_return_object(<<"file_head">>, [], [UserName, BFile_str]),
            ok = riak_cs_stats:update_with_start(object_head, StartTime);
       true ->
            ok
    end,
    dt_return(<<"produce_body">>, [ContentLength], [UserName, BFile_str]),
    {{known_length_stream, ContentLength, {<<>>, StreamFun}}, NewRQ, Ctx}.

%% @doc Callback for deleting an object.
-spec delete_resource(term(), term()) -> {true, term(), #key_context{}}.
delete_resource(RD, Ctx=#key_context{bucket=Bucket,
                                     key=Key,
                                     get_fsm_pid=GetFsmPid,
                                     context=InnerCtx}) ->
    BFile_str = [Bucket, $,, Key],
    UserName = extract_name(InnerCtx#context.user),
    dt_entry(<<"delete_resource">>, [], [UserName, BFile_str]),
    dt_entry_object(<<"file_delete">>, [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    BinKey = list_to_binary(Key),
    #context{riakc_pid=RiakPid} = InnerCtx,
    DeleteObjectResponse = riak_cs_utils:delete_object(Bucket, BinKey, RiakPid),
    handle_delete_object(DeleteObjectResponse, UserName, BFile_str, RD, Ctx).

%% @private
handle_delete_object({error, Error}, UserName, BFile_str, RD, Ctx) ->
    lager:error("delete object failed with reason: ", [Error]),
    dt_return(<<"delete_resource">>, [0], [UserName, BFile_str]),
    dt_return_object(<<"file_delete">>, [0], [UserName, BFile_str]),
    {false, RD, Ctx};
handle_delete_object({ok, _UUIDsMarkedforDelete}, UserName, BFile_str, RD, Ctx) ->
    dt_return(<<"delete_resource">>, [1], [UserName, BFile_str]),
    dt_return_object(<<"file_delete">>, [1], [UserName, BFile_str]),
    {true, RD, Ctx}.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    dt_entry(<<"content_types_accepted">>),
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            DefaultCType = "application/octet-stream",
            {[{DefaultCType, accept_body}],
             RD,
             Ctx#key_context{putctype=DefaultCType}};
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
    {boolean() | {halt, term()}, term(), term()}.
accept_body(RD, Ctx=#key_context{bucket=Bucket,
                                 key=KeyStr,
                                 manifest=Mfst,
                                 owner=Owner,
                                 get_fsm_pid=GetFsmPid,
                                 context=#context{user=User,
                                                  riakc_pid=RiakPid,
                                                  requested_perm='WRITE_ACP'}})
  when Bucket /= undefined, KeyStr /= undefined,
       Mfst /= undefined, RiakPid /= undefined ->
    BFile_str = [Bucket, $,, KeyStr],
    UserName = extract_name(User),
    dt_entry(<<"accept_body">>, [], [UserName, BFile_str]),
    dt_entry_object(<<"file_put_acl">>, [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    Body = binary_to_list(wrq:req_body(RD)),
    case Body of
        [] ->
            %% Check for `x-amz-acl' header to support
            %% the use of a canned ACL.
            Acl = riak_cs_acl_utils:canned_acl(
                    wrq:get_req_header("x-amz-acl", RD),
                    {User?RCS_USER.display_name,
                     User?RCS_USER.canonical_id,
                     User?RCS_USER.key_id},
                    Owner,
                    RiakPid);
        _ ->
            Acl = riak_cs_acl_utils:acl_from_xml(Body,
                                                   User?RCS_USER.key_id,
                                                   RiakPid)
    end,
    %% Write new ACL to active manifest
    Key = list_to_binary(KeyStr),
    case riak_cs_utils:set_object_acl(Bucket, Key, Mfst, Acl, RiakPid) of
        ok ->
            dt_return(<<"accept_body">>, [200], [UserName, BFile_str]),
            dt_return_object(<<"file_put_acl">>, [200], [UserName, BFile_str]),
            {{halt, 200}, RD, Ctx};
        {error, Reason} ->
            Code = riak_cs_s3_response:status_code(Reason),
            dt_return(<<"accept_body">>, [Code], [UserName, BFile_str]),
            dt_return_object(<<"file_put_acl">>, [Code], [UserName, BFile_str]),
            riak_cs_s3_response:api_error(Reason, RD, Ctx)
    end;
accept_body(RD, Ctx=#key_context{bucket=Bucket,
                                 key=Key,
                                 putctype=ContentType,
                                 size=Size,
                                 get_fsm_pid=GetFsmPid,
                                 owner=Owner,
                                 context=#context{user=User,
                                                  riakc_pid=RiakPid}}) ->
    BFile_str = [Bucket, $,, Key],
    UserName = extract_name(User),
    dt_entry(<<"accept_body">>, [], [UserName, BFile_str]),
    dt_entry_object(<<"file_put">>, [], [UserName, BFile_str]),
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
            RiakPid),
    Args = [{Bucket, list_to_binary(Key), Size, list_to_binary(ContentType),
             Metadata, BlockSize, ACL, timer:seconds(60), self(), RiakPid}],
    {ok, Pid} = riak_cs_put_fsm_sup:start_put_fsm(node(), Args),
    accept_streambody(RD, Ctx, Pid, wrq:stream_req_body(RD, riak_cs_lfs_utils:block_size())).

accept_streambody(RD, Ctx=#key_context{size=0}, Pid, {_Data, _Next}) ->
    finalize_request(RD, Ctx, Pid);
accept_streambody(RD, Ctx=#key_context{bucket=Bucket,
                                       key=Key,
                                       context=#context{user=User}},
                  Pid, {Data, Next}) ->
    BFile_str = [Bucket, $,, Key],
    UserName = extract_name(User),
    dt_entry(<<"accept_streambody">>, [size(Data)], [UserName, BFile_str]),
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
finalize_request(RD, #key_context{bucket=Bucket,
                                  key=Key,
                                  size=S,
                                  context=#context{start_time=StartTime,
                                                   user=User}}=Ctx, Pid) ->
    BFile_str = [Bucket, $,, Key],
    UserName = extract_name(User),
    dt_entry(<<"finalize_request">>, [S], [UserName, BFile_str]),
    dt_entry_object(<<"file_put">>, [S], [UserName, BFile_str]),
    %% TODO: probably want something that counts actual bytes uploaded
    %% instead, to record partial/aborted uploads
    AccessRD = riak_cs_access_logger:set_bytes_in(S, RD),

    {ok, Manifest} = riak_cs_put_fsm:finalize(Pid),
    ETag = "\"" ++ riak_cs_utils:binary_to_hexlist(Manifest?MANIFEST.content_md5) ++ "\"",
    ok = riak_cs_stats:update_with_start(object_put, StartTime),
    dt_return(<<"finalize_request">>, [S], [UserName, BFile_str]),
    {{halt, 200}, wrq:set_resp_header("ETag",  ETag, AccessRD), Ctx}.

finish_request(RD, KeyCtx=#key_context{bucket=Bucket,
                                       key=Key,
                                       context=InnerCtx=#context{user=User}}) ->
    BFile_str = [Bucket, $,, Key],
    UserName = extract_name(User),
    dt_entry(<<"finish_request">>, [], [UserName, BFile_str]),
    #context{riakc_pid=RiakPid} = InnerCtx,
    case RiakPid of
        undefined ->
            dt_return(<<"finish_request">>, [0], [UserName, BFile_str]),
            {true, RD, KeyCtx};
        _ ->
            riak_cs_utils:close_riak_connection(RiakPid),
            UpdInnerCtx = InnerCtx#context{riakc_pid=undefined},
            dt_return(<<"finish_request">>, [1], [UserName, BFile_str]),
            {true, RD, KeyCtx#key_context{context=UpdInnerCtx}}
    end.

extract_name(X) ->
    riak_cs_wm_utils:extract_name(X).

dt_entry(Func) ->
    dt_entry(Func, [], []).

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, Ints, ?MODULE, Func, Strings).

dt_entry_object(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_OBJECT_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 2, Ints, ?MODULE, Func, Strings).

dt_return_object(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_OBJECT_OP, 2, Ints, ?MODULE, Func, Strings).
