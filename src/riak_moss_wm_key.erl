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
        {true, Owner} ->
            %% bill the owner, not the actor
            AccessRD = riak_moss_access_logger:set_user(Owner, RD),
            forbidden(Method, AccessRD, NewCtx);
        true ->
            %% the actor *is* the owner
            AccessRD = riak_moss_access_logger:set_user(
                         ICtx#context.user, RD),
            forbidden(Method, AccessRD, NewCtx);
        false ->
            riak_moss_wm_utils:deny_access(RD, NewCtx)
    end.

%% @doc Final step of {@link forbidden/2}: after the permissions have
%% been confirmed, ensure that the document metadata is available
%% during GET & HEAD processing.
forbidden(Method, RD, Ctx=#key_context{doc_metadata=undefined})
  when Method == 'GET'; Method == 'HEAD' ->
    %% no idea whether the doc is available - go check
    NewCtx = riak_moss_wm_utils:ensure_doc(Ctx),
    forbidden(Method, RD, NewCtx);
forbidden(Method, RD, Ctx=#key_context{doc_metadata=notfound})
  when Method == 'GET'; Method == 'HEAD' ->
    %% the doc is not available - say it doesn't exist
    {{halt, 404}, RD, Ctx};
forbidden(_, RD, Ctx) ->
    %% the doc is available, or we don't care (for PUT & such)
    {false, RD, Ctx}.

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
                    case Length =< ?MAX_CONTENT_LENGTH of
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
content_types_provided(RD, Ctx) ->
    %% TODO:
    %% As I understand S3, the content types provided
    %% will either come from the value that was
    %% last PUT or, from you adding a
    %% `response-content-type` header in the request.
    Method = wrq:method(RD),
    if Method == 'GET'; Method == 'HEAD' ->
            DocCtx = riak_moss_wm_utils:ensure_doc(Ctx),
            ContentType = dict:fetch("content-type", DocCtx#key_context.doc_metadata),
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
                              context=#context{requested_perm='READ_ACP'}}=KeyCtx) ->
    riak_moss_get_fsm:stop(GetFsmPid),
    {riak_moss_acl_utils:empty_acl_xml(), RD, KeyCtx};
produce_body(RD, #key_context{get_fsm_pid=GetFsmPid, doc_metadata=DocMeta}=Ctx) ->
    ContentLength = dict:fetch("content-length", DocMeta),
    ContentMd5 = dict:fetch("content-md5", DocMeta),
    ETag = "\"" ++ riak_moss_utils:binary_to_hexlist(ContentMd5) ++ "\"",
    NewRQ = lists:foldl(fun({K, V}, Rq) -> wrq:set_resp_header(K, V, Rq) end,
                        RD,
                        [{"ETag",  ETag},
                         {"Last-Modified", dict:fetch("last-modified", DocMeta)}
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
    case riak_moss_delete_fsm_sup:start_delete_fsm(node(), [Bucket, BinKey, 600000]) of
        {ok, _Pid} ->
            {true, RD, Ctx};
        {error, Reason} ->
            lager:error("delete fsm couldn't be started: ~p", [Reason]),
            riak_moss_s3_response:api_error({riak_connect_failed, Reason},
                                            RD,
                                            Ctx)
    end.

-spec content_types_accepted(term(), term()) ->
    {[{string(), atom()}], term(), term()}.
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            {[{"binary/octet-stream", accept_body}], RD, Ctx};
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
                                 putctype=ContentType,
                                 size=Size}) ->
    Args = [Bucket, Key, Size, ContentType, <<>>, 60000],
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
finalize_request(RD, #key_context{size=S}=Ctx, Pid) ->
    %% TODO: probably want something that counts actual bytes uploaded
    %% instead, to record partial/aborted uploads
    AccessRD = riak_moss_access_logger:set_bytes_in(S, RD),

    %Metadata = dict:from_list([{<<"content-type">>, CType}]),
    {ok, Manifest} = riak_moss_put_fsm:finalize(Pid),
    ETag = "\"" ++ riak_moss_utils:binary_to_hexlist(riak_moss_lfs_utils:content_md5(Manifest)) ++ "\"",
    {true, wrq:set_resp_header("ETag",  ETag, AccessRD), Ctx}.
