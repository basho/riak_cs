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

-module(riak_cs_wm_object).

-export([init/1,
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
         valid_entity_length/2]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{local_context=#key_context{}}}.

-spec malformed_request(#wm_reqdata{}, #context{}) -> {false, #wm_reqdata{}, #context{}}.
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
-spec authorize(#wm_reqdata{}, #context{}) ->
                       {boolean() | {halt, term()}, #wm_reqdata{}, #context{}}.
authorize(RD, Ctx0=#context{local_context=LocalCtx0,
                            response_module=ResponseMod,
                            riakc_pid=RiakPid}) ->
    Method = wrq:method(RD),
    RequestedAccess =
        riak_cs_acl_utils:requested_access(Method, false),
    LocalCtx = riak_cs_wm_utils:ensure_doc(LocalCtx0, RiakPid),
    Ctx = Ctx0#context{requested_perm=RequestedAccess,local_context=LocalCtx},

    %% Final step of {@link forbidden/2}: Authentication succeeded,
    case {Method, LocalCtx#key_context.manifest} of
        {'GET', notfound} ->
            ResponseMod:api_error(no_such_key,
                                  riak_cs_access_log_handler:set_user(Ctx#context.user, RD),
                                  Ctx);
        {'HEAD', notfound} ->
            ResponseMod:api_error(no_such_key,
                                  riak_cs_access_log_handler:set_user(Ctx#context.user, RD),
                                  Ctx);
        _ ->
            riak_cs_wm_utils:object_access_authorize_helper(object, true, RD, Ctx)
    end.



%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% TODO: POST
    ['HEAD', 'GET', 'DELETE', 'PUT'].

-spec valid_entity_length(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
valid_entity_length(RD, Ctx=#context{response_module=ResponseMod}) ->
    case wrq:method(RD) of
        'PUT' ->
            case catch(
                   list_to_integer(
                     wrq:get_req_header("Content-Length", RD))) of
                Length when is_integer(Length) ->
                    case Length =< riak_cs_lfs_utils:max_content_len() of
                        false ->
                            ResponseMod:api_error(
                              entity_too_large, RD, Ctx);
                        true ->
                            check_0length_metadata_update(Length, RD, Ctx)
                    end;
                _ ->
                    {false, RD, Ctx}
            end;
        _ ->
            {true, RD, Ctx}
    end.

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
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

-spec generate_etag(#wm_reqdata{}, #context{}) -> {string(), #wm_reqdata{}, #context{}}.
generate_etag(RD, Ctx=#context{local_context=LocalCtx}) ->
    Mfst = LocalCtx#key_context.manifest,
    ETag = riak_cs_utils:etag_from_binary_no_quotes(Mfst?MANIFEST.content_md5),
    {ETag, RD, Ctx}.

-spec last_modified(#wm_reqdata{}, #context{}) -> {calendar:datetime(), #wm_reqdata{}, #context{}}.
last_modified(RD, Ctx=#context{local_context=LocalCtx}) ->
    Mfst = LocalCtx#key_context.manifest,
    ErlDate = riak_cs_wm_utils:iso_8601_to_erl_date(Mfst?MANIFEST.created),
    {ErlDate, RD, Ctx}.

-spec produce_body(#wm_reqdata{}, #context{}) ->
                          {{known_length_stream, non_neg_integer(), {<<>>, function()}}, #wm_reqdata{}, #context{}}.
produce_body(RD, Ctx=#context{local_context=LocalCtx,
                              response_module=ResponseMod}) ->
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

produce_body(RD, Ctx=#context{local_context=LocalCtx,
                              start_time=StartTime,
                              user=User},
             {Start, End}, RespRange) ->
    #key_context{get_fsm_pid=GetFsmPid, manifest=Mfst} = LocalCtx,
    {Bucket, File} = Mfst?MANIFEST.bkey,
    ResourceLength = Mfst?MANIFEST.content_length,
    BFile_str = [Bucket, $,, File],
    UserName = riak_cs_wm_utils:extract_name(User),
    Method = wrq:method(RD),
    Func = case Method of
               'HEAD' -> <<"object_head">>;
               _ -> <<"object_get">>
           end,
    riak_cs_dtrace:dt_object_entry(?MODULE, Func, [], [UserName, BFile_str]),
    ContentMd5 = Mfst?MANIFEST.content_md5,
    LastModified = riak_cs_wm_utils:to_rfc_1123(Mfst?MANIFEST.created),
    ETag = format_etag(ContentMd5),
    NewRQ1 = lists:foldl(fun({K, V}, Rq) -> wrq:set_resp_header(K, V, Rq) end,
                         RD,
                         [{"ETag",  ETag},
                          {"Last-Modified", LastModified}
                         ] ++  Mfst?MANIFEST.metadata),
    NewRQ2 = wrq:set_resp_range(RespRange, NewRQ1),
    StreamBody =
        case Method == 'HEAD'
            orelse
            ResourceLength == 0 of
            true ->
                riak_cs_get_fsm:stop(GetFsmPid),
                fun() -> {<<>>, done} end;
            false ->
                riak_cs_get_fsm:continue(GetFsmPid, {Start, End}),
                {<<>>, fun() ->
                               riak_cs_wm_utils:streaming_get(
                                 GetFsmPid, StartTime, UserName, BFile_str)
                       end}
        end,
    if Method == 'HEAD' ->
            riak_cs_dtrace:dt_object_return(?MODULE, <<"object_head">>,
                                            [], [UserName, BFile_str]),
            ok = riak_cs_stats:update_with_start(object_head, StartTime);
       true ->
            ok
    end,
    {{known_length_stream, ResourceLength, StreamBody}, NewRQ2, Ctx}.

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
-spec delete_resource(#wm_reqdata{}, #context{}) -> {true, #wm_reqdata{}, #context{}}.
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
    _ = lager:error("delete object failed with reason: ~p", [Error]),
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_delete">>, [0], [UserName, BFile_str]),
    {false, RD, Ctx};
handle_delete_object({ok, _UUIDsMarkedforDelete}, UserName, BFile_str, RD, Ctx) ->
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_delete">>, [1], [UserName, BFile_str]),
    {true, RD, Ctx}.

-spec content_types_accepted(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(RD, Ctx) ->
    content_types_accepted(wrq:get_req_header("Content-Type", RD), RD, Ctx).

-spec content_types_accepted(undefined | string(), #wm_reqdata{}, #context{}) ->
                                    {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_accepted(CT, RD, Ctx)
  when CT =:= undefined;
       CT =:= [] ->
    content_types_accepted("application/octet-stream", RD, Ctx);
content_types_accepted(CT, RD, Ctx=#context{local_context=LocalCtx0}) ->
    %% This was shamelessly ripped out of
    %% https://github.com/basho/riak_kv/blob/0d91ca641a309f2962a216daa0cee869c82ffe26/src/riak_kv_wm_object.erl#L492
    {Media, _Params} = mochiweb_util:parse_header(CT),
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
    end.

-spec accept_body(#wm_reqdata{}, #context{}) -> {{halt, integer()}, #wm_reqdata{}, #context{}}.
accept_body(RD, Ctx=#context{local_context=LocalCtx,
                             response_module=ResponseMod,
                             riakc_pid=RiakcPid})
  when LocalCtx#key_context.update_metadata == true ->
    #key_context{bucket=Bucket, key=KeyStr, manifest=Mfst} = LocalCtx,
    Acl = Mfst?MANIFEST.acl,
    NewAcl = Acl?ACL{creation_time = now()},
    Metadata = riak_cs_wm_utils:extract_user_metadata(RD),
    case riak_cs_utils:set_object_acl(Bucket, list_to_binary(KeyStr),
                                      Mfst?MANIFEST{metadata=Metadata}, NewAcl,
                                      RiakcPid) of
        ok ->
            ETag = riak_cs_utils:etag_from_binary(Mfst?MANIFEST.content_md5),
            RD2 = wrq:set_resp_header("ETag", ETag, RD),
            ResponseMod:copy_object_response(Mfst, RD2, Ctx);
        {error, Err} ->
            ResponseMod:api_error(Err, RD, Ctx)
    end;
accept_body(RD, Ctx=#context{local_context=LocalCtx,
                             user=User,
                             riakc_pid=RiakcPid}) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 putctype=ContentType,
                 size=Size,
                 get_fsm_pid=GetFsmPid} = LocalCtx,
    BFile_str = [Bucket, $,, Key],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_object_entry(?MODULE, <<"object_put">>,
                                   [], [UserName, BFile_str]),
    riak_cs_get_fsm:stop(GetFsmPid),
    Metadata = riak_cs_wm_utils:extract_user_metadata(RD),
    BlockSize = riak_cs_lfs_utils:block_size(),

    %%ReadGrant = wrq:get_req_header("x-amz-grant-read", RD),
    %%_ = lager:error("The read grant is ~p", [ReadGrant]),

    %%Headers = wrq:req_headers(RD),
    %%_ = lager:error("All headers are: ~p", [Headers]),

    %% Check for `x-amz-acl' header to support
    %% non-default ACL at bucket creation time.
    ACL = riak_cs_acl_utils:canned_acl(
            wrq:get_req_header("x-amz-acl", RD),
            {User?RCS_USER.display_name,
             User?RCS_USER.canonical_id,
             User?RCS_USER.key_id},
            riak_cs_wm_utils:bucket_owner(Bucket, RiakcPid)),
    Args = [{Bucket, list_to_binary(Key), Size, list_to_binary(ContentType),
             Metadata, BlockSize, ACL, timer:seconds(60), self(), RiakcPid}],
    {ok, Pid} = riak_cs_put_fsm_sup:start_put_fsm(node(), Args),
    accept_streambody(RD, Ctx, Pid, wrq:stream_req_body(RD, riak_cs_lfs_utils:block_size())).

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

%% TODO:
%% We need to do some checking to make sure
%% the bucket exists for the user who is doing
%% this PUT
-spec finalize_request(#wm_reqdata{}, #context{}, pid()) -> {{halt, 200}, #wm_reqdata{}, #context{}}.
finalize_request(RD,
                 Ctx=#context{local_context=LocalCtx,
                              start_time=StartTime,
                              response_module=ResponseMod,
                              user=User},
                 Pid) ->
    #key_context{bucket=Bucket,
                 key=Key,
                 size=S} = LocalCtx,
    BFile_str = [Bucket, $,, Key],
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finalize_request">>, [S], [UserName, BFile_str]),
    ContentMD5 = wrq:get_req_header("content-md5", RD),
    Response =
        case riak_cs_put_fsm:finalize(Pid, ContentMD5) of
            {ok, Manifest} ->
                ETag = riak_cs_utils:etag_from_binary(Manifest?MANIFEST.content_md5),
                %% TODO: probably want something that counts actual bytes uploaded
                %% instead, to record partial/aborted uploads
                AccessRD = riak_cs_access_log_handler:set_bytes_in(S, RD),
                {{halt, 200}, wrq:set_resp_header("ETag", ETag, AccessRD), Ctx};
            {error, invalid_digest} ->
                ResponseMod:invalid_digest_response(ContentMD5, RD, Ctx);
            {error, Reason} ->
                ResponseMod:api_error(Reason, RD, Ctx)
        end,
    ok = riak_cs_stats:update_with_start(object_put, StartTime),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finalize_request">>, [S], [UserName, BFile_str]),
    riak_cs_dtrace:dt_object_return(?MODULE, <<"object_put">>, [S], [UserName, BFile_str]),
    Response.

check_0length_metadata_update(Length, RD, Ctx=#context{local_context=LocalCtx}) ->
    %% The authorize() callback has already been called, which means
    %% that ensure_doc() has been called, so the local context
    %% manifest is up-to-date: the object exists or it doesn't.
    case (not is_atom(LocalCtx#key_context.manifest) andalso
          zero_length_metadata_update_p(Length, RD)) of
        false ->
            UpdLocalCtx = LocalCtx#key_context{size=Length},
            {true, RD, Ctx#context{local_context=UpdLocalCtx}};
        true ->
            UpdLocalCtx = LocalCtx#key_context{size=Length,
                                               update_metadata=true},
            {true, RD, Ctx#context{local_context=UpdLocalCtx}}
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

-spec format_etag(binary() | {binary(), string()}) -> string().
format_etag({ContentMd5, Suffix}) ->
    riak_cs_utils:etag_from_binary(ContentMd5, Suffix);
format_etag(ContentMd5) ->
    riak_cs_utils:etag_from_binary(ContentMd5).
