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

%% @doc Object copying Note that Riak CS does not verify checksum in case
%% of copy specified with range copy with "x-amz-copy-source-range". Nor
%% in the case where the source object was uploaded via multipart upload.

-module(riak_cs_copy_object).

-include("riak_cs.hrl").

-export([test_condition_and_permission/4,
         malformed_request/1,
         new_metadata/2,
         get_copy_source/1,
         start_put_fsm/7,
         copy/4, copy/5,
         copy_range/2]).

%% Do not use this
-export([connection_checker/1]).

-include_lib("webmachine/include/webmachine.hrl").

%% @doc tests condition and permission on source object:
%% * x-amz-copy-source-if-* stuff
%% * bucket/object acl
%% * bucket policy
-spec test_condition_and_permission(riak_client(), lfs_manifest(), #wm_reqdata{}, #context{}) ->
                                           {boolean()|{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
test_condition_and_permission(RcPid, SrcManifest, RD, Ctx) ->

    ETag = riak_cs_manifest:etag(SrcManifest),
    LastUpdate = SrcManifest?MANIFEST.created,

    %% TODO: write tests around these conditions, any kind of test is okay
    case condition_check(RD, ETag, LastUpdate) of
        ok ->
            authorize_on_src(RcPid, SrcManifest, RD, Ctx);
        Other ->
            {Other, RD, Ctx}
    end.

%% @doc tests permission on acl, policy
-spec authorize_on_src(riak_client(), lfs_manifest(), #wm_reqdata{}, #context{}) ->
                              {boolean()|{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize_on_src(RcPid, SrcManifest, RD,
                 #context{auth_module=AuthMod, local_context=LocalCtx} = Ctx) ->
    {SrcBucket, SrcKey} = SrcManifest?MANIFEST.bkey,
    {ok, SrcBucketObj} = riak_cs_bucket:fetch_bucket_object(SrcBucket, RcPid),
    {ok, SrcBucketAcl} = riak_cs_acl:bucket_acl(SrcBucketObj),

    {UserKey, _} = AuthMod:identify(RD, Ctx),
    {User, UserObj} =
        case riak_cs_user:get_user(UserKey, RcPid) of
            {ok, {User0, UserObj0}} ->
                {User0, UserObj0};
            {error, no_user_key} ->
                {undefined, undefined}
        end,

    _ = lager:debug("UserKey: ~p / ~p", [UserKey, User]),

    %% Build fake context for checking read access to the src object
    OtherLocalCtx = LocalCtx#key_context{bucket=SrcBucket,
                                         key=binary_to_list(SrcKey),
                                         bucket_object=SrcBucketObj,
                                         manifest=SrcManifest},
    OtherCtx = Ctx#context{local_context=OtherLocalCtx,
                           acl=SrcBucketAcl,
                           user=User, user_object=UserObj},

    %% Build fake ReqData for checking read access to the src object
    %% TODO: use unicode module for for key? or is this encoded?
    Path = string:join(["/buckets", binary_to_list(SrcBucket),
                        "objects", binary_to_list(SrcKey)], "/"),
    _ = lager:debug("raw path: ~p => ~p", [wrq:raw_path(RD), Path]),
    _ = lager:debug("src bucket obj: ~p", [SrcBucketObj]),
    OtherRD0 = wrq:create('GET', wrq:version(RD), Path, wrq:req_headers(RD)),
    OtherRD = wrq:load_dispatch_data([{bucket, binary_to_list(SrcBucket)},
                                      {object, binary_to_list(SrcKey)}],
                                     wrq:host_tokens(RD), wrq:port(RD), [],
                                     wrq:app_root(RD), wrq:disp_path(RD), OtherRD0),

    %% check the permission on ACL and Policy
    _ = riak_cs_wm_utils:object_access_authorize_helper(object, false,
                                                        OtherRD, OtherCtx).

-spec malformed_request(#wm_reqdata{}) ->
                               false |
                               {true, {invalid_argument, string(), string()}}.
malformed_request(RD) ->
    MDDirectiveKey = "x-amz-metadata-directive",
    case wrq:get_req_header(MDDirectiveKey, RD) of
        undefined -> false;
        "REPLACE" -> false;
        "COPY"    -> false;
        Value     -> {true, {invalid_argument, MDDirectiveKey, Value}}
    end.

%% @doc just kicks up put fsm
-spec new_metadata(lfs_manifest(), #wm_reqdata{}) -> {binary(), [{string(), string()}]}.
new_metadata(SrcManifest, RD) ->
    case wrq:get_req_header("x-amz-metadata-directive", RD) of
        "REPLACE" ->
            {case wrq:get_req_header("content-type", RD) of
                 undefined -> SrcManifest?MANIFEST.content_type;
                 ReqCT -> list_to_binary(ReqCT)
             end,
             riak_cs_wm_utils:extract_user_metadata(RD)};
        _ ->
            {SrcManifest?MANIFEST.content_type,
             orddict:to_list(SrcManifest?MANIFEST.metadata)}
    end.

-spec start_put_fsm(binary(), binary(), non_neg_integer(), binary(),
                    proplists:proplist(), acl(), riak_client()) -> {ok, pid()}.
start_put_fsm(Bucket, Key, ContentLength, ContentType, Metadata, Acl, RcPid) ->
    BlockSize = riak_cs_lfs_utils:block_size(),
    riak_cs_put_fsm_sup:start_put_fsm(node(),
                                      [{Bucket,
                                        Key,
                                        ContentLength,
                                        ContentType,
                                        Metadata,
                                        BlockSize,
                                        Acl,
                                        timer:seconds(60),
                                        self(),
                                        RcPid}]).


%% @doc check "x-amz-copy-source" to know whether it requests copying
%% from another object
-spec get_copy_source(#wm_reqdata{}) -> undefined |
                                        {binary(), binary()} |
                                        {error, atom()}.
get_copy_source(RD) ->
    %% for oos (TODO)
    %% case wrq:get_req_header("x-copy-from", RD) of
    %% for s3
    handle_copy_source(wrq:get_req_header("x-amz-copy-source", RD)).

handle_copy_source(undefined) ->
    undefined;
handle_copy_source([$/, $/ | _Path]) ->
    {error, bad_request};
handle_copy_source([$/|Path]) ->
    handle_copy_source(Path);
handle_copy_source(Path0) when is_list(Path0) ->
    Path = mochiweb_util:unquote(Path0),
    Length = length(Path),
    case string:chr(Path, $/) of
        0 ->
            {error, bad_request};
        Length ->
            {error, bad_request};
        Offset ->
            Bucket = string:substr(Path, 1, Offset-1),
            SplitKey = string:substr(Path, Offset+1),
            case SplitKey of
                [] ->
                    {error, bad_request};
                _ ->
                    {iolist_to_binary(Bucket),
                     iolist_to_binary(SplitKey)}
            end
    end.

%% @doc runs copy
-spec copy(pid(), lfs_manifest(), riak_client(), fun(() -> boolean())) ->
                  {ok, DstManifest::lfs_manifest()} | {error, term()}.
copy(PutFsmPid, SrcManifest, ReadRcPid, ContFun) ->
    copy(PutFsmPid, SrcManifest, ReadRcPid, ContFun, undefined).

%% @doc runs copy, Target PUT FSM pid and source object manifest are
%% specified. This function kicks up GET FSM of source object and
%% streams blocks to specified PUT FSM. ContFun is a function that
%% returns whether copying should be continued or not; Clients may
%% disconnect the TCP connection while waiting for copying large
%% objects. But mochiweb/webmachine cannot notify nor stop copying,
%% thus some fd watcher is expected here.
-spec copy(pid(), lfs_manifest(), riak_client(), fun(()->boolean()),
           {non_neg_integer(), non_neg_integer()}|fail|undefined) ->
                  {ok, DstManifest::lfs_manifest()} | {error, term()}.
copy(_, _, _, _, fail) ->
    {error, bad_request};
copy(PutFsmPid, ?MANIFEST{content_length=0} = _SrcManifest,
     _ReadRcPid, _, _) ->
    %% Zero-size copy will successfully create zero-size object
    riak_cs_put_fsm:finalize(PutFsmPid, undefined);
copy(PutFsmPid, SrcManifest, ReadRcPid, ContFun, Range0) ->
    {ok, GetFsmPid} = start_get_fsm(SrcManifest, ReadRcPid),
    _RetrievedManifest = riak_cs_get_fsm:get_manifest(GetFsmPid),

    {MD5, Range} =
        case Range0 of
            undefined ->
                %% normal PUT Copy; where whole MD5 will be checked
                %% before and after
                SrcMD5 = get_content_md5(SrcManifest?MANIFEST.content_md5),
                {SrcMD5, {0, SrcManifest?MANIFEST.content_length-1}};
            {_, _} ->
                %% Range specified PUT Copy; where no MD5 checksum
                %% will be checked: see riak_cs_put_fsm:is_digest_valid/2
                {undefined, Range0}
        end,

    riak_cs_get_fsm:continue(GetFsmPid, Range),
    get_and_put(GetFsmPid, PutFsmPid, MD5, ContFun).

-spec get_content_md5(tuple() | binary()) -> string().
get_content_md5({_MD5, _Str}) ->
    undefined;
get_content_md5(MD5) ->
    base64:encode_to_string(MD5).

-spec get_and_put(pid(), pid(), list(), fun(() -> boolean()))
                 -> {ok, lfs_manifest()} | {error, term()}.
get_and_put(GetPid, PutPid, MD5, ContFun) ->
    case ContFun() of
        true ->
            case riak_cs_get_fsm:get_next_chunk(GetPid) of
                {done, <<>>} ->
                    riak_cs_get_fsm:stop(GetPid),
                    riak_cs_put_fsm:finalize(PutPid, MD5);
                {chunk, Block} ->
                    riak_cs_put_fsm:augment_data(PutPid, Block),
                    get_and_put(GetPid, PutPid, MD5, ContFun)
            end;
        false ->
            _ = lager:debug("Connection lost during a copy", []),
            catch riak_cs_get_fsm:stop(GetPid),
            catch riak_cs_put_fsm:force_stop(PutPid),
            {error, connection_lost}
    end.

-spec start_get_fsm(lfs_manifest(), riak_client()) -> {ok, pid()}.
start_get_fsm(SrcManifest, ReadRcPid) ->
    {Bucket, Key} = SrcManifest?MANIFEST.bkey,
    FetchConcurrency = riak_cs_lfs_utils:fetch_concurrency(),
    BufferFactor = riak_cs_lfs_utils:get_fsm_buffer_size_factor(),
    riak_cs_get_fsm_sup:start_get_fsm(node(),
                                      Bucket,
                                      Key,
                                      self(),
                                      ReadRcPid,
                                      FetchConcurrency,
                                      BufferFactor).


-spec condition_check(#wm_reqdata{}, string(), string()) -> ok | {halt, 412}.
condition_check(RD, ETag, LastUpdate) ->
    %% x-amz-copy-source-if-match
    CopySourceIfMatch = wrq:get_req_header("x-amz-copy-source-if-match", RD),

    %% x-amz-copy-source-if-none-match
    CopySourceIfNoneMatch = wrq:get_req_header("x-amz-copy-source-if-none-match", RD),

    %% x-amz-copy-source-if-unmodified-since
    CopySourceIfUnmodifiedSince = wrq:get_req_header("x-amz-copy-source-if-unmodified-since", RD),

    %% x-amz-copy-source-if-modified-since
    CopySourceIfModifiedSince = wrq:get_req_header("x-amz-copy-source-if-modified-since", RD),

    %% TODO: do they work by AND or OR?
    %% TODO: validation of dates in HTTP manner
    case {CopySourceIfMatch, CopySourceIfUnmodifiedSince} of
        {undefined, undefined} ->
            case {CopySourceIfNoneMatch, CopySourceIfModifiedSince} of
                {undefined, undefined} ->
                    ok;
                {ETag, undefined} ->
                    {halt, 412};
                {_, CopySourceIfModifiedSince} when LastUpdate >= CopySourceIfModifiedSince ->
                    ok;
                _ ->
                    {halt, 412}
            end;
        {ETag, _} ->
            case {CopySourceIfNoneMatch, CopySourceIfModifiedSince} of
                {undefined, undefined} ->
                    ok;
                _ ->
                    {halt, 412}
            end;
        {_, CopySourceIfUnmodifiedSince} when LastUpdate =< CopySourceIfUnmodifiedSince ->
            case {CopySourceIfNoneMatch, CopySourceIfModifiedSince} of
                {undefined, undefined} ->
                    ok;
                _ ->
                    {halt, 412}
            end;
        {_, CopySourceIfUnmodifiedSince} ->
            {halt, 412}
    end.


%% @doc retrieve "x-amz-copy-source-range"
-spec copy_range(#wm_reqdata{}, ?MANIFEST{}) -> {non_neg_integer(), non_neg_integer()} | fail.
copy_range(RD, ?MANIFEST{content_length=Len}) ->
    case wrq:get_req_header("x-amz-copy-source-range", RD) of
        undefined ->
            %% Then end is the index of the last byte, not the length, so subtract 1
            {0, Len-1};
        Str ->
            case mochiweb_http:parse_range_request(Str) of
                %% Use only the first range
                [{none, End}|_]   -> {0, End};
                [{Start, none}|_] -> {Start, Len-1};
                [{Start, End}|_]  -> {Start, End};
                [] -> fail;
                fail -> fail
            end
    end.


%% @doc nasty hack, do not use this other than for disconnect
%% detection in copying objects.
-type mochiweb_socket() :: inet:socket() | {ssl, ssl:sslsocket()}.
-spec connection_checker(mochiweb_socket()) -> fun(() -> boolean()).
connection_checker(Socket) ->
    fun() ->
            case mochiweb_socket:peername(Socket) of
                {error,_E} ->
                    false;
                {ok,_} ->
                    case mochiweb_socket:recv(Socket, 0, 0) of
                        {error, timeout} ->
                            true;
                        {error, _E} ->
                            false;
                        {ok, _} ->
                            %% This cannot happen....
                            throw(copy_interrupted)
                    end
            end
    end.
