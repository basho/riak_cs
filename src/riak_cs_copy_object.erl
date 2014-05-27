-module(riak_cs_copy_object).

-include("riak_cs.hrl").

%%API
-export([test_condition_and_permission/3,
         start_put_fsm/6, get_copy_source/1,
         copy/3, copy/4,
         copy_range/2]).

-include_lib("webmachine/include/webmachine.hrl").


%% @doc tests condition and permission on source object:
%% * x-amz-copy-source-if-* stuff
%% * bucket/object acl
%% * bucket policy
-spec test_condition_and_permission(lfs_manifest(), #wm_reqdata{}, #context{}) ->
                                           {boolean()|{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
test_condition_and_permission(SrcManifest, RD, Ctx) ->

    ContentMD5 = base64:encode_to_string(SrcManifest?MANIFEST.content_md5),
    LastUpdate = SrcManifest?MANIFEST.last_block_written_time,

    %% TODO: write tests around these conditions, any kind of test is okay
    case condition_check(RD, ContentMD5, LastUpdate) of
        ok ->
            _ = authorize_on_src(SrcManifest, RD, Ctx);
        Other ->
            {Other, RD, Ctx}
    end.

%% @doc tests permission on acl, policy
-spec authorize_on_src(lfs_manifest(), #wm_reqdata{}, #context{}) ->
                              {boolean()|{halt, non_neg_integer()}, #wm_reqdata{}, #context{}}.
authorize_on_src(SrcManifest, RD,
                 #context{auth_module=AuthMod,
                          local_context=LocalCtx,
                          riak_client=RcPid} = Ctx) ->
    {SrcBucket, SrcKey} = SrcManifest?MANIFEST.bkey,
    {ok, SrcBucketObj} = riak_cs_bucket:fetch_bucket_object(SrcBucket, RcPid),
    {ok, SrcBucketAcl} = riak_cs_acl:bucket_acl(SrcBucketObj),

    {UserKey, _} = AuthMod:identify(RD, Ctx),
    {User, UserObj} =
        case riak_cs_utils:get_user(UserKey, RcPid) of
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


-define(timeout, timer:minutes(5)).

%% @doc just kicks up put fsm
-spec start_put_fsm(binary(), binary(), lfs_manifest(),
                    proplists:proplist(), acl(), riak_client()) -> {ok, pid()}.
start_put_fsm(Bucket, Key, M, Metadata, Acl, RcPid) ->
    BlockSize = riak_cs_lfs_utils:block_size(),
    riak_cs_put_fsm_sup:start_put_fsm(node(),
                                      [{Bucket,
                                        Key,
                                        M?MANIFEST.content_length,
                                        M?MANIFEST.content_type,
                                        Metadata,
                                        BlockSize,
                                        Acl,
                                        ?timeout,
                                        self(),
                                        RcPid}]).


%% @doc check "x-amz-copy-source" to know whether it requests copying
%% from another object
-spec get_copy_source(#wm_reqdata{}) -> undefined | {binary(), binary()} |
    {error, atom()}.
get_copy_source(RD) ->
    %% for oos (TODO)
    %% case wrq:get_req_header("x-copy-from", RD) of
    %% for s3
    handle_copy_source(wrq:get_req_header("x-amz-copy-source", RD)).

handle_copy_source(undefined) ->
    undefined;
handle_copy_source([$/|Path]) ->
    handle_copy_source(Path);
handle_copy_source(Path0) when is_list(Path0) ->
    Path = mochiweb_util:unquote(Path0),

    Offset = string:chr(Path, $/),
    Bucket = string:substr(Path, 1, Offset-1),
    SplitKey = string:substr(Path, Offset+1),

    case SplitKey of
        [] ->
            {error, invalid_x_copy_from_path};
        _ ->
            {iolist_to_binary(Bucket),
             iolist_to_binary(SplitKey)}
    end.

%% @doc runs copy
-spec copy(pid(), lfs_manifest(), riak_client()) ->
                  {ok, DstManifest::lfs_manifest()} | {error, term()}.
copy(PutFsmPid, SrcManifest, ReadRcPid) ->
    copy(PutFsmPid, SrcManifest, ReadRcPid,
         {0, SrcManifest?MANIFEST.content_length-1}).

%% @doc runs copy, Target PUT FSM pid and source object manifest are
%% specified. This function kicks up GET FSM of source object and
%% streams blocks to specified PUT FSM.
-spec copy(pid(), lfs_manifest(), riak_client(),
           {non_neg_integer(), non_neg_integer()}|fail) ->
                  {ok, DstManifest::lfs_manifest()} | {error, term()}.
copy(_, _, _, fail) ->
    {error, bad_request};
copy(PutFsmPid, SrcManifest, ReadRcPid, Range) ->

    {ok, GetFsmPid} = start_get_fsm(SrcManifest, ReadRcPid),

    _RetrievedManifest = riak_cs_get_fsm:get_manifest(GetFsmPid),
    %% Then end is the index of the last byte, not the length, so subtract 1
    riak_cs_get_fsm:continue(GetFsmPid, Range),
    MD5 = get_content_md5(SrcManifest?MANIFEST.content_md5),
    get_and_put(GetFsmPid, PutFsmPid, MD5).

-spec get_content_md5(tuple() | binary()) -> string().
get_content_md5({_MD5, _Str}) ->
    undefined;
get_content_md5(MD5) ->
    base64:encode_to_string(MD5).

-spec get_and_put(pid(), pid(), list()) -> {ok, lfs_manifest()} | {error, term()}.
get_and_put(GetPid, PutPid, MD5) ->
    case riak_cs_get_fsm:get_next_chunk(GetPid) of
        {done, <<>>} ->
            riak_cs_get_fsm:stop(GetPid),
            riak_cs_put_fsm:finalize(PutPid, MD5);
        {chunk, Block} ->
            riak_cs_put_fsm:augment_data(PutPid, Block),
            get_and_put(GetPid, PutPid, MD5)
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


%% @doc retrieve "x-amx-copy-source-range"
-spec copy_range(#wm_reqdata{}, ?MANIFEST{}) -> {non_neg_integer(), non_neg_integer()} | fail.
copy_range(RD, ?MANIFEST{content_length=Len}) ->
    case wrq:get_req_header("x-amz-copy-source-range", RD) of
        undefined -> {0, Len-1};
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
