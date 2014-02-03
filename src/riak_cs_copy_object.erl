-module(riak_cs_copy_object).

-include("riak_cs.hrl").

%%API
-export([copy/1]).

%% TEST API
-export([test/5]).

-spec test(string(), binary(), binary(), binary(), binary()) -> ok.
test(KeyId, SrcBucket, SrcKey, DstBucket, DstKey) ->
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    {ok, {User, _}} = riak_cs_utils:get_user(KeyId, RiakcPid),
    {ok, _, Manifests} = riak_cs_utils:get_manifests(RiakcPid, SrcBucket, SrcKey),
    {ok, Manifest} = riak_cs_manifest_utils:active_manifest(orddict:from_list(Manifests)),
    Acl = ?ACL{owner={User?RCS_USER.display_name,
                      User?RCS_USER.canonical_id,
                      User?RCS_USER.key_id},
               grants=[{'AllUsers', ['READ']}],
               creation_time=os:timestamp()},
    CopyCtx = #copy_ctx{src_manifest=Manifest,
                        dst_bucket=DstBucket,
                        dst_key=DstKey,
                        dst_metadata=[],
                        dst_acl=Acl},
    ok = copy(CopyCtx),
    riak_cs_utils:close_riak_connection(RiakcPid).

-define(timeout, timer:minutes(5)).

-spec copy(pid()) -> ok.
copy(CopyCtx) ->
    {ok, GetRiakcPid} = riak_cs_utils:riak_connection(),
    {ok, PutRiakcPid} = riak_cs_utils:riak_connection(),
    {ok, GetFsmPid} = start_get_fsm(CopyCtx, GetRiakcPid),
    {ok, PutFsmPid} = start_put_fsm(CopyCtx, PutRiakcPid),
    Manifest = CopyCtx#copy_ctx.src_manifest,
    _RetrievedManifest = riak_cs_get_fsm:get_manifest(GetFsmPid),
    %% Then end is the index of the last byte, not the length, so subtract 1
    riak_cs_get_fsm:continue(GetFsmPid, {0, Manifest?MANIFEST.content_length-1}),
    MD5 = get_content_md5(Manifest?MANIFEST.content_md5),
    {ok, _Manifest} = get_and_put(GetFsmPid, PutFsmPid, MD5),
    riak_cs_utils:close_riak_connection(GetRiakcPid),
    riak_cs_utils:close_riak_connection(PutRiakcPid),
    ok.

-spec get_content_md5(tuple() | binary()) -> string().
get_content_md5({_MD5, _Str}) ->
    %%Suffix = list_to_binary(Str),
    %%<<MD5/binary, Suffix/binary>>,
    undefined;
get_content_md5(MD5) ->
    base64:encode(MD5).

-spec get_and_put(pid(), pid(), list()) -> ok | {error, term()}.
get_and_put(GetPid, PutPid, MD5) ->
    case riak_cs_get_fsm:get_next_chunk(GetPid) of
        {done, <<>>} ->
            riak_cs_put_fsm:finalize(PutPid, undefined);
      %%      riak_cs_put_fsm:finalize(PutPid, binary_to_list(MD5));
        {chunk, Block} ->
            riak_cs_put_fsm:augment_data(PutPid, Block),
            get_and_put(GetPid, PutPid, MD5)
    end.

-spec start_get_fsm(#copy_ctx{}, pid()) -> {ok, pid()}.
start_get_fsm(#copy_ctx{src_manifest=M}, RiakcPid) ->
    {Bucket, Key} = M?MANIFEST.bkey,
    FetchConcurrency = riak_cs_lfs_utils:fetch_concurrency(),
    BufferFactor = riak_cs_lfs_utils:get_fsm_buffer_size_factor(),
    riak_cs_get_fsm_sup:start_get_fsm(node(),
                                      Bucket,
                                      Key,
                                      self(),
                                      RiakcPid,
                                      FetchConcurrency,
                                      BufferFactor).

-spec start_put_fsm(#copy_ctx{}, pid()) -> {ok, pid()}.
start_put_fsm(#copy_ctx{dst_acl=Acl,
                        src_manifest=M,
                        dst_metadata=Metadata,
                        dst_bucket=Bucket,
                        dst_key=Key}, RiakcPid) ->
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
                                        RiakcPid}]).
