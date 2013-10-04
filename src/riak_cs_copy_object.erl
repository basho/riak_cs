-module(riak_cs_copy_object).

-include("riak_cs.hrl").

%%API
-export([copy/1]).
    
-define(timeout, timer:minutes(5)).

-spec copy(pid()) -> ok.
copy(CopyCtx) ->
    {ok, GetFsmPid} = start_get_fsm(CopyCtx),
    {ok, PutFsmPid} = start_put_fsm(CopyCtx),
    Manifest = CopyCtx#copy_ctx.src_manifest,
    get_and_put(GetFsmPid, PutFsmPid, Manifest?MANIFEST.content_md5).

-spec get_and_put(pid(), pid(), list()) -> ok | {error, term()}.
get_and_put(GetPid, PutPid, MD5) ->
    case riak_cs_get_fsm:get_next_chunk(GetPid) of
        {done, <<>>} ->
            riak_cs_put_fsm:finalize(PutPid, MD5),
            ok;
        {chunk, Block} ->
            riak_cs_put_fsm:augment_data(PutPid, Block),
            get_and_put(GetPid, PutPid, MD5)
    end.

-spec start_get_fsm(#copy_ctx{}) -> {ok, pid()}.
start_get_fsm(#copy_ctx{src_manifest=M}) ->
    {Bucket, Key} = M?MANIFEST.bkey,
    FetchConcurrency = riak_cs_lfs_utils:fetch_concurrency(),
    BufferFactor = riak_cs_lfs_utils:get_fsm_buffer_size_factor(),
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
    riak_cs_get_fsm_sup:start_get_fsm(node(),
                                      Bucket,
                                      Key,
                                      self(),
                                      RiakcPid,
                                      FetchConcurrency,
                                      BufferFactor).

-spec start_put_fsm(#copy_ctx{}) -> {ok, pid()}.
start_put_fsm(#copy_ctx{dst_acl=Acl, 
                        src_manifest=M, 
                        dst_metadata=Metadata,
                        dst_bucket=Bucket, 
                        dst_key=Key}) ->
    BlockSize = riak_cs_lfs_utils:block_size(),
    {ok, RiakcPid} = riak_cs_utils:riak_connection(),
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
