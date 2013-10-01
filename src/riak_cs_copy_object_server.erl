-module(riak_cs_copy_object_server).

-behaviour(gen_server).

-include("riak_cs.hrl").

%%API
-export([start_link/1, copy/1]).
    
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(timeout, timer:hours(1)).

-record(state, {copy_ctx :: #copy_ctx{},
                get_fsm_pid :: pid(),
                put_fsm_pid :: pid()}).

-spec start_link(#copy_ctx{}) -> {ok, pid()} | {error, term()}.
start_link(CopyCtx) ->
    gen_server:start_link(?MODULE, CopyCtx, []).

-spec copy(pid()) -> ok.
copy(CopyPid) ->
    gen_server:call(CopyPid, copy, ?timeout). 

init(CopyCtx) ->
    {ok, #state{copy_ctx=CopyCtx}}.

handle_call(copy, _From, State=#state{copy_ctx=CopyCtx}) ->
    {ok, GetFsmPid} = start_get_fsm(CopyCtx),
    {ok, PutFsmPid} = start_put_fsm(CopyCtx),
    Manifest = CopyCtx#copy_ctx.src_manifest,
    Result = get_and_put(GetFsmPid, PutFsmPid, Manifest?MANIFEST.content_md5),
    {reply, Result, State};
handle_call(Msg, From, State) ->
    lager:error("Invalid call: Msg ~p from ~p.~n", [Msg, From]),
    {reply, {error, badarg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec get_and_put(pid(), pid(), list()) -> ok | {error, term()}.
get_and_put(GetPid, PutPid, MD5) ->
    case riak_cs_get_fsm:get_next_chunk(GetPid) of
        {done, <<>>} ->
            riak_cs_put_fsm:finalize(PutPid, MD5),
            riak_cs_get_fsm:stop(GetPid),
            riak_cs_put_fsm:stop(GetPid),
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
                                      

