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

%% @doc Module to manage storage of objects and files

-module(riak_cs_put_fsm).

-behaviour(gen_fsm).

-include("riak_cs.hrl").

%% API
-export([start_link/1, start_link/2,
         get_uuid/1,
         augment_data/2,
         block_written/2,
         finalize/1,
         force_stop/1]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         prepare/3,
         not_full/2,
         full/2,
         all_received/2,
         not_full/3,
         all_received/3,
         done/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(SERVER, ?MODULE).
-define(EMPTYORDSET, ordsets:new()).
-define(MD5_CHUNK_SIZE, 64*1024).

-record(state, {timeout :: timeout(),
                block_size :: pos_integer(),
                caller :: reference(),
                uuid :: binary(),
                md5 :: binary(),
                reply_pid :: {pid(), reference()},
                mani_pid :: undefined | pid(),
                riakc_pid :: pid(),
                make_new_manifest_p :: boolean(),
                timer_ref :: reference(),
                bucket :: binary(),
                key :: binary(),
                metadata :: term(),
                acl :: acl(),
                manifest :: lfs_manifest(),
                content_length :: non_neg_integer(),
                content_type :: binary(),
                num_bytes_received=0 :: non_neg_integer(),
                max_buffer_size :: non_neg_integer(),
                current_buffer_size=0 :: non_neg_integer(),
                buffer_queue=[] :: [binary()], %% not actually a queue, but we treat it like one
                remainder_data :: undefined | binary(),
                free_writers :: undefined | ordsets:ordset(pid()),
                unacked_writes=ordsets:new() :: ordsets:ordset(non_neg_integer()),
                next_block_id=0 :: non_neg_integer(),
                all_writer_pids :: undefined | list(pid()),
                md5_chunk_size :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link({binary(), binary(), non_neg_integer(), binary(),
                  term(), pos_integer(), acl(), timeout(), pid(), pid()}) ->
                        {ok, pid()} | {error, term()}.
start_link(Tuple) when is_tuple(Tuple) ->
    start_link(Tuple, true).

-spec start_link({binary(), binary(), non_neg_integer(), binary(),
                  term(), pos_integer(), acl(), timeout(), pid(), pid()},
                 boolean()) ->
                        {ok, pid()} | {error, term()}.
start_link({_Bucket,
            _Key,
            _ContentLength,
            _ContentType,
            _Metadata,
            _BlockSize,
            _Acl,
            _Timeout,
            _Caller,
            _RiakPid}=Arg1,
           MakeNewManifestP) ->
    gen_fsm:start_link(?MODULE, {Arg1, MakeNewManifestP}, []).

get_uuid(Pid) ->
    gen_fsm:sync_send_event(Pid, {get_uuid}, infinity).

augment_data(Pid, Data) ->
    gen_fsm:sync_send_event(Pid, {augment_data, Data}, infinity).

finalize(Pid) ->
    gen_fsm:sync_send_event(Pid, finalize, infinity).

force_stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, force_stop, infinity).

block_written(Pid, BlockID) ->
    gen_fsm:send_event(Pid, {block_written, BlockID, self()}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------

%% TODO:
%% Metadata support is a future feature,
%% but I'm just stubbing it in here
%% so that I can be thinking about how it
%% might be implemented. Does it actually
%% make things more confusing?
-spec init({{binary(), binary(), non_neg_integer(), binary(),
             term(), pos_integer(), acl(), timeout(), pid(), pid()},
            term()}) ->
                  {ok, prepare, #state{}, timeout()}.
init({{Bucket, Key, ContentLength, ContentType,
       Metadata, BlockSize, Acl, Timeout, Caller, RiakPid},
      MakeNewManifestP}) ->
    %% We need to do this (the monitor) for two reasons
    %% 1. We're started through a supervisor, so the
    %%    proc that actually intends to start us isn't
    %%    linked to us.
    %% 2. Even if we didn't use a supervisor, the webmachine
    %%    process uses exit(..., normal), even on abnormal
    %%    terminations, so this process would still
    %%    live.
    CallerRef = erlang:monitor(process, Caller),

    UUID = druuid:v4(),
    {ok, prepare, #state{bucket=Bucket,
                         key=Key,
                         block_size=BlockSize,
                         caller=CallerRef,
                         uuid=UUID,
                         metadata=Metadata,
                         acl=Acl,
                         content_length=ContentLength,
                         content_type=ContentType,
                         riakc_pid=RiakPid,
                         make_new_manifest_p=MakeNewManifestP,
                         timeout=Timeout,
                         md5_chunk_size=riak_cs_config:md5_chunk_size()},
     0}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
prepare(timeout, State=#state{content_length=0}) ->
    NewState = prepare(State),
    Md5 = crypto:md5_final(NewState#state.md5),
    NewManifest = NewState#state.manifest?MANIFEST{content_md5=Md5,
                                                   state=active,
                                                   last_block_written_time=os:timestamp()},
    {next_state, done, NewState#state{md5=Md5, manifest=NewManifest}};
prepare(timeout, State) ->
    NewState = prepare(State),
    {next_state, not_full, NewState}.

prepare(finalize, From, State=#state{content_length=0}) ->
    NewState = prepare(State),
    Md5 = crypto:md5_final(NewState#state.md5),
    NewManifest = NewState#state.manifest?MANIFEST{content_md5=Md5,
                                                   state=active,
                                                   last_block_written_time=os:timestamp()},
    done(finalize, From, NewState#state{md5=Md5, manifest=NewManifest});

prepare({get_uuid}, _From, State) ->
    {reply, State#state.uuid, prepare, State};

prepare({augment_data, NewData}, From, State) ->
    #state{content_length=CLength,
           num_bytes_received=NumBytesReceived,
           current_buffer_size=CurrentBufferSize,
           max_buffer_size=MaxBufferSize} = NewState = prepare(State),
    case handle_chunk(CLength, NumBytesReceived, size(NewData),
                      CurrentBufferSize, MaxBufferSize) of
        accept ->
            handle_accept_chunk(NewData, NewState);
        backpressure ->
            handle_backpressure_for_chunk(NewData, From, NewState);
        last_chunk ->
            handle_receiving_last_chunk(NewData, NewState)
    end.

%% when a block is written
%% and we were already not full,
%% we're still not full
not_full({block_written, BlockID, WriterPid}, State) ->
    NewState = state_from_block_written(BlockID, WriterPid, State),
    {next_state, not_full, NewState}.

full({block_written, BlockID, WriterPid}, State=#state{reply_pid=Waiter}) ->
    NewState = state_from_block_written(BlockID, WriterPid, State),
    gen_fsm:reply(Waiter, ok),
    {next_state, not_full, NewState#state{reply_pid=undefined}}.

all_received({augment_data, <<>>}, State) ->
    {next_state, all_received, State};
all_received({block_written, BlockID, WriterPid}, State=#state{mani_pid=ManiPid,
                                                               timer_ref=TimerRef}) ->

    NewState = state_from_block_written(BlockID, WriterPid, State),
    Manifest = NewState#state.manifest?MANIFEST{state=active},
    case ordsets:size(NewState#state.unacked_writes) of
        0 ->
            case State#state.reply_pid of
                undefined ->
                    {next_state, done, NewState};
                ReplyPid ->
                    %% reply with the final manifest
                    _ = erlang:cancel_timer(TimerRef),
                    case maybe_update_manifest_with_confirmation(ManiPid, Manifest) of
                        ok ->
                            gen_fsm:reply(ReplyPid, {ok, Manifest}),
                            {stop, normal, NewState};
                        Error ->
                            gen_fsm:reply(ReplyPid, {error, Error}),
                            {stop, Error, NewState}
                    end
            end;
        _ ->
            {next_state, all_received, NewState}
    end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------

%% Note, we never receive the `augment_data`
%% event in the full state because there should
%% only ever be a singleton proc sending the event,
%% and it will be blocked waiting for a response from
%% when we transitioned from not_full => full

not_full({get_uuid}, _From, State) ->
    {reply, State#state.uuid, not_full, State};

not_full({augment_data, NewData}, From,
         State=#state{content_length=CLength,
                      num_bytes_received=NumBytesReceived,
                      current_buffer_size=CurrentBufferSize,
                      max_buffer_size=MaxBufferSize}) ->

    case handle_chunk(CLength, NumBytesReceived, size(NewData),
                      CurrentBufferSize, MaxBufferSize) of
        accept ->
            handle_accept_chunk(NewData, State);
        backpressure ->
            handle_backpressure_for_chunk(NewData, From, State);
        last_chunk ->
            handle_receiving_last_chunk(NewData, State)
    end.

all_received({augment_data, <<>>}, _From, State) ->
    {next_state, all_received, State};
all_received(finalize, From, State) ->
    %% 1. stash the From pid into our
    %%    state so that we know to reply
    %%    later with the finished manifest
    {next_state, all_received, State#state{reply_pid=From}}.

done(finalize, _From, State=#state{manifest=Manifest,
                                   mani_pid=ManiPid,
                                   timer_ref=TimerRef}) ->
    %% 1. reply immediately
    %%    with the finished manifest
    _ = erlang:cancel_timer(TimerRef),
    case maybe_update_manifest_with_confirmation(ManiPid, Manifest) of
        ok ->
            {stop, normal, {ok, Manifest}, State};
        Error ->
            {stop, Error, {error, Error}, State}
    end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_sync_event(current_state, _From, StateName, State) ->
    Reply = {StateName, State},
    {reply, Reply, StateName, State};
handle_sync_event(force_stop, _From, _StateName, State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(save_manifest, StateName, State=#state{mani_pid=ManiPid,
                                                   manifest=Manifest}) ->
    %% 1. save the manifest
    maybe_update_manifest(ManiPid, Manifest),
    TRef = erlang:send_after(60000, self(), save_manifest),
    {next_state, StateName, State#state{timer_ref=TRef}};
%% TODO:
%% add a clause for handling down
%% messages from the blocks gen_servers
handle_info({'DOWN', CallerRef, process, _Pid, Reason}, _StateName, State=#state{caller=CallerRef}) ->
    {stop, Reason, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, #state{mani_pid=ManiPid,
                                      all_writer_pids=BlockServerPids}) ->
    riak_cs_manifest_fsm:maybe_stop_manifest_fsm(ManiPid),
    riak_cs_block_server:maybe_stop_block_servers(BlockServerPids),
    ok.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Handle expensive initialization operations required for the put_fsm.
-spec prepare(#state{}) -> #state{}.
prepare(State=#state{bucket=Bucket,
                     key=Key,
                     block_size=BlockSize,
                     uuid=UUID,
                     content_length=ContentLength,
                     content_type=ContentType,
                     metadata=Metadata,
                     acl=Acl,
                     riakc_pid=RiakPid,
                     make_new_manifest_p=MakeNewManifestP})
  when is_integer(ContentLength), ContentLength >= 0 ->
    %% 1. start the manifest_fsm proc
    {ok, ManiPid} = maybe_riak_cs_manifest_fsm_start_link(
                      MakeNewManifestP, Bucket, Key, RiakPid),
    %% TODO:
    %% this shouldn't be hardcoded.
    Md5 = crypto:md5_init(),
    WriterPids = case ContentLength of
                     0 ->
                         %% Don't start any writers
                         %% if we're not going to need
                         %% to write any blocks
                         [];
                     _ ->
                         riak_cs_block_server:start_block_servers(RiakPid,
                                                                    riak_cs_lfs_utils:put_concurrency())
                 end,
    FreeWriters = ordsets:from_list(WriterPids),
    MaxBufferSize = (riak_cs_lfs_utils:put_fsm_buffer_size_factor() * BlockSize),

    %% for now, always populate cluster_id
    ClusterID = riak_cs_config:cluster_id(RiakPid),
    Manifest =
        riak_cs_lfs_utils:new_manifest(Bucket,
                                         Key,
                                         UUID,
                                         ContentLength,
                                         ContentType,
                                         %% we don't know the md5 yet
                                         undefined,
                                         Metadata,
                                         BlockSize,
                                         Acl,
                                         [],
                                         ClusterID),
    NewManifest = Manifest?MANIFEST{write_start_time=os:timestamp()},

    %% TODO:
    %% this time probably
    %% shouldn't be hardcoded,
    %% and if it is, what should
    %% it be?
    TRef = erlang:send_after(60000, self(), save_manifest),
    ok = maybe_add_new_manifest(ManiPid, NewManifest),
    State#state{manifest=NewManifest,
                md5=Md5,
                timer_ref=TRef,
                mani_pid=ManiPid,
                max_buffer_size=MaxBufferSize,
                all_writer_pids=WriterPids,
                free_writers=FreeWriters}.

handle_chunk(ContentLength, NumBytesReceived, NewDataSize, CurrentBufferSize, MaxBufferSize) ->
    if
        (NumBytesReceived + NewDataSize) == ContentLength ->
            last_chunk;
        (CurrentBufferSize + NewDataSize) >= MaxBufferSize ->
            backpressure;
        true ->
            accept
    end.

combine_new_and_remainder_data(NewData, undefined) ->
    NewData;
combine_new_and_remainder_data(NewData, Remainder) ->
    <<Remainder/binary, NewData/binary>>.

%% @private
%% @doc Break up a data binary into a list of block-sized chunks
-spec data_blocks(binary(),
                  pos_integer(),
                  non_neg_integer(),
                  pos_integer(),
                  [binary()]) ->
                         {[binary()], undefined | binary()}.
data_blocks(<<>>, _, _, _, Blocks) ->
    {Blocks, undefined};
data_blocks(Data, ContentLength, BytesReceived, BlockSize, Blocks) ->
    if
        byte_size(Data) >= BlockSize ->
            <<BlockData:BlockSize/binary, RestData/binary>> = Data,
            data_blocks(RestData,
                        ContentLength,
                        BytesReceived,
                        BlockSize,
                        append_data_block(BlockData, Blocks));
        ContentLength == BytesReceived ->
            data_blocks(<<>>,
                        ContentLength,
                        BytesReceived,
                        BlockSize,
                        append_data_block(Data, Blocks));
        true ->
            {Blocks, Data}
    end.

%% @private
%% @doc Append a data block to an list of data blocks.
-spec append_data_block(binary(), [binary()]) -> [binary()].
append_data_block(BlockData, Blocks) ->
    lists:reverse([BlockData | lists:reverse(Blocks)]).

%% @private
%% @doc Maybe send a message to one of the
%%      gen_servers to write another block,
%%      and then return the updated func inputs
-spec maybe_write_blocks(term()) -> term().
maybe_write_blocks(State=#state{buffer_queue=[]}) ->
    State;
maybe_write_blocks(State=#state{free_writers=[]}) ->
    State;
maybe_write_blocks(State=#state{buffer_queue=[ToWrite | RestBuffer],
                                free_writers=FreeWriters,
                                unacked_writes=UnackedWrites,
                                bucket=Bucket,
                                key=Key,
                                manifest=Manifest,
                                next_block_id=NextBlockID}) ->

    UUID = Manifest?MANIFEST.uuid,
    WriterPid = hd(ordsets:to_list(FreeWriters)),
    NewFreeWriters = ordsets:del_element(WriterPid, FreeWriters),
    NewUnackedWrites = ordsets:add_element(NextBlockID, UnackedWrites),

    riak_cs_block_server:put_block(WriterPid, Bucket, Key, UUID, NextBlockID, ToWrite),

    maybe_write_blocks(State#state{buffer_queue=RestBuffer,
                                   free_writers=NewFreeWriters,
                                   unacked_writes=NewUnackedWrites,
                                   next_block_id=(NextBlockID + 1)}).

-spec state_from_block_written(non_neg_integer(), pid(), term()) -> term().
state_from_block_written(BlockID, WriterPid, State=#state{unacked_writes=UnackedWrites,
                                                          block_size=BlockSize,
                                                          free_writers=FreeWriters,
                                                          current_buffer_size=CurrentBufferSize,
                                                          manifest=Manifest}) ->

    NewManifest = riak_cs_lfs_utils:remove_write_block(Manifest, BlockID),
    NewUnackedWrites = ordsets:del_element(BlockID, UnackedWrites),
    NewFreeWriters = ordsets:add_element(WriterPid, FreeWriters),

    %% After the last chunk is written,
    %% there's a chance this could go negative
    %% if the last block isn't a full
    %% block size, so don't bring it below 0
    NewCurrentBufferSize = max(0, CurrentBufferSize - BlockSize),

    maybe_write_blocks(State#state{manifest=NewManifest,
                                   unacked_writes=NewUnackedWrites,
                                   current_buffer_size=NewCurrentBufferSize,
                                   free_writers=NewFreeWriters}).

handle_accept_chunk(NewData, State=#state{buffer_queue=BufferQueue,
                                          remainder_data=RemainderData,
                                          block_size=BlockSize,
                                          num_bytes_received=PreviousBytesReceived,
                                          md5=Md5,
                                          current_buffer_size=CurrentBufferSize,
                                          content_length=ContentLength,
                                          md5_chunk_size=Md5ChunkSize}) ->

    NewMd5 = riak_cs_utils:chunked_md5(NewData, Md5, Md5ChunkSize),
    NewRemainderData = combine_new_and_remainder_data(NewData, RemainderData),
    UpdatedBytesReceived = PreviousBytesReceived + size(NewData),
    if UpdatedBytesReceived > ContentLength ->
           exit({too_many_bytes_received, got, UpdatedBytesReceived,
                 content_length, ContentLength});
       true ->
           ok
    end,
    NewCurrentBufferSize = CurrentBufferSize + size(NewData),

    {NewBufferQueue, NewRemainderData2} =
        data_blocks(NewRemainderData, ContentLength, UpdatedBytesReceived, BlockSize, BufferQueue),

    NewStateData = maybe_write_blocks(State#state{buffer_queue=NewBufferQueue,
                                                  remainder_data=NewRemainderData2,
                                                  md5=NewMd5,
                                                  current_buffer_size=NewCurrentBufferSize,
                                                  num_bytes_received=UpdatedBytesReceived}),
    Reply = ok,
    {reply, Reply, not_full, NewStateData}.

%% pattern match on reply_pid=undefined as a sort of
%% assert
handle_backpressure_for_chunk(NewData, From, State=#state{reply_pid=undefined,
                                                          buffer_queue=BufferQueue,
                                                          md5=Md5,
                                                          block_size=BlockSize,
                                                          remainder_data=RemainderData,
                                                          current_buffer_size=CurrentBufferSize,
                                                          num_bytes_received=PreviousBytesReceived,
                                                          content_length=ContentLength,
                                                          md5_chunk_size=Md5ChunkSize}) ->

    NewMd5 = riak_cs_utils:chunked_md5(NewData, Md5, Md5ChunkSize),
    NewRemainderData = combine_new_and_remainder_data(NewData, RemainderData),
    UpdatedBytesReceived = PreviousBytesReceived + size(NewData),

    NewCurrentBufferSize = CurrentBufferSize + size(NewData),

    {NewBufferQueue, NewRemainderData2} =
        data_blocks(NewRemainderData, ContentLength, UpdatedBytesReceived, BlockSize, BufferQueue),

    NewStateData = maybe_write_blocks(State#state{buffer_queue=NewBufferQueue,
                                                  remainder_data=NewRemainderData2,
                                                  md5=NewMd5,
                                                  current_buffer_size=NewCurrentBufferSize,
                                                  num_bytes_received=UpdatedBytesReceived}),

    {next_state, full, NewStateData#state{reply_pid=From}}.

handle_receiving_last_chunk(NewData, State=#state{buffer_queue=BufferQueue,
                                                  remainder_data=RemainderData,
                                                  md5=Md5,
                                                  block_size=BlockSize,
                                                  manifest=Manifest,
                                                  current_buffer_size=CurrentBufferSize,
                                                  num_bytes_received=PreviousBytesReceived,
                                                  content_length=ContentLength,
                                                  md5_chunk_size=Md5ChunkSize}) ->

    NewMd5 = crypto:md5_final(riak_cs_utils:chunked_md5(NewData, Md5, Md5ChunkSize)),
    NewManifest = Manifest?MANIFEST{content_md5=NewMd5},
    NewRemainderData = combine_new_and_remainder_data(NewData, RemainderData),
    UpdatedBytesReceived = PreviousBytesReceived + size(NewData),

    NewCurrentBufferSize = CurrentBufferSize + size(NewData),

    {NewBufferQueue, NewRemainderData2} =
        data_blocks(NewRemainderData, ContentLength, UpdatedBytesReceived, BlockSize, BufferQueue),

    NewStateData = maybe_write_blocks(State#state{buffer_queue=NewBufferQueue,
                                                  remainder_data=NewRemainderData2,
                                                  md5=NewMd5,
                                                  manifest=NewManifest,
                                                  current_buffer_size=NewCurrentBufferSize,
                                                  num_bytes_received=UpdatedBytesReceived}),

    Reply = ok,
    {reply, Reply, all_received, NewStateData}.

maybe_riak_cs_manifest_fsm_start_link(false, _Bucket, _Key, _RiakPid) ->
    {ok, undefined};
maybe_riak_cs_manifest_fsm_start_link(true, Bucket, Key, RiakPid) ->
    riak_cs_manifest_fsm:start_link(Bucket, Key, RiakPid).

maybe_add_new_manifest(undefined, _NewManifest) ->
    ok;
maybe_add_new_manifest(ManiPid, NewManifest) ->
    riak_cs_manifest_fsm:add_new_manifest(ManiPid, NewManifest).

maybe_update_manifest(undefined, _Manifest) ->
    ok;
maybe_update_manifest(ManiPid, Manifest) ->
    riak_cs_manifest_fsm:update_manifest(ManiPid, Manifest).

maybe_update_manifest_with_confirmation(undefined, _Manifest) ->
    ok;
maybe_update_manifest_with_confirmation(ManiPid, Manifest) ->
    riak_cs_manifest_fsm:update_manifest_with_confirmation(ManiPid, Manifest).
