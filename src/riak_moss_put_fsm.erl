%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_put_fsm).

-behaviour(gen_fsm).

-include("riak_moss.hrl").

%% API
-export([start_link/6,
         augment_data/2,
         block_written/2,
         finalize/1]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
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

-record(state, {timeout :: pos_integer(),
                reply_pid :: pid(),
                mani_pid :: pid(),
                timer_ref :: term(),
                bucket :: binary(),
                key :: binary(),
                metadata :: term(),
                manifest :: lfs_manifest(),
                content_length :: pos_integer(),
                content_type :: binary(),
                num_bytes_received=0,
                max_buffer_size :: non_neg_integer(),
                current_buffer_size=0,
                buffer_queue=[], %% not actually a queue, but we treat it like one
                remainder_data :: undefined | binary(),
                free_writers :: ordsets:new(),
                unacked_writes=ordsets:new(),
                next_block_id=0,
                all_writer_pids :: list(pid())}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Bucket, Key, ContentLength, ContentType, Metadata, Timeout) ->
    Args = [Bucket, Key, ContentLength, ContentType, Metadata, Timeout],
    gen_fsm:start_link(?MODULE, Args, []).

augment_data(Pid, Data) ->
    gen_fsm:sync_send_event(Pid, {augment_data, Data}).

finalize(Pid) ->
    gen_fsm:sync_send_event(Pid, finalize).

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
init([Bucket, Key, ContentLength, ContentType, Metadata, Timeout]) ->
    {ok, prepare, #state{bucket=Bucket,
                         key=Key,
                         metadata=Metadata,
                         content_length=ContentLength,
                         content_type=ContentType,
                         timeout=Timeout},
                     0}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
prepare(timeout, State=#state{bucket=Bucket,
                              key=Key,
                              content_length=ContentLength,
                              content_type=ContentType,
                              metadata=Metadata}) ->

    %% 1. start the manifest_fsm proc
    {ok, ManiPid} = riak_moss_manifest_fsm:start_link(Bucket, Key),
    %% TODO:
    %% this shouldn't be hardcoded.
    %% Also, use poolboy :)
    WriterPids = start_writer_servers(1),
    FreeWriters = ordsets:from_list(WriterPids),
    MaxBufferSize = riak_moss_lfs_utils:block_size(),
    UUID = druuid:v4(),
    Manifest =
    riak_moss_lfs_utils:new_manifest(Bucket,
                                     Key,
                                     UUID,
                                     ContentLength,
                                     ContentType,
                                     %% we don't know the md5 yet
                                     undefined, 
                                     Metadata),
    NewManifest = Manifest#lfs_manifest_v2{write_start_time=erlang:now()},

    %% TODO:
    %% this time probably
    %% shouldn't be hardcoded,
    %% and if it is, what should
    %% it be?
    {ok, TRef} = timer:send_interval(60000, self(), save_manifest),
    riak_moss_manifest_fsm:add_new_manifest(ManiPid, NewManifest),
    {next_state, not_full, State#state{manifest=NewManifest,
                                       timer_ref=TRef,
                                       mani_pid=ManiPid,
                                       max_buffer_size=MaxBufferSize,
                                       all_writer_pids=WriterPids,
                                       free_writers=FreeWriters}}.

%% when a block is written
%% and we were already not full,
%% we're still not full
not_full({block_written, BlockID, WriterPid}, State) ->
    NewState = state_from_block_written(BlockID, WriterPid, State),
    {next_state, not_full, NewState}.

full({block_written, BlockID, WriterPid}, State=#state{reply_pid=Waiter}) ->
    NewState = state_from_block_written(BlockID, WriterPid, State),
    gen_fsm:reply(Waiter, ok),
    {next_state, not_full, NewState}.

all_received({block_written, BlockID, WriterPid}, State=#state{mani_pid=ManiPid}) ->

    NewState = state_from_block_written(BlockID, WriterPid, State),
    Manifest = NewState#state.manifest,
    case ordsets:size(NewState#state.unacked_writes) of
        0 ->
            case State#state.reply_pid of
                undefined ->
                    {next_state, done, NewState};
                ReplyPid ->
                    %% reply with the final
                    %% manifest
                    %% TODO:
                    %% The reply shouldn't
                    %% be 'ok', it should
                    %% be the final manifest
                    case riak_moss_manifest_fsm:update_manifest_with_confirmation(ManiPid, Manifest) of
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

all_received(finalize, From, State) ->
    %% 1. stash the From pid into our
    %%    state so that we know to reply
    %%    later with the finished manifest
    {next_state, all_received, State#state{reply_pid=From}}.

done(finalize, _From, State=#state{manifest=Manifest, mani_pid=ManiPid}) ->
    %% 1. reply immediately
    %%    with the finished manifest
    case riak_moss_manifest_fsm:update_manifest_with_confirmation(ManiPid, Manifest) of
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
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(save_manifest, StateName, State=#state{mani_pid=ManiPid,
                                                   manifest=Manifest}) ->
    %% 1. save the manifest

    %% TODO:
    %% are there any times where
    %% we should be cancelling the
    %% timer here, depending on the
    %% state we're in?
    riak_moss_manifest_fsm:update_manifest(ManiPid, Manifest),
    {next_state, StateName, State};
%% TODO:
%% add a clause for handling down
%% messages from the blocks gen_servers
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    %% TODO:
    %% need to stop the manifest fsm
    %% and the blocks gen_servers
    ok.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
-spec data_blocks(binary(), pos_integer(), non_neg_integer(), [binary()]) ->
                         {[binary()], undefined | binary()}.
data_blocks(Data, ContentLength, BytesReceived, Blocks) ->
    data_blocks(Data,
                ContentLength,
                BytesReceived,
                riak_moss_lfs_utils:block_size(),
                Blocks).

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

    UUID = Manifest#lfs_manifest_v2.uuid,
    WriterPid = hd(ordsets:to_list(FreeWriters)),
    NewFreeWriters = ordsets:del_element(WriterPid, FreeWriters),
    NewUnackedWrites = ordsets:add_element(NextBlockID, UnackedWrites),

    riak_moss_block_server:put_block(WriterPid, Bucket, Key, UUID, NextBlockID, ToWrite),

    maybe_write_blocks(State#state{buffer_queue=RestBuffer,
                                   free_writers=NewFreeWriters,
                                   unacked_writes=NewUnackedWrites,
                                   next_block_id=(NextBlockID + 1)}).

-spec state_from_block_written(non_neg_integer(), pid(), term()) -> term().
state_from_block_written(BlockID, WriterPid, State=#state{unacked_writes=UnackedWrites,
                                                          free_writers=FreeWriters,
                                                          manifest=Manifest}) ->
    NewManifest = riak_moss_lfs_utils:remove_write_block(Manifest, BlockID),
    NewUnackedWrites = ordsets:del_element(BlockID, UnackedWrites),
    NewFreeWriters = ordsets:add_element(WriterPid, FreeWriters),

    maybe_write_blocks(State#state{manifest=NewManifest,
                                   unacked_writes=NewUnackedWrites,
                                   free_writers=NewFreeWriters}).

%% @private
%% @doc Start a number
%%      of riak_moss_block_server
%%      processes and return a list
%%      of their pids
-spec start_writer_servers(pos_integer()) -> list(pid()).
start_writer_servers(NumServers) ->
    %% TODO:
    %% doesn't handle
    %% failure at all
    [Pid || {ok, Pid} <- 
        [riak_moss_block_server:start_link() ||
            _ <- lists:seq(1, NumServers)]].

handle_accept_chunk(NewData, State=#state{buffer_queue=BufferQueue,
                                          remainder_data=RemainderData,
                                          num_bytes_received=PreviousBytesReceived,
                                          content_length=ContentLength}) ->

    NewRemainderData = combine_new_and_remainder_data(NewData, RemainderData),
    UpdatedBytesReceived = PreviousBytesReceived + size(NewData),

    {NewBufferQueue, NewRemainderData2} =
    data_blocks(NewRemainderData, ContentLength, UpdatedBytesReceived, BufferQueue),

    NewStateData = maybe_write_blocks(State#state{buffer_queue=NewBufferQueue,
                                                  remainder_data=NewRemainderData2,
                                                  num_bytes_received=UpdatedBytesReceived}),
    Reply = ok,
    {reply, Reply, not_full, NewStateData}.

%% pattern match on reply_pid=undefined as a sort of
%% assert
handle_backpressure_for_chunk(NewData, From, State=#state{reply_pid=undefined,
                                                          buffer_queue=BufferQueue,
                                                          remainder_data=RemainderData,
                                                          num_bytes_received=PreviousBytesReceived,
                                                          content_length=ContentLength}) ->

    NewRemainderData = combine_new_and_remainder_data(NewData, RemainderData),
    UpdatedBytesReceived = PreviousBytesReceived + size(NewData),

    {NewBufferQueue, NewRemainderData2} =
    data_blocks(NewRemainderData, ContentLength, UpdatedBytesReceived, BufferQueue),

    NewStateData = maybe_write_blocks(State#state{buffer_queue=NewBufferQueue,
                                                  remainder_data=NewRemainderData2,
                                                  num_bytes_received=UpdatedBytesReceived}),

    {next_state, full, NewStateData#state{reply_pid=From}}.

handle_receiving_last_chunk(NewData, State=#state{buffer_queue=BufferQueue,
                                                  remainder_data=RemainderData,
                                                  num_bytes_received=PreviousBytesReceived,
                                                  content_length=ContentLength}) ->

    NewRemainderData = combine_new_and_remainder_data(NewData, RemainderData),
    UpdatedBytesReceived = PreviousBytesReceived + size(NewData),

    {NewBufferQueue, NewRemainderData2} =
    data_blocks(NewRemainderData, ContentLength, UpdatedBytesReceived, BufferQueue),

    NewStateData = maybe_write_blocks(State#state{buffer_queue=NewBufferQueue,
                                                  remainder_data=NewRemainderData2,
                                                  num_bytes_received=UpdatedBytesReceived}),

    Reply = ok,
    {reply, Reply, all_received, NewStateData}.
