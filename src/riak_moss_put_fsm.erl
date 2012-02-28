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
-export([start_link/0,
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
                num_bytes_received :: non_neg_integer(),
                max_buffer_size :: non_neg_integer(),
                current_buffer_size :: non_neg_integer(),
                buffer_queue=queue:new(),
                remainder_data :: binary(),
                free_writers :: ordsets:new(),
                unacked_writes=ordsets:new(),
                all_writer_pids :: list(pid())}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

augment_data(Pid, Data) ->
    gen_fsm:sync_send_event(Pid, {augment_data, Data}).

finalize(Pid) ->
    gen_fsm:sync_send_event(Pid, finalize).

block_written(Pid, BlockID) ->
    gen_fsm:sync_send_event(Pid, {block_written, BlockID, self()}).

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
    %%    
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

    %% TODO:
    %% this time probably
    %% shouldn't be hardcoded,
    %% and if it is, what should
    %% it be?
    {ok, TRef} = timer:send_interval(60000, self(), save_manifest),
    {next_state, not_full, State#state{manifest=Manifest,
                                       timer_ref=TRef,
                                       mani_pid=ManiPid,
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

all_received({block_written, BlockID, WriterPid}, State) ->
    NewState = state_from_block_written(BlockID, WriterPid, State),
    case ordsets:size(NewState#state.unacked_writes) of
        0 ->
            {next_state, done, NewState};
        _ ->
            {next_state, all_received, NewState}
    end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------

%% when this new data
%% is the last chunk
not_full({augment_data, NewData}, _From, 
                State=#state{content_length=CLength,
                             num_bytes_received=NumBytesReceived,
                             current_buffer_size=CurrentBufferSize,
                             max_buffer_size=MaxBufferSize}) ->

    case handle_chunk(CLength, NumBytesReceived, size(NewData),
                             CurrentBufferSize, MaxBufferSize) of
        last_chunk ->
            %% handle_receiving_last_chunk(),
            Reply = ok,
            {reply, Reply, all_received, State};
        accept ->
            %% handle_accept_chunk(),
            Reply = ok,
            {reply, Reply, not_full, State};
        backpressure ->
            %% stash the From pid into
            %% state
            %% handle_backpressure_for_chunk(),
            Reply = ok,
            {reply, Reply, not_full, State}
    %% 1. Maybe write another block
    end.

all_received(finalize, _From, State) ->
    %% 1. stash the From pid into our
    %%    state so that we know to reply
    %%    later with the finished manifest
    Reply = ok,
    {reply, Reply, all_received, State}.

done(finalize, _From, State) ->
    %% 1. reply immediately
    %%    with the finished manifest
    Reply = ok,
    {reply, Reply, stop, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(save_manifest, StateName, State) ->
    %% 1. save the manifest

    %% TODO:
    %% are there any times where
    %% we should be cancelling the
    %% timer here, depending on the
    %% state we're in?
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
    ok.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_chunk(_ContentLength, _NumBytesReceived, _NewDataSize, _CurrentBufferSize, _MaxBufferSize) ->
    ok.

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
-spec maybe_write_block(term(), term(), term()) -> {term(), term(), term()}.
maybe_write_block(BufferQueue, FreeWriters, UnackedWrites) ->
    {BufferQueue, FreeWriters, UnackedWrites}.

-spec state_from_block_written(non_neg_integer(), pid(), term()) -> term().
state_from_block_written(BlockID, WriterPid,
                                State=#state{unacked_writes=UnackedWrites,
                                             free_writers=FreeWriters,
                                             buffer_queue=BufferQueue}) ->
    NewUnackedSet = ordsets:del_element(BlockID, UnackedWrites),
    NewFreeWriters = ordsets:add_element(WriterPid, FreeWriters),
    %% 3. Maybe write another block
    {NewBufferQueue, NewFreeWriters2, NewUnackedSet2} =
    maybe_write_block(BufferQueue, NewFreeWriters, NewUnackedSet),
    State#state{unacked_writes=NewUnackedSet2,
                free_writers=NewFreeWriters2,
                buffer_queue=NewBufferQueue}.

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
