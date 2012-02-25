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

-record(state, {reply_pid :: pid(),
                timer_ref :: term(),
                bucket :: binary(),
                key :: binary(),
                manifest :: lfs_manifest(),
                content_length :: pos_integer(),
                content_type :: binary(),
                num_bytes_received :: non_neg_integer(),
                max_buffer_size :: non_neg_integer(),
                current_buffer_size :: non_neg_integer(),
                buffer_queue=queue:new(),
                remainder_data :: binary(),
                unacked_writes=ordsets:new()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

augment_data(Pid, Data) ->
    gen_fsm:sync_send_event(Pid, {augment_data, Data}).

finalize(Pid) ->
    gen_fsm:sync_send_event(Pid, finalize).

block_written(Pid, BlockID) ->
    gen_fsm:sync_send_event(Pid, {block_written, BlockID}).

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
init([Bucket, Key, ContentLength, ContentType, _Metadata]) ->
    {ok, prepare, #state{bucket=Bucket,
                         key=Key,
                         content_length=ContentLength,
                         content_type=ContentType},
                     0}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
prepare(timeout, State) ->
    %% do set up work

    %% 1. start the manifest_fsm proc
    %% 2. create a new manifest
    %% 3. start (or pull from poolboy)
    %%    blocks gen_servers
    %% 4. start a timer that will
    %%    send events to let us know to
    %%    to save the manifest to the
    %%    manifest_fsm
    {ok, TRef} = timer:send_interval(60000, self(), save_manifest),
    {next_state, not_full, State#state{timer_ref=TRef}}.

%% when a block is written
%% and we were already not full,
%% we're still not full
not_full({block_written, _BlockID}, State) ->
    %% 1. send a message to the
    %%    gen_server that sent us this
    %%    message to start writing
    %%    the next block from our
    %%    buffer
    %% 2. Remove this block from the
    %%    unacked_writes set
    {next_state, not_full, State}.

full({block_written, _BlockID}, State) ->
    %% 1. send a message to the
    %%    gen_server that sent us this
    %%    message to start writing
    %%    the next block from our
    %%    buffer
    %% 2. Remove this block from the
    %%    unacked_writes set
    {next_state, not_full, State}.

all_received({block_written, BlockID}, State=#state{unacked_writes=UnackedWrites}) ->
    NewUnackedSet = ordsets:del_element(BlockID, UnackedWrites),
    case ordsets:size(NewUnackedSet) of
        0 ->
            {next_state, done, State};
        _ ->
            {next_state, all_received, State}
    end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------

%% when this new data
%% is the last chunk
not_full({augment_data, NewData}, _From, State=#state{content_length=CLength,
                                                      num_bytes_received=NumBytesReceived,
                                                      current_buffer_size=CurrentBufferSize,
                                                      max_buffer_size=MaxBufferSize}) ->
    case handle_chunk(CLength, NumBytesReceived, size(NewData), CurrentBufferSize, MaxBufferSize) of
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
    end.

all_received(finalize, _From, State) ->
    Reply = ok,
    {reply, Reply, all_received, State}.

done(finalize, _From, State) ->
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
handle_sync_event({augment_data, _NewData}, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State};
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
