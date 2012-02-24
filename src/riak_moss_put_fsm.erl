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
                max_buffer_size :: non_neg_integer(),
                curent_buffer_size :: non_neg_integer(),
                remainder_data :: binary()}).

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
init([]) ->
    {ok, prepare, #state{}, 0}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
prepare(timeout, State) ->
    %% do set up work
    {next_state, not_full, State}.

%% when a block is written
%% and we were already not full,
%% we're still not full
not_full({block_written, _BlockID}, State) ->
    {next_state, not_full, State}.

%% when we're in the full state
%% a block being written can either
%% keep us in the full state,
%% or drop us down to the not_full
%% state

%% when this block being written
%% drops us down to not_full
full({block_written, _BlockID}, State) ->
    {next_state, not_full, State}.

all_received({block_written, _BlockID}, State) ->
    {next_state, all_received, State};
all_received({block_written, _BlockID}, State) ->
    {next_state, done, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------

%% when size of NewData doesn't put us over the edge
not_full({augment_data, NewData}, _From, State) ->
    Reply = ok,
    {reply, Reply, not_full, State};
%% when size of NewData does make our buffer full
not_full({augment_data, NewData}, _From, State) ->
    Reply = ok,
    {reply, Reply, full, State};
not_full({augment_data, NewData}, _From, State) ->
    Reply = ok,
    {reply, Reply, all_received, State}.

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
