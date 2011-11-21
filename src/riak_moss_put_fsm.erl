%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_put_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/6,
         current_state/1]).

-endif.

%% API
-export([start_link/5,
         send_event/2,
         augment_data/2]).

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         write_root/2,
         write_block/2,
         waiting/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {bucket :: binary(),
                filename :: binary(),
                data :: undefined | [binary()],
                writer_pid :: undefined | pid(),
                content_length :: pos_integer(),
                bytes_received :: non_neg_integer(),
                next_block_id=0 :: non_neg_integer(),
                raw_data :: undefined | binary(),
                timeout :: timeout()}).
-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_put_fsm'.
-spec start_link(binary(),
                 string(),
                 pos_integer(),
                 binary(),
                 timeout()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Bucket, Name, ContentLength, Data, Timeout) ->
    Args = [Bucket, Name, ContentLength, Data, Timeout],
    gen_fsm:start_link(?MODULE, Args, []).

%% @doc Send an event to a `riak_moss_put_fsm'.
-spec send_event(pid(), term()) -> ok.
send_event(Pid, Event) ->
    gen_fsm:send_event(Pid, Event).

%% @doc Augment the file data being written by a `riak_moss_put_fsm'.
-spec augment_data(pid(), binary()) -> ok | {error, term()}.
augment_data(Pid, Data) ->
    try
        gen_fsm:sync_send_all_state_event(Pid, {augment_data, Data})
    catch
        _:Reason ->
            case Reason of
                {noproc, _} ->
                    {error, {riak_moss_put_fsm_dead, Pid}};
                _ ->
                    {error, Reason}
            end
    end.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @doc Initialize the fsm.
-spec init([binary() | string() | pos_integer() | timeout()]) ->
                  {ok, initialize, state(), 0} |
                  {ok, write_root, state()}.
init([Bucket, Name, ContentLength, RawData, Timeout]) ->
    %% @TODO Get rid of this once sure that we can
    %% guarantee file name will be passed in as a binary.
    case is_binary(Name) of
        true ->
            FileName = Name;
        false ->
            FileName = list_to_binary(Name)
    end,

    RawDataSize = byte_size(RawData),
    %% Break up the current data into block-sized chunks
    %% @TODO Maybe move this function to `riak_moss_lfs_utils'.
    {Data, Remainder} = data_blocks(RawData, ContentLength, RawDataSize, []),
    State = #state{bucket=Bucket,
                   filename=FileName,
                   content_length=ContentLength,
                   bytes_received=RawDataSize,
                   data=Data,
                   raw_data=Remainder,
                   timeout=Timeout},
    {ok, initialize, State, 0};
init({test, Args, StateProps}) ->
    {ok, initialize, State, 0} = init(Args),
    %% Update the state with entries from StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    ModStateFun = fun({Field, Value}, State0) ->
                          Pos = proplists:get_value(Field, FieldPos),
                          setelement(Pos, State0, Value)
                  end,
    TestState = lists:foldl(ModStateFun, State, StateProps),
    {ok, write_root, TestState}.

%% @doc First state of the put fsm
-spec initialize(timeout, state()) ->
                        {next_state, write_root, state(), timeout()} |
                        {stop, term(), state()}.
initialize(timeout, State=#state{bucket=Bucket,
                                 filename=FileName,
                                 content_length=ContentLength,
                                 timeout=Timeout}) ->
    %% Start the worker to perform the writing
    case start_writer() of
        {ok, WriterPid} ->
            %% Provide the writer with the file details
            riak_moss_writer:initialize(WriterPid,
                                        self(),
                                        Bucket,
                                        FileName,
                                        ContentLength),
            UpdState = State#state{writer_pid=WriterPid},
            {next_state, write_root, UpdState, Timeout};
        {error, Reason} ->
            lager:error("Failed to start the put fsm writer process. Reason: ",
                        [Reason]),
            {stop, Reason, State}
    end.

%% @doc State for writing to the root block of a file.
-spec write_root(writer_ready | {block_written, pos_integer()},
                 state()) ->
                        {next_state,
                         write_block,
                         state(),
                         non_neg_integer()}.
write_root(writer_ready, State=#state{writer_pid=WriterPid,
                                      timeout=Timeout}) ->
    %% Send request to the writer to write the initial root block
    riak_moss_writer:write_root(WriterPid),
    {next_state, write_block, State, Timeout};
write_root({block_written, BlockId}, State=#state{writer_pid=WriterPid,
                                                  timeout=Timeout}) ->
    riak_moss_writer:update_root(WriterPid, {block_ready, BlockId}),
    {next_state, write_block, State, Timeout}.

%% @doc State for writing a block of a file. The
%% transition from this state is to `write_root'.
-spec write_block(root_ready | all_blocks_written, state()) ->
                         {next_state,
                          write_root,
                          state(),
                          non_neg_integer()}.
write_block(root_ready, State=#state{data=Data,
                                     next_block_id=BlockID,
                                     writer_pid=WriterPid,
                                     timeout=Timeout}) ->
    case Data of
        [] ->
            %% All received data has been written so wait
            %% for more data to arrive.
            {next_state, waiting, State, Timeout};
        [NextBlock | RestData] ->
            riak_moss_writer:write_block(WriterPid, BlockID, NextBlock),
            UpdState = State#state{data=RestData,
                                   next_block_id=BlockID+1},
            {next_state, write_root, UpdState, Timeout}
    end;
write_block(all_blocks_written, State) ->
    %% @TODO Respond to the request initiator
    {stop, normal, State}.

%% @doc State that is transistioned to when all the data
%% that has been received by the fsm has been written, but
%% more data for the file remains.
-spec waiting(timeout, state()) -> {stop, timeout, state()}.
waiting(timeout, State) ->
    {stop, timeout, State}.

%% @doc Handle events that should be handled
%% the same regardless of the current state.
-spec handle_event(term(), atom(), state()) ->
                          {stop, badmsg, state()}.
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), state()) ->
                               {reply, term(), atom(), state()} |
                               {next_state, atom(), state()}.
handle_sync_event(current_state, _From, StateName, State) ->
    {reply, StateName, StateName, State};
handle_sync_event({augment_data, NewData},
             _From,
             StateName,
             State=#state{data=Data,
                          content_length=ContentLength,
                          raw_data=RawData,
                          bytes_received=BytesReceived
                         }) ->
    UpdBytesReceived = BytesReceived + byte_size(NewData),
    case RawData of
        undefined ->
            {UpdData, Remainder} = data_blocks(NewData,
                                               ContentLength,
                                               UpdBytesReceived,
                                               Data);
        _ ->
            {UpdData, Remainder} =
                data_blocks(<<RawData/binary, NewData/binary>>,
                            ContentLength,
                            UpdBytesReceived,
                            Data)
    end,
    UpdState = State#state{data=UpdData,
                           bytes_received=UpdBytesReceived,
                           raw_data=Remainder},
    case StateName of
        waiting ->
            gen_fsm:send_event(self(), root_ready),
            NextState = write_block;
        _ ->
            NextState = StateName
    end,
    {reply, ok, NextState, UpdState};
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @doc @TODO
-spec handle_info(term(), atom(), state()) ->
                         {next_state, atom(), state(), timeout()} |
                         {stop, badmsg, state()}.
handle_info({'EXIT', _Pid, _Reason}, StateName, State=#state{timeout=Timeout}) ->
    {next_state, StateName, State, Timeout};
handle_info({_ReqId, {ok, _Pid}},
            StateName,
            State=#state{timeout=Timeout}) ->
    {next_state, StateName, State, Timeout};
handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Unused.
-spec terminate(term(), atom(), state()) -> ok.
terminate(Reason, _StateName, _State) ->
    Reason.

%% @doc Unused.
-spec code_change(term(), atom(), state(), term()) ->
                         {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
%% @doc Start a `riak_moss_writer' process to perform the actual work
%% of writing data to Riak.
-spec start_writer() -> {ok, pid()} | {error, term()}.
start_writer() ->
    riak_moss_writer_sup:start_writer(node(), []).

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

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start a `riak_moss_put_fsm' for testing.
-spec test_link([{atom(), term()}],
                binary(),
                string(),
                pos_integer(),
                binary(),
                timeout()) ->
                       {ok, pid()} | ignore | {error, term()}.
test_link(StateProps, Bucket, Name, ContentLength, Data, Timeout) ->
    Args = [Bucket, Name, ContentLength, Data, Timeout],
    gen_fsm:start_link(?MODULE, {test, Args, StateProps}, []).

%% @doc Get the current state of the fsm for testing inspection
-spec current_state(pid()) -> atom() | {error, term()}.
current_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_state).

-endif.
