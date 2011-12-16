%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_delete_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/7,
         current_state/1]).

-endif.

%% API
-export([start_link/3,
         send_event/2,
         finalize/1]).

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         waiting_file_info/2,
         delete_blocks/2,
         delete_manifest/2,
         client_wait/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {bucket :: binary(),
                filename :: binary(),
                block_count :: non_neg_integer(),
                blocks_remaining :: non_neg_integer(),
                deleter_pid :: undefined | pid(),
                waiter :: undefined | {reference(), pid()},
                timeout :: timeout()}).
-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_delete_fsm'.
-spec start_link(binary(),
                 string(),
                 timeout()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Bucket, Name, Timeout) ->
    Args = [Bucket, Name, Timeout],
    gen_fsm:start_link(?MODULE, Args, []).

%% @doc Send an event to a `riak_moss_delete_fsm'.
-spec send_event(pid(), term()) -> ok.
send_event(Pid, Event) ->
    gen_fsm:send_event(Pid, Event).

%% @doc Finalize the put and return manifest.
-spec finalize(pid()) -> {ok, riak_moss_lfs_utils:lfs_manifest()}
                             | {error, term()}.
finalize(Pid) ->
    send_sync_event(Pid, finalize, 60000).

-spec send_sync_event(pid(), term(), timeout()) -> term() | {error, term()}.
send_sync_event(Pid, Msg, Timeout) ->
    try
        gen_fsm:sync_send_all_state_event(Pid, Msg, Timeout)
    catch
        _:Reason ->
            case Reason of
                {noproc, _} ->
                    {error, {riak_moss_delete_fsm_dead, Pid}};
                _ ->
                    {error, Reason}
            end
    end.


%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @doc Initialize the fsm.
-spec init([binary() | timeout()]) ->
                  {ok, initialize, state(), 0} |
                  {ok, write_root, state()}.
init([Bucket, Name, Timeout]) ->
    %% @TODO Get rid of this once sure that we can
    %% guarantee file name will be passed in as a binary.
    case is_binary(Name) of
        true ->
            FileName = Name;
        false ->
            FileName = list_to_binary(Name)
    end,

    State = #state{bucket=Bucket,
                   filename=FileName,
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
    {ok, waiting_file_info, TestState}.

%% @doc First state of the delete fsm
-spec initialize(timeout, state()) ->
                        {next_state, write_root, state(), timeout()} |
                        {stop, term(), state()}.
initialize(timeout, State=#state{bucket=Bucket,
                                 filename=FileName,
                                 timeout=Timeout}) ->
    %% Start the worker to perform the writing
    case start_deleter() of
        {ok, DeleterPid} ->
            %% Provide the deleter with the file details
            riak_moss_writer:initialize(DeleterPid,
                                        self(),
                                        Bucket,
                                        FileName),
            UpdState = State#state{deleter_pid=DeleterPid},
            {next_state, waiting_file_info, UpdState, Timeout};
        {error, Reason} ->
            lager:error("Failed to start the delete fsm deleter process. Reason: ",
                        [Reason]),
            {stop, Reason, State}
    end.

%% @doc State to receive information about the file or object to be
%% deleted from the deleter process.
-spec waiting_file_info({deleter_ready, object | {file, BlockCount}}
                        state()) ->
                               {next_state,
                                delete_blocks | delete_object,
                                state(),
                                non_neg_integer()}.
waiting_file_info({deleter_ready, object}, State=#state{writer_pid=WriterPid,
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
write_block({all_blocks_written, Manifest}, State=#state{waiter=Waiter, timeout=Timeout}) ->
    NewState = State#state{final_manifest=Manifest},
    case Waiter of
        undefined ->
            {next_state, client_wait, NewState, Timeout};
        Waiter ->
            gen_fsm:reply(Waiter, {ok, Manifest}),
            {stop, normal, NewState}
    end.

%% @doc State that is transistioned to when all the data
%% that has been received by the fsm has been written, but
%% more data for the file remains.
-spec waiting(timeout, state()) -> {stop, timeout, state()}.
waiting(timeout, State) ->
    {stop, timeout, State}.

client_wait(timeout, State) ->
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
handle_sync_event(finalize, From, StateName, State=#state{final_manifest=undefined}) ->
    {next_state, StateName, State#state{waiter=From}};
handle_sync_event(finalize, _From, StateName, State=#state{final_manifest=M}) ->
    {reply, {ok, M}, StateName, State};
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

%% @doc Start a `riak_moss_delete_fsm' for testing.
-spec test_link([{atom(), term()}],
                binary(),
                string(),
                timeout()) ->
                       {ok, pid()} | ignore | {error, term()}.
test_link(StateProps, Bucket, Name, Timeout) ->
    Args = [Bucket, Name, Timeout],
    gen_fsm:start_link(?MODULE, {test, Args, StateProps}, []).

%% @doc Get the current state of the fsm for testing inspection
-spec current_state(pid()) -> atom() | {error, term()}.
current_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_state).

-endif.
