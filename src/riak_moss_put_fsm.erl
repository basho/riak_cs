%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_put_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         write_root/2,
         write_chunk/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).


-record(state, {filename :: binary(),
                data :: [binary()],
                writer_pid :: pid(),
                file_size :: pos_integer(),
                block_size :: pos_integer(),
                timeout :: timeout()}).
-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_put_fsm'.
-spec start_link([term()]) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_fsm:start_link(?MODULE, [Args], []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @doc Initialize the fsm.
%% -spec init([string(), pos_integer(), pos_integer(), binary(), timeout()]) ->
-spec init([term()]) -> {ok, initialize, state(), 0}.
init([Name, FileSize, BlockSize, _Data, Timeout]) ->
    State = #state{filename=list_to_binary(Name),
                   file_size=FileSize,
                   block_size=BlockSize,
                   timeout=Timeout},
    {ok, initialize, State, 0}.

%% @doc First state of the put fsm
-spec initialize(timeout, state()) ->
                        {next_state, write_root, state(), timeout()} |
                        {stop, term(), state()}.
initialize(timeout, State=#state{filename=FileName,
                                 file_size=FileSize,
                                 block_size=BlockSize,
                                 timeout=Timeout}) ->
    %% Start the worker to perform the writing
    case start_writer() of
        {ok, WriterPid} ->
            %% Provide the writer with the file details
            riak_moss_writer:initialize(WriterPid,
                                        self(),
                                        FileName,
                                        FileSize,
                                        BlockSize),
            UpdState = State#state{writer_pid=WriterPid},
            {next_state, write_root, UpdState, Timeout};
        {error, Reason} ->
            lager:error("Failed to start the put fsm writer process. Reason: ",
                        [Reason]),
            {stop, Reason, State}
    end.

%% @doc @TODO
-spec write_root(writer_ready | {chunk_complete, pos_integer()},
                 state()) ->
                        {next_state,
                         write_chunk,
                         state(),
                         non_neg_integer()}.
write_root(writer_ready, State=#state{writer_pid=WriterPid,
                                      timeout=Timeout}) ->
    %% Send request to the writer to write the initial root block
    riak_moss_writer:write_root(WriterPid),
    {next_state, write_chunk, State, Timeout};
write_root({chunk_complete, _Chunk}, State=#state{timeout=Timeout}) ->
    {next_state, write_chunk, State, Timeout}.

%% @doc @TODO
-spec write_chunk(term(), state()) ->
                        {next_state,
                         write_root,
                         state(),
                         non_neg_integer()}.
write_chunk(_Data, State=#state{timeout=Timeout}) ->
    {next_state, write_root, State, Timeout}.

%% @doc Unused.
-spec handle_event(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Unused.
-spec handle_sync_event(term(), term(), atom(), state()) ->
         {reply, ok, atom(), state()}.
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @doc @TODO
-spec handle_info(term(), atom(), state()) ->
         {next_state, atom(), state()}.
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

%% @doc Start a `riak_moss_writer' process to perform the actual work
%% of writing data to Riak.
-spec start_writer() -> {ok, pid()} | {error, term()}.
start_writer() ->
    riak_moss_writer_sup:start_writer().

