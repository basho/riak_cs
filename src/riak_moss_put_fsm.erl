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


-record(state, {name :: string(),
                data :: [binary()],
                pb_pid :: pid(),
                writer_pid :: pid(),
                size :: non_neg_integer(),
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
-spec init([string(), non_neg_integer(), timeout()]) ->
                  {ok, initialize, state(), 0}.
init([Name, FileSize, BlockSize, Data, Timeout]) ->
    State = #state{name=Name,
                   size=Size,
                   timeout=Timeout},
    {ok, initialize, State, 0}.

%% @doc First state of the put fsm
initialize(timeout, State=#state{timeout=Timeout}) ->
    %% Get a connection to riak
    case riak_moss_lfs_utils:get_connection() of
        {ok, PbPid} ->
            %% Start the worker to perform the writing
            case start_writer() of
                {ok, WriterPid} ->
                    ok;
                {error, Reason1} ->
                    lager:error("Failed to start the put fsm writer process. Reason: ",
                                [Reason1]),
                    {stop, Reason, State}
            end,
            UpdState = State#state{pb_pid=PbPid,
                                   writer_pid=WriterPid},
            {next_state, write_root, UpdState, Timeout};
        {error, Reason} ->
            lager:error("Failed to establish connection to Riak. Reason: ~p",
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
write_root(writer_ready, State=#state{timeout=Timeout}) ->
    %% Write the initial root block

    %% @TODO Use actual UUID
    UUID = riak_moss:unique_hex_id(),
    %% Calculate the number of blocks
    BlockCount = block_count(FileSize, BlockSize),

    %% Punting on optimization where no extra blocks are created if
    %% file size < or ~= to block size.
    Blocks = [list_to_binary(FileName ++ ":" ++ UUID ++ ":" ++ integer_to_list(X)) ||
                 X <- lists:seq(1,BlockCount)],
    lager:debug("Blocks for ~p: ~p", [FileName, Blocks]),
    %% Assemble the metadata
    MDDict = dict:from_list([{?MD_USERMETA, MetaData ++
                                [{"file_size", FileSize},
                                 {"block_size", BlockSize},
                                 {"blocks_missing", term_to_binary(Blocks)},
                                 {"alive", false},
                                 {"uuid", UUID}]}]),
    Obj = riakc_obj:new(list_to_binary(Bucket),
                        list_to_binary(FileName)),
    NewObj = riakc_obj:update_metadata(
               riakc_obj:update_value(Obj, list_to_binary(UUID)), MDDict),
    case riakc_pb_socket:put(Pid, NewObj) of
        ok ->
            {ok, StoredObj} = riakc_pb_socket:get(Pid, list_to_binary(Bucket), list_to_binary(FileName)),
            put(Pid, Bucket, FileName, StoredObj, BlockSize, Blocks, Data);
        {error, _Reason1} ->
            ok
    end,
    {next_state, write_chunk, State, Timeout};
write_root({chunk_complete, Chunk}, State=#state{timeout=Timeout}) ->
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

