%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_writer).

-behaviour(gen_server).

%% API
-export([start_link/1,
         initialize/5,
         write_root/1,
         update_root/2,
         write_block/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {filename :: binary(),
                uuid :: string() | binary(), % @TODO Remove string() after
                                                % using druuid for uuids.
                file_size :: pos_integer(),
                block_size :: pos_integer(),
                riak_pid :: pid(),
                fsm_pid :: pid()}).
-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_writer'.
-spec start_link([term()]) ->
                        {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% @doc Setup some state information once the
%% server has started.
-spec initialize(pid(), pid(), binary(), pos_integer(), pos_integer()) -> ok.
initialize(Pid, FsmPid, FileName, FileSize, BlockSize) ->
    gen_server:cast(Pid, {initialize, FsmPid, FileName, FileSize, BlockSize}).

%% @doc Write a new root block
-spec write_root(pid()) -> {ok, term()}.
write_root(Pid) ->
    gen_server:cast(Pid, write_root).

%% @doc Update a root block
-type update_op() :: {block_ready, pos_integer()}.
-spec update_root(pid(), update_op()) -> ok.
update_root(Pid, UpdateOp) ->
    gen_server:cast(Pid, {update_root, UpdateOp}).

%% @doc Write a file block
-spec write_block(pid(), pos_integer(), binary()) -> ok.
write_block(Pid, BlockID, Data) ->
    gen_server:cast(Pid, {write_block, BlockID, Data}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([term()]) -> {ok, state()} | {stop, term()}.
init(_Args) ->
    %% Get a connection to riak
    case riak_moss_lfs_utils:riak_connection() of
        {ok, RiakPid} ->
            {ok, #state{riak_pid=RiakPid}};
        {error, Reason} ->
            lager:error("Failed to establish connection to Riak. Reason: ~p",
                        [Reason]),
            {stop, riak_connect_failed}
    end.

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

%% @doc Handle asynchronous commands issued via
%% the exported functions.
-spec handle_cast(term(), state()) ->
                         {noreply, state()}.
handle_cast({initialize, FsmPid, FileName, FileSize, BlockSize}, State) ->

    gen_fsm:send_event(FsmPid, writer_ready),
    {noreply, State#state{fsm_pid=FsmPid,
                          filename=FileName,
                          file_size=FileSize,
                          block_size=BlockSize}};
handle_cast(write_root, State=#state{fsm_pid=FsmPid}) ->
    %% @TODO Use druuid for actual UUIDs :)
    UUID = riak_moss:unique_hex_id(),

    %% @TODO Actually write the root block to riak
    %% Calculate the number of blocks
    %% BlockCount = block_count(FileSize, BlockSize),
    %% Assemble the metadata
    %% MDDict = dict:from_list([{?MD_USERMETA, MetaData ++
    %%                             [{"file_size", FileSize},
    %%                              {"block_size", BlockSize},
    %%                              {"blocks_missing", term_to_binary(Blocks)},
    %%                              {"alive", false},
    %%                              {"uuid", UUID}]}]),
    %% Obj = riakc_obj:new(list_to_binary(Bucket),
    %%                     list_to_binary(FileName)),
    %% NewObj = riakc_obj:update_metadata(
    %%            riakc_obj:update_value(Obj, list_to_binary(UUID)), MDDict),
    %% case riakc_pb_socket:put(Pid, NewObj) of
    %%     ok ->
    %%         {ok, StoredObj} = riakc_pb_socket:get(Pid, list_to_binary(Bucket), list_to_binary(FileName)),
    %%         put(Pid, Bucket, FileName, StoredObj, BlockSize, Blocks, Data);
    %%     {error, _Reason1} ->
    %%         ok
    %% end,
    gen_fsm:send_event(FsmPid, root_ready),
    {noreply, State#state{uuid=UUID}};
handle_cast({update_root, _UpdateOp}, State=#state{fsm_pid=FsmPid}) ->
    %% @TODO Update root block
    %% @TODO If `block_remaining' is empty
    %% gen_fsm:send_event(FsmPid, all_blocks_written),
    gen_fsm:send_event(FsmPid, root_ready),
    {noreply, State};
handle_cast({write_block, BlockID, _Data}, State=#state{fsm_pid=FsmPid}) ->
    %% @TODO Write the block data to riak
    gen_fsm:send_event(FsmPid, {block_written, BlockID}),
    {noreply, State};
handle_cast(Event, State) ->
    lager:warning("Received unknown cast event: ~p", [Event]),
    {noreply, State}.

%% @doc @TODO
-spec handle_info(term(), state()) ->
         {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Unused.
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @doc Unused.
-spec code_change(term(), state(), term()) ->
         {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
