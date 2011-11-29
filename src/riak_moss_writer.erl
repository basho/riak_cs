%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_writer).

-behaviour(gen_server).

-include("riak_moss.hrl").

%% API
-export([start_link/0,
         initialize/6,
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

-record(state, {bucket :: binary(),
                filename :: binary(),
                uuid :: binary(),
                file_size :: pos_integer(),
                block_size :: pos_integer(),
                riak_pid :: pid(),
                fsm_pid :: pid()}).
-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_writer'.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% @doc Setup some state information once the
%% server has started.
-spec initialize(pid(),
                 pid(),
                 binary(),
                 binary(),
                 pos_integer(),
                 pos_integer()) -> ok.
initialize(Pid, FsmPid, Bucket, FileName, FileSize, BlockSize) ->
    gen_server:cast(Pid, {initialize,
                          FsmPid,
                          Bucket,
                          FileName,
                          FileSize,
                          BlockSize}).

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
-spec init([]) -> {ok, state()} | {stop, term()}.
init([]) ->
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
handle_cast({initialize, FsmPid, Bucket, FileName, FileSize, BlockSize}, State) ->
    gen_fsm:send_event(FsmPid, writer_ready),
    {noreply, State#state{bucket=Bucket,
                          fsm_pid=FsmPid,
                          filename=FileName,
                          file_size=FileSize,
                          block_size=BlockSize}};
handle_cast(write_root, State=#state{bucket=Bucket,
                                     fsm_pid=FsmPid,
                                     filename=FileName,
                                     file_size=FileSize,
                                     block_size=BlockSize,
                                     riak_pid=RiakPid}) ->
    UUID = druuid:v4(),
    case write_root_block(RiakPid, Bucket, FileName, UUID, FileSize, BlockSize) of
        ok ->
            gen_fsm:send_event(FsmPid, root_ready);
        {error, _Reason} ->
            %% @TODO Handle error condition
            ok
    end,
    {noreply, State#state{uuid=UUID}};
handle_cast({update_root, UpdateOp}, State=#state{bucket=Bucket,
                                                  filename=FileName,
                                                  fsm_pid=FsmPid,
                                                  riak_pid=RiakPid,
                                                  uuid=UUID}) ->
    case update_root_block(RiakPid, Bucket, FileName, UUID, UpdateOp) of
        {ok, Status} ->
            gen_fsm:send_event(FsmPid, Status);
        {error, _Reason} ->
            %% @TODO Handle error condition including
            %% case where the UUID has changed.
            ok
    end,
    {noreply, State};
handle_cast({write_block, BlockID, Data}, State=#state{bucket=Bucket,
                                                       filename=FileName,
                                                       fsm_pid=FsmPid,
                                                       riak_pid=RiakPid,
                                                       uuid=UUID}) ->
    BlockName = riak_moss_lfs_utils:block_name(FileName, UUID, BlockID),
    case write_data_block(RiakPid, Bucket, BlockName, Data) of
        ok ->
            gen_fsm:send_event(FsmPid, {block_written, BlockID});
        {error, _Reason} ->
            %% @TODO Handle error condition
            ok
    end,
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

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
%% @doc Write the initial root block for a file to Riak.
-spec write_root_block(pid(),
                       binary(),
                       binary(),
                       binary(),
                       pos_integer(),
                       pos_integer()) ->
                              ok | {error, term()}.
write_root_block(Pid, Bucket, FileName, UUID, FileSize, BlockSize) ->
    %% Create a new file manifest
    Manifest = riak_moss_lfs_utils:new_manifest(Bucket,
                                                FileName,
                                                UUID,
                                                FileSize,
                                                BlockSize),
    case riakc_pb_socket:get(Pid, Bucket, FileName) of
        {ok, _StoredObj} ->
            %% @TODO The file being written by this writer will
            %% supercede the existing one.
            ok;
        {error, notfound} ->
            ok
    end,
    Obj = riakc_obj:new(Bucket, FileName, term_to_binary(Manifest)),
    riakc_pb_socket:put(Pid, Obj).

%% @private
%% @doc Update the root block for a file stored in Riak.
-spec update_root_block(pid(), binary(), binary(), binary(), update_op()) ->
                               {ok, root_ready | all_blocks_written} | {error, term()}.
update_root_block(Pid, Bucket, FileName, _UUID, {block_ready, BlockID}) ->
    case riakc_pb_socket:get(Pid, Bucket, FileName) of
        {ok, Obj} ->
            Manifest = binary_to_term(riakc_obj:get_value(Obj)),
            %% @TODO Check if the UUID is different
            BlocksRemaining = Manifest#lfs_manifest.blocks_remaining,
            UpdBlocksRemaining = sets:del_element(BlockID, BlocksRemaining),
            case sets:to_list(UpdBlocksRemaining) of
                [] ->
                    UpdManifest =
                        riak_moss_lfs_utils:finalize_manifest(Manifest),
                    Status = all_blocks_written;
                _ ->
                    UpdManifest = riak_moss_lfs_utils:remove_block(Manifest, BlockID),
                    Status = root_ready
            end,
            UpdObj = riakc_obj:update_value(Obj, term_to_binary(UpdManifest)),
            case riakc_pb_socket:put(Pid, UpdObj) of
                ok ->
                    {ok, Status};
                {error, Reason1} ->
                    {error, Reason1}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% @doc Write a data block of a file to Riak.
-spec write_data_block(pid(), binary(), binary(), binary()) ->
                              ok | {error, term()}.
write_data_block(Pid, Bucket, BlockName, Data) ->
    Obj = riakc_obj:new(Bucket, BlockName, Data),
    riakc_pb_socket:put(Pid, Obj).
