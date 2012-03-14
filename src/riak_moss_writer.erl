%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_writer).

-behaviour(gen_server).

-include("riak_moss.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0]).

-endif.

%% API
-export([start_link/0,
         initialize/7,
         write_root/1,
         update_root/2,
         write_block/3,
         stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {bucket_id :: binary(),
                bucket :: binary(),
                content_type :: string(),
                filename :: binary(),
                uuid :: binary(),
                file_size :: pos_integer(),
                block_size :: pos_integer(),
                riak_pid :: pid(),
                fsm_pid :: pid(),
                md5 :: binary(),
                storage_module :: atom()}).
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
                 binary(),
                 pos_integer(),
                 string()) -> ok.
initialize(Pid, FsmPid, Bucket, BucketId, FileName, ContentLength, ContentType) ->
    gen_server:cast(Pid, {initialize,
                          FsmPid,
                          Bucket,
                          BucketId,
                          FileName,
                          ContentLength,
                          ContentType}).

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

stop(Pid) ->
    gen_server:cast(Pid, stop).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([] | {test, [atom()]}) -> {ok, state()} | {stop, term()}.
init([]) ->
    %% Get a connection to riak
    case riak_moss_utils:riak_connection() of
        {ok, RiakPid} ->
            MD5 = crypto:md5_init(),
            {ok, #state{riak_pid=RiakPid,
                        md5=MD5,
                        storage_module=riakc_pb_socket}};
        {error, Reason} ->
            lager:error("Failed to establish connection to Riak. Reason: ~p",
                        [Reason]),
            {stop, riak_connect_failed}
    end;
init(test) ->
      {ok, #state{storage_module=riak_socket_dummy}}.

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @doc Handle asynchronous commands issued via
%% the exported functions.
-spec handle_cast(term(), state()) ->
                         {noreply, state()}.
handle_cast({initialize, FsmPid, Bucket, BucketId, FileName, ContentLength, ContentType}, State) ->
    riak_moss_put_fsm:send_event(FsmPid, writer_ready),
    {noreply, State#state{bucket_id=BucketId,
                          bucket=Bucket,
                          content_type=ContentType,
                          fsm_pid=FsmPid,
                          filename=FileName,
                          file_size=ContentLength}};
handle_cast(write_root, State=#state{bucket=Bucket,
                                     content_type=ContentType,
                                     fsm_pid=FsmPid,
                                     filename=FileName,
                                     file_size=ContentLength,
                                     riak_pid=RiakPid,
                                     storage_module=StorageModule}) ->
    UUID = druuid:v4(),
    ObjsBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case write_root_block(RiakPid,
                          StorageModule,
                          ObjsBucket,
                          FileName,
                          UUID,
                          ContentLength,
                          ContentType) of
        ok ->
            riak_moss_put_fsm:send_event(FsmPid, root_ready);
        {error, _Reason} ->
            %% @TODO Handle error condition
            ok
    end,
    {noreply, State#state{uuid=UUID}};
handle_cast({update_root, UpdateOp}, State=#state{bucket=Bucket,
                                                  filename=FileName,
                                                  fsm_pid=FsmPid,
                                                  riak_pid=RiakPid,
                                                  md5=MD5,
                                                  storage_module=StorageModule,
                                                  uuid=UUID}) ->
    ObjsBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case update_root_block(RiakPid, StorageModule, ObjsBucket, FileName, UUID, MD5, UpdateOp) of
        {ok, {all_blocks_written, Manifest}} ->
            riak_moss_put_fsm:send_event(FsmPid, {all_blocks_written, Manifest});
        {ok, Status} ->
            riak_moss_put_fsm:send_event(FsmPid, Status);
        {error, _Reason} ->
            %% @TODO Handle error condition including
            %% case where the UUID has changed.
            ok
    end,
    {noreply, State};
handle_cast({write_block, BlockID, Data}, State=#state{bucket_id=BucketId,
                                                       bucket=Bucket,
                                                       filename=FileName,
                                                       fsm_pid=FsmPid,
                                                       md5=MD5,
                                                       riak_pid=RiakPid,
                                                       storage_module=StorageModule,
                                                       uuid=UUID}) ->
    BlockName = riak_moss_lfs_utils:block_name(FileName, UUID, BlockID),
    BlocksBucket = riak_moss_utils:to_bucket_name(blocks, BucketId),
    NewMD5 = crypto:md5_update(MD5, Data),
    case write_data_block(RiakPid, StorageModule, Bucket, FileName, BlocksBucket, BlockName, Data) of
        ok ->
            riak_moss_put_fsm:send_event(FsmPid, {block_written, BlockID});
        {error, _Reason} ->
            %% @TODO Handle error condition
            ok
    end,
    {noreply, State#state{md5=NewMD5}};
handle_cast(stop, State=#state{riak_pid=RiakPid,
                               storage_module=StorageMod}) ->
    StorageMod:stop(RiakPid),
    {stop, normal, State};
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
                       atom(),
                       binary(),
                       binary(),
                       binary(),
                       pos_integer(),
                       string()) ->
                              ok | {error, term()}.
write_root_block(Pid, Module, Bucket, FileName, UUID, ContentLength, ContentType) ->
    %% Create a new file manifest
    Manifest = riak_moss_lfs_utils:new_manifest(Bucket,
                                                FileName,
                                                UUID,
                                                ContentLength,
                                                undefined,
                                                dict:new()),
    case Module:get(Pid, Bucket, FileName) of
        {ok, _StoredObj} ->
            %% @TODO The file being written by this writer will
            %% supercede the existing one.
            ok;
        {error, notfound} ->
            ok
    end,
    Obj = riakc_obj:new(Bucket,
                        FileName,
                        term_to_binary(Manifest),
                        ContentType),
    Module:put(Pid, Obj).

%% @private
%% @doc Update the root block for a file stored in Riak.
-spec update_root_block(pid(), atom(), binary(), binary(), binary(), binary(), update_op()) ->
                               {ok, root_ready | all_blocks_written} | {error, term()}.
update_root_block(Pid, Module, Bucket, FileName, _UUID, MD5, {block_ready, BlockID}) ->
    case Module:get(Pid, Bucket, FileName) of
        {ok, Obj} ->
            Manifest = binary_to_term(riakc_obj:get_value(Obj)),
            %% @TODO Check if the UUID is different
            UpdManifest = riak_moss_lfs_utils:remove_block(Manifest, BlockID),
            BlocksRemaining = riak_moss_lfs_utils:sorted_blocks_remaining(UpdManifest),
            case BlocksRemaining of
                [] ->
                    UpdManifest1 =
                        riak_moss_lfs_utils:finalize_manifest(Manifest, crypto:md5_final(MD5)),
                    Status = {all_blocks_written, UpdManifest1};
                _ ->
                    UpdManifest1 = UpdManifest,
                    Status = root_ready
            end,
            UpdObj = riakc_obj:update_value(Obj, term_to_binary(UpdManifest1)),
            case Module:put(Pid, UpdObj) of
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
-spec write_data_block(pid(), atom(), binary(), binary(), binary(), binary(), binary()) ->
                              ok | {error, term()}.
write_data_block(Pid, Module, S3Bucket, FileName, Bucket, Key, Data) ->
    Obj0 = riakc_obj:new(Bucket, Key, Data),
    %% SLF TODO: It looks like the #rpbputreq record for the riakc_pb_socket
    %%           client doesn't send the object's metadata over the PB wire,
    %%           so this metadata update is in vain?
    %%
    %%           I think it's a good idea to have "backpointers", to
    %%           be able to map these UUID jibberish things back to
    %%           human-sensible S3 bucket & file names.
    Obj = riakc_obj:update_metadata(Obj0, dict:from_list([{bucket, S3Bucket}, {filename, FileName}])),
    Module:put(Pid, Obj).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start a `riak_moss_writer' for testing.
-spec test_link() -> {ok, pid()} | {error, term()}.
test_link() ->
    gen_server:start_link(?MODULE, test, []).

-endif.
