%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_deleter).

-behaviour(gen_server).

-include("riak_moss.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0]).

-endif.

%% API
-export([start_link/0,
         initialize/4,
         delete_root/1,
         update_root/2,
         delete_block/2]).

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
                riak_pid :: pid(),
                fsm_pid :: pid(),
                storage_module :: atom()}).
-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_deleter'.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% @doc Setup some state information once the
%% server has started.
-spec initialize(pid(),
                 pid(),
                 binary(),
                 binary()) -> ok.
initialize(Pid, FsmPid, Bucket, FileName) ->
    gen_server:cast(Pid, {initialize,
                          FsmPid,
                          Bucket,
                          FileName}).

%% @doc Delete a root block
-spec delete_root(pid()) -> {ok, term()}.
delete_root(Pid) ->
    gen_server:cast(Pid, delete_root).

%% @doc Update a root block
-type update_op() :: set_inactive.
-spec update_root(pid(), update_op()) -> ok.
update_root(Pid, UpdateOp) ->
    gen_server:cast(Pid, {update_root, UpdateOp}).

%% @doc Delete a file block
-spec delete_block(pid(), pos_integer()) -> ok.
delete_block(Pid, BlockID) ->
    gen_server:cast(Pid, {delete_block, BlockID}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([] | {test, [atom()]}) -> {ok, state()} | {stop, term()}.
init([]) ->
    %% Get a connection to riak
    case riak_moss_utils:riak_connection() of
        {ok, RiakPid} ->
            {ok, #state{riak_pid=RiakPid,
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
handle_cast({initialize, FsmPid, Bucket, FileName}, State) ->
    #state{storage_module=StorageModule,
           riak_pid=RiakPid} = State,
    %% Get the details about the object or file to be deleted
    case object_details(RiakPid, StorageModule, Bucket, FileName) of
        object ->
            ObjDetails = object,
            UUID = undefined;
        {file, BlockCount, UUID} ->
            ObjDetails = {file, BlockCount};
        {error, Reason} ->
            ObjDetails = {error, Reason},
            UUID = undefined
    end,
    riak_moss_delete_fsm:send_event(FsmPid, {deleter_ready, ObjDetails}),
    {noreply, State#state{bucket=Bucket,
                          fsm_pid=FsmPid,
                          filename=FileName,
                          uuid=UUID}};
handle_cast(delete_root, State=#state{bucket=Bucket,
                                      fsm_pid=FsmPid,
                                      filename=FileName,
                                      riak_pid=RiakPid,
                                      storage_module=StorageModule,
                                      uuid=UUID}) ->
    ObjsBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case delete_root_block(RiakPid,
                           StorageModule,
                           ObjsBucket,
                           FileName,
                           UUID) of
        ok ->
            riak_moss_delete_fsm:send_event(FsmPid, root_deleted);
        {error, _Reason} ->
            %% @TODO Handle error condition
            ok
    end,
    {noreply, State};
handle_cast({update_root, UpdateOp}, State=#state{bucket=Bucket,
                                                  filename=FileName,
                                                  fsm_pid=FsmPid,
                                                  riak_pid=RiakPid,
                                                  storage_module=StorageModule,
                                                  uuid=UUID}) ->
    ObjsBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case update_root_block(RiakPid, StorageModule, ObjsBucket, FileName, UUID, UpdateOp) of
        {ok, Status} ->
            riak_moss_delete_fsm:send_event(FsmPid, Status);
        {error, _Reason} ->
            %% @TODO Handle error condition including
            %% case where the UUID has changed.
            ok
    end,
    {noreply, State};
handle_cast({delete_block, BlockID}, State=#state{bucket=Bucket,
                                                  filename=FileName,
                                                  fsm_pid=FsmPid,
                                                  riak_pid=RiakPid,
                                                  storage_module=StorageModule,
                                                  uuid=UUID}) ->
    BlockName = riak_moss_lfs_utils:block_name(FileName, UUID, BlockID),
    BlocksBucket = riak_moss_utils:to_bucket_name(blocks, Bucket),
    case delete_data_block(RiakPid, StorageModule, BlocksBucket, BlockName) of
        ok ->
            riak_moss_delete_fsm:send_event(FsmPid, {block_deleted, BlockID});
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
%% @doc Delete the root block for a file from Riak.
-spec delete_root_block(pid(),
                        atom(),
                        binary(),
                        binary(),
                        binary() | undefined) ->
                               ok | {error, term()}.
delete_root_block(Pid, Module, Bucket, FileName, _UUID) ->
    case Module:delete(Pid, Bucket, FileName) of
        ok ->
            %% @TODO Verify the UUID before deleting
            %% unless it is `undefined'.
            ok;
        {error, Reason} ->
            lager:warning("Unable to delete the file manifest for ~p. Reason: ~p",
                          [FileName, Reason]),
            ok
    end.

%% @private
%% @doc Update the root block for a file stored in Riak.
-spec update_root_block(pid(), atom(), binary(), binary(), binary(), update_op()) ->
                               {ok, root_inactive} | {error, term()}.
update_root_block(Pid, Module, Bucket, FileName, _UUID, set_inactive) ->
    case Module:get(Pid, Bucket, FileName) of
        {ok, Obj} ->
            Manifest = binary_to_term(riakc_obj:get_value(Obj)),
            %% @TODO Check if the UUID is different
            UpdManifest = riak_moss_lfs_utils:set_inactive(Manifest),
            UpdObj = riakc_obj:update_value(Obj, term_to_binary(UpdManifest)),
            case Module:put(Pid, UpdObj) of
                ok ->
                    {ok, root_inactive};
                {error, Reason1} ->
                    {error, Reason1}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% @doc Delete a data block of a file from Riak.
-spec delete_data_block(pid(), atom(), binary(), binary()) ->
                               ok | {error, term()}.
delete_data_block(Pid, Module, Bucket, BlockName) ->
    Module:delete(Pid, Bucket, BlockName).

%% @private
%% @doc Determine if the target of deletion is a simple object
%% or a file.
-spec object_details(pid(), atom(), binary(), binary()) ->
                            object | {file, pos_integer()} | {error, term()}.
object_details(Pid, Module, Bucket, FileName) ->
    ObjsBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case Module:get(Pid, ObjsBucket, FileName) of
        {ok, Obj} ->
            Value = riakc_obj:get_value(Obj),
            case riak_moss_lfs_utils:is_manifest(Value) of
                true ->
                    Manifest = binary_to_term(Value),
                    BlockCount = riak_moss_lfs_utils:block_count(Manifest),
                    UUID = riak_moss_lfs_utils:file_uuid(Manifest),
                    {file, BlockCount, UUID};
                false ->
                    object
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start a `riak_moss_deleter' for testing.
-spec test_link() -> {ok, pid()} | {error, term()}.
test_link() ->
    gen_server:start_link(?MODULE, test, []).

-endif.
