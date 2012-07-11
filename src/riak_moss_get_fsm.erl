%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc get fsm for Riak Moss.

-module(riak_moss_get_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/4]).

-endif.

-include("riak_moss.hrl").

%% API
-export([start_link/4,
         stop/1,
         continue/1,
         manifest/2,
         chunk/3,
         get_manifest/1,
         get_next_chunk/1]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         prepare/3,
         waiting_value/2,
         waiting_value/3,
         waiting_continue_or_stop/2,
         waiting_chunks/2,
         waiting_chunks/3,
         sending_remaining/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {from :: {pid(), reference()},
                mani_fsm_pid :: pid(),
                riakc_pid :: pid(),
                bucket :: term(),
                caller :: reference(),
                key :: term(),
                block_buffer=[] :: [{pos_integer(), term()}],
                manifest :: term(),
                manifest_uuid :: term(),
                blocks_left :: set(),
                test=false :: boolean(),
                next_block=0 :: 0 | pos_integer(),
                last_block_requested :: pos_integer(),
                total_blocks :: pos_integer(),
                free_readers :: [pid()],
                all_reader_pids :: [pid()]}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link(binary(), binary(), pid(), pid()) -> {ok, pid()} | {error, term()}.

start_link(Bucket, Key, Caller, RiakPid) ->
    gen_fsm:start_link(?MODULE, [Bucket, Key, Caller, RiakPid], []).

stop(Pid) ->
    gen_fsm:send_event(Pid, stop).

continue(Pid) ->
    gen_fsm:send_event(Pid, continue).

get_manifest(Pid) ->
    gen_fsm:sync_send_event(Pid, get_manifest, infinity).

get_next_chunk(Pid) ->
    gen_fsm:sync_send_event(Pid, get_next_chunk, infinity).

manifest(Pid, ManifestValue) ->
    gen_fsm:send_event(Pid, {object, self(), ManifestValue}).

chunk(Pid, ChunkSeq, ChunkValue) ->
    gen_fsm:send_event(Pid, {chunk, self(), {ChunkSeq, ChunkValue}}).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([Bucket, Key, Caller, RiakPid])
  when is_binary(Bucket), is_binary(Key), is_pid(Caller), is_pid(RiakPid) ->
    %% We need to do this (the monitor) for two reasons
    %% 1. We're started through a supervisor, so the
    %%    proc that actually intends to start us isn't
    %%    linked to us.
    %% 2. Even if we didn't use a supervisor, the webmachine
    %%    process uses exit(..., normal), even on abnormal
    %%    terminations, so this process would still
    %%    live.
    CallerRef = erlang:monitor(process, Caller),
    %% we want to trap exits because
    %% `erlang:link` isn't atomic, and
    %% since we're starting the reader
    %% through a supervisor we can't use
    %% `spawn_link`. If the process has already
    %% died before we call link, we'll get
    %% an exit Reason of `noproc`
    process_flag(trap_exit, true),

    State = #state{bucket=Bucket,
                   caller=CallerRef,
                   key=Key,
                   riakc_pid=RiakPid},
    {ok, prepare, State, 0};
init([test, Bucket, Key, ContentLength, BlockSize]) ->
    {ok, prepare, State1, 0} = init([Bucket, Key, self(), self()]),

    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, ReaderPid} =
        riak_moss_dummy_reader:start_link([self(),
                                           Bucket,
                                           Key,
                                           ContentLength,
                                           BlockSize]),
    link(ReaderPid),
    {ok, Manifest} = riak_moss_dummy_reader:get_manifest(ReaderPid),
    {ok, waiting_value, State1#state{free_readers=[ReaderPid],
                                     manifest=Manifest,
                                     test=true}}.

prepare(timeout, State) ->
    NewState = prepare(State),
    {next_state, waiting_value, NewState}.

prepare(get_manifest, _From, State) ->
    PreparedState = prepare(State),
    case PreparedState#state.manifest of
        undefined ->
            {stop, normal, notfound, PreparedState};
        Mfst ->
            NextStateTimeout = 60000,
            NewState = PreparedState#state{manifest_uuid=Mfst?MANIFEST.uuid,
                                           from=undefined},
            {reply, Mfst, waiting_continue_or_stop, NewState, NextStateTimeout}
    end.

waiting_value(stop, State) ->
    {stop, normal, State}.

waiting_value(get_manifest, _From, State=#state{manifest=undefined}) ->
    {stop, normal, notfound, State};
waiting_value(get_manifest, _From, State=#state{manifest=Mfst}) ->
    NextStateTimeout = 60000,
    NewState = State#state{manifest_uuid=Mfst?MANIFEST.uuid,
                           from=undefined},
    {reply, Mfst, waiting_continue_or_stop, NewState, NextStateTimeout}.

waiting_continue_or_stop(timeout, State) ->
    {stop, normal, State};
waiting_continue_or_stop(stop, State) ->
    {stop, normal, State};
waiting_continue_or_stop(continue, #state{manifest=Manifest,
                                          bucket=BucketName,
                                          key=Key,
                                          next_block=NextBlock,
                                          manifest_uuid=UUID,
                                          free_readers=Readers,
                                          riakc_pid=RiakPid}=State) ->
    BlockSequences = riak_moss_lfs_utils:block_sequences_for_manifest(Manifest),
    case BlockSequences of
        [] ->
            %% We should never get here because empty
            %% files are handled by the wm resource.
            _ = lager:warning("~p:~p has no blocks", [BucketName, Key]),
            {stop, normal, State};
        [_|_] ->
            BlocksLeft = sets:from_list(BlockSequences),
            TotalBlocks = sets:size(BlocksLeft),

            %% Start the block servers
            case Readers of
                undefined ->
                    FreeReaders =
                    riak_moss_block_server:start_block_servers(RiakPid,
                        riak_moss_lfs_utils:fetch_concurrency()),
                    _ = lager:debug("Block Servers: ~p", [FreeReaders]);
                _ ->
                    FreeReaders = Readers
            end,
            %% start retrieving the first set of blocks
            {LastBlockRequested, UpdFreeReaders} =
                read_blocks(BucketName, Key, UUID, FreeReaders, NextBlock, TotalBlocks),
            NewState = State#state{blocks_left=BlocksLeft,
                                   last_block_requested=LastBlockRequested-1,
                                   total_blocks=TotalBlocks,
                                   free_readers=UpdFreeReaders},
            {next_state, waiting_chunks, NewState}
    end.

waiting_chunks(get_next_chunk, From, #state{block_buffer=[], from=PreviousFrom}=State) when PreviousFrom =:= undefined ->
    %% we don't have a chunk ready
    %% yet, so we'll make note
    %% of the sender and go back
    %% into waiting for another
    %% chunk
    {next_state, waiting_chunks, State#state{from=From}};
waiting_chunks(get_next_chunk,
               _From,
               State=#state{block_buffer=[{NextBlock, Block} | RestBlockBuffer],
                            next_block=NextBlock}) ->
    _ = lager:debug("Returning block ~p to client", [NextBlock]),
    {reply, {chunk, Block}, waiting_chunks, State#state{block_buffer=RestBlockBuffer,
                                                        next_block=NextBlock+1}};
waiting_chunks(get_next_chunk, From, State) ->
    {next_state, waiting_chunks, State#state{from=From}}.

waiting_chunks({chunk, Pid, {NextBlock, BlockReturnValue}}, #state{from=From,
                                                                   blocks_left=Remaining,
                                                                   manifest_uuid=UUID,
                                                                   key=Key,
                                                                   bucket=BucketName,
                                                                   next_block=NextBlock,
                                                                   free_readers=FreeReaders,
                                                                   last_block_requested=LastBlockRequested,
                                                                   total_blocks=TotalBlocks,
                                                                   block_buffer=BlockBuffer}=State) ->
    _ = lager:debug("Retrieved block ~p", [NextBlock]),
    {ok, BlockValue} = BlockReturnValue,
    NewRemaining = sets:del_element(NextBlock, Remaining),
    BlocksLeft = sets:size(NewRemaining),
    _ = lager:debug("BlocksLeft: ~p", [BlocksLeft]),
    case From of
        undefined ->
            UpdBlockBuffer =
                lists:sort(fun block_sorter/2,
                           [{NextBlock, BlockValue} | BlockBuffer]),
            _ = lager:debug("BlockBuffer: ~p", [UpdBlockBuffer]),
            NewState0 = State#state{blocks_left=NewRemaining,
                                   block_buffer=UpdBlockBuffer},
            case BlocksLeft of
                0 ->
                    NewState = NewState0#state{free_readers=[Pid | FreeReaders]},
                    NextStateName = sending_remaining;
                _ ->
                    {ReadRequests, UpdFreeReaders} =
                        read_blocks(BucketName, Key, UUID, [Pid | FreeReaders], NextBlock+1, TotalBlocks),
                    NewState = NewState0#state{last_block_requested=LastBlockRequested+ReadRequests,
                                               free_readers=UpdFreeReaders},
                    NextStateName = waiting_chunks
            end,
            {next_state, NextStateName, NewState};
        _ ->
            _ = lager:debug("Returning block ~p to client", [NextBlock]),
            NewState0 = State#state{blocks_left=NewRemaining,
                                    from=undefined,
                                    free_readers=[Pid | FreeReaders],
                                    next_block=NextBlock+1},
            case BlocksLeft of
                0 when length(BlockBuffer) > 0 ->
                    gen_fsm:reply(From, {chunk, BlockValue}),
                    NewState=NewState0,
                    {next_state, sending_remaining, NewState};
                0 ->
                    gen_fsm:reply(From, {done, BlockValue}),
                    NewState=NewState0,
                    {stop, normal, NewState};
                _ ->
                    gen_fsm:reply(From, {chunk, BlockValue}),
                    {ReadRequests, UpdFreeReaders} =
                        read_blocks(BucketName, Key, UUID, [Pid | FreeReaders], LastBlockRequested+1, TotalBlocks),
                    _ = lager:debug("Started ~p new readers. Free readers: ~p", [ReadRequests, UpdFreeReaders]),
                    NewState = NewState0#state{last_block_requested=LastBlockRequested+ReadRequests,
                                               free_readers=UpdFreeReaders},
                    {next_state, waiting_chunks, NewState}
            end
    end;

waiting_chunks({chunk, Pid, {BlockSeq, BlockReturnValue}}, #state{blocks_left=Remaining,
                                                                   manifest_uuid=UUID,
                                                                   key=Key,
                                                                   bucket=BucketName,
                                                                   free_readers=FreeReaders,
                                                                   last_block_requested=LastBlockRequested,
                                                                   total_blocks=TotalBlocks,
                                                                   block_buffer=BlockBuffer}=State) ->
    %% we don't deal with missing chunks
    %% at all here, so this pattern
    %% match will fail
    _ = lager:debug("Retrieved block ~p", [BlockSeq]),
    {ok, BlockValue} = BlockReturnValue,
    NewRemaining = sets:del_element(BlockSeq, Remaining),
    BlocksLeft = sets:size(NewRemaining),
    UpdBlockBuffer =
        lists:sort(fun block_sorter/2,
                   [{BlockSeq, BlockValue} | BlockBuffer]),
    NewState0 = State#state{blocks_left=NewRemaining,
                            block_buffer=UpdBlockBuffer},
    _ = lager:debug("BlocksLeft: ~p", [BlocksLeft]),
    case BlocksLeft of
        0 ->
            NewState = NewState0#state{free_readers=[Pid | FreeReaders]},
            NextStateName = sending_remaining;
        _ ->
            {ReadRequests, UpdFreeReaders} =
                read_blocks(BucketName, Key, UUID, [Pid | FreeReaders], LastBlockRequested+1, TotalBlocks),
            NewState = NewState0#state{last_block_requested=LastBlockRequested+ReadRequests,
                                       free_readers=UpdFreeReaders},
            NextStateName = waiting_chunks
    end,
    {next_state, NextStateName, NewState}.

sending_remaining(get_next_chunk, _From, #state{block_buffer=[{BlockSeq, Block} | RestBlockBuffer]}=State) ->
    _ = lager:debug("Returning block ~p to client", [BlockSeq]),
    NewState = State#state{block_buffer=RestBlockBuffer},
    case RestBlockBuffer of
        [] ->
            {stop, normal, {done, Block}, NewState};
        _ ->
            {reply, {chunk, Block}, sending_remaining, NewState}
    end.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
%% TODO:
%% we don't want to just
%% stop whenever a reader is
%% killed once we have some concurrency
%% in our readers. But since we just
%% have one reader process now, if it dies,
%% we have no reason to stick around
%%
%% @TODO Also handle reader pid death
handle_info({'EXIT', ManiPid, _Reason}, _StateName, StateData=#state{mani_fsm_pid=ManiPid}) ->
    {stop, normal, StateData};
handle_info({'DOWN', CallerRef, process, _Pid, Reason},
            _StateName,
            State=#state{caller=CallerRef}) ->
    {stop, Reason, State};
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
terminate(_Reason, _StateName, #state{test=false,
                                      all_reader_pids=BlockServerPids,
                                      mani_fsm_pid=ManiPid}) ->

    riak_moss_manifest_fsm:stop(ManiPid),
    _ = case BlockServerPids of
            undefined ->
                ok;
            _ ->
                [riak_moss_block_server:stop(P) || P <- BlockServerPids]
        end,
    ok;
terminate(_Reason, _StateName, #state{test=true,
                                      free_readers=ReaderPids}) ->
    case ReaderPids of
        [ReaderPid|_] ->
            exit(ReaderPid, normal);
        _ ->
            ok
    end.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec block_sorter({pos_integer(), term()}, {pos_integer(), term()}) -> boolean().
block_sorter({A, _}, {B, _}) ->
    A < B.

-spec prepare(#state{}) -> #state{}.
prepare(#state{bucket=Bucket,
               key=Key,
               riakc_pid=RiakPid}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    {ok, ManiPid} = riak_moss_manifest_fsm:start_link(Bucket, Key, RiakPid),
    case riak_moss_manifest_fsm:get_active_manifest(ManiPid) of
        {ok, Manifest} ->
            _ = lager:debug("Manifest: ~p", [Manifest]),
            State#state{manifest=Manifest,
                        mani_fsm_pid=ManiPid};
        {error, notfound} ->
            State#state{mani_fsm_pid=ManiPid}
    end.

-spec read_blocks(binary(), binary(), binary(), [pid()], pos_integer(), pos_integer()) ->
                         {pos_integer(), [pid()]}.
read_blocks(Bucket, Key, UUID, FreeReaders, NextBlock, TotalBlocks) ->
    read_blocks(Bucket,
                Key,
                UUID,
                FreeReaders,
                NextBlock,
                TotalBlocks,
                0).

-spec read_blocks(binary(),
                  binary(),
                  binary(),
                  [pid()],
                  pos_integer(),
                  pos_integer(),
                  non_neg_integer()) ->
                         {pos_integer(), [pid()]}.
read_blocks(_Bucket, _Key, _UUID, [], _, _, ReadsRequested) ->
    {ReadsRequested, []};
read_blocks(_Bucket, _Key, _UUID, FreeReaders, _TotalBlocks, _TotalBlocks, ReadsRequested) ->
    {ReadsRequested, FreeReaders};
read_blocks(Bucket, Key, UUID, [ReaderPid | RestFreeReaders], NextBlock, _TotalBlocks, ReadsRequested) ->
    riak_moss_block_server:get_block(ReaderPid, Bucket, Key, UUID, NextBlock),
    read_blocks(Bucket, Key, UUID, RestFreeReaders, NextBlock+1, _TotalBlocks, ReadsRequested+1).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

test_link(Bucket, Key, ContentLength, BlockSize) ->
    gen_fsm:start_link(?MODULE, [test, Bucket, Key, ContentLength, BlockSize], []).

-endif.
