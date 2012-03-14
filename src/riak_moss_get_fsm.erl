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
-export([test_link/5]).

-endif.

-include("riak_moss.hrl").

%% API
-export([start_link/3,
         stop/1,
         continue/1,
         manifest/2,
         chunk/3,
         get_metadata/1,
         get_next_chunk/1]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         waiting_value/2,
         waiting_value/3,
         waiting_metadata_request/3,
         waiting_chunk_request/3,
         waiting_continue_or_stop/2,
         waiting_chunks/2,
         waiting_chunks/3,
         sending_remaining/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {from :: pid(),
                reader_pid :: pid(),
                bucket :: term(),
                bucket_id :: binary(),
                key :: term(),
                value_cache :: binary(),
                metadata_cache :: term(),
                chunk_queue :: term(),
                manifest :: term(),
                manifest_uuid :: term(),
                blocks_left :: list(),
                test=false :: boolean()}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Bucket, BucketId, Key) ->
    gen_fsm:start_link(?MODULE, [Bucket, BucketId, Key], []).

stop(Pid) ->
    gen_fsm:send_event(Pid, stop).

continue(Pid) ->
    gen_fsm:send_event(Pid, continue).

get_metadata(Pid) ->
    gen_fsm:sync_send_event(Pid, get_metadata, 60000).

get_next_chunk(Pid) ->
    gen_fsm:sync_send_event(Pid, get_next_chunk, 60000).

manifest(Pid, ManifestValue) ->
    gen_fsm:send_event(Pid, {object, self(), ManifestValue}).

chunk(Pid, ChunkSeq, ChunkValue) ->
    gen_fsm:send_event(Pid, {chunk, self(), {ChunkSeq, ChunkValue}}).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([Bucket, BucketId, Key]) ->
    %% we want to trap exits because
    %% `erlang:link` isn't atomic, and
    %% since we're starting the reader
    %% through a supervisor we can't use
    %% `spawn_link`. If the process has already
    %% died before we call link, we'll get
    %% an exit Reason of `noproc`
    process_flag(trap_exit, true),


    Queue = queue:new(),
    State = #state{bucket=Bucket, bucket_id=BucketId, key=Key,
                   chunk_queue=Queue},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0};
init([test, Bucket, BucketId, Key, ContentLength, BlockSize]) ->
    {ok, prepare, State1, 0} = init([Bucket, BucketId, Key]),
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, ReaderPid} = riak_moss_dummy_reader:start_link([self(), ContentLength, BlockSize]),
    link(ReaderPid),
    riak_moss_reader:get_manifest(ReaderPid, Bucket, Key),
    {ok, waiting_value, State1#state{reader_pid=ReaderPid, test=true}}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, #state{bucket=Bucket, key=Key}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    {ok, ReaderPid} = riak_moss_reader_sup:start_reader(node(), self()),
    link(ReaderPid),
    riak_moss_reader:get_manifest(ReaderPid, Bucket, Key),
    {next_state, waiting_value, State#state{reader_pid=ReaderPid}}.

waiting_value(get_metadata, From, State) ->
    NewState = State#state{from=From},
    {next_state, waiting_value, NewState}.

waiting_value({object, _Pid, Reply}, #state{from=From}=State) ->
    %% determine if the object is a normal
    %% object, or a manifest object
    NextStateTimeout = 60000,
    ReturnMeta = case Reply of
        {error, notfound} ->
            NewState = State,
            notfound;
        {ok, Value} ->
            RawValue = riakc_obj:get_value(Value),
            case riak_moss_lfs_utils:is_manifest(RawValue) of
                false ->
                    %% TODO:
                    %% we don't deal with siblings here
                    %% at all
                    Metadata = riakc_obj:get_metadata(Value),
                    %% @TODO
                    %% content length is a hack now
                    %% because we're forcing everything
                    %% to be a manifest
                    Mfst = undefined,
                    ContentLength = 0,
                    ContentMd5 = <<>>,
                    NewState = State#state{value_cache=RawValue};
                true ->
                    Mfst = binary_to_term(RawValue),
                    Metadata = riak_moss_lfs_utils:metadata_from_manifest(Mfst),
                    ContentLength = riak_moss_lfs_utils:content_length(Mfst),
                    ContentMd5 = riak_moss_lfs_utils:content_md5(Mfst),
                    NewState = State#state{manifest=Mfst,
                                           manifest_uuid=riak_moss_lfs_utils:file_uuid(Mfst)}
            end,
            lists:foldl(
              fun({K, V}, Dict) -> dict:store(K, V, Dict) end,
              Metadata,
              if Mfst == undefined ->
                      [];
                 true ->
                      LastModified = riak_moss_wm_utils:to_rfc_1123(
                                         riak_moss_lfs_utils:created(Mfst)),
                      [{"last-modified", LastModified}]
              end ++
                  [{"content-type", riakc_obj:get_content_type(Value)},
                   {"content-md5", ContentMd5},
                   {"content-length", ContentLength}])
    end,

    NextState = case From of
        undefined ->
            waiting_metadata_request;
        _ ->
            gen_fsm:reply(From, ReturnMeta),
            waiting_continue_or_stop
    end,
    {next_state, NextState, NewState#state{from=undefined, metadata_cache=ReturnMeta}, NextStateTimeout}.

waiting_metadata_request(get_metadata, _From, #state{metadata_cache=Metadata}=State) ->
    {reply, Metadata, waiting_continue_or_stop, State#state{metadata_cache=undefined}}.

waiting_continue_or_stop(timeout, State) ->
    {stop, normal, State};
waiting_continue_or_stop(stop, State) ->
    {stop, normal, State};
waiting_continue_or_stop(continue, #state{value_cache=CachedValue,
                                          manifest=Manifest,
                                          bucket_id=BucketId,
                                          key=Key,
                                          manifest_uuid=UUID,
                                          reader_pid=ReaderPid}=State) ->
    case CachedValue of
        undefined ->
            BlockSequences = riak_moss_lfs_utils:block_sequences_for_manifest(Manifest),
            case BlockSequences of
                [] ->
                    %% No blocks = empty file
                    {next_state, waiting_chunk_request, State};
                    %% {next_state, waiting_chunk_request,
                    %%  State#state{value_cache = <<>>}};
                [_|_] ->
                    BlocksLeft = sets:from_list(BlockSequences),

                    %% start retrieving the first block
                    riak_moss_reader:get_chunk(ReaderPid, BucketId, Key, UUID,
                                               hd(BlockSequences)),
                    {next_state, waiting_chunks, State#state{blocks_left=BlocksLeft}}
            end;
        _ ->
            %% we don't actually have to start
            %% retrieving chunks, as we already
            %% have the value cached in our State
            {next_state, waiting_chunk_request, State}
    end.

waiting_chunk_request(get_next_chunk, _From, #state{value_cache=CachedValue}=State) ->
    {stop, normal, CachedValue, State}.

waiting_chunks(get_next_chunk, From, #state{chunk_queue=ChunkQueue, from=PreviousFrom}=State) when PreviousFrom =:= undefined ->
    case queue:is_empty(ChunkQueue) of
        true ->
            %% we don't have a chunk ready
            %% yet, so we'll make note
            %% of the sender and go back
            %% into waiting for another
            %% chunk
            {next_state, waiting_chunks, State#state{from=From}};
        _ ->
            {{value, ToReturn}, NewQueue} = queue:out(ChunkQueue),
            {reply, ToReturn, waiting_chunks, State#state{chunk_queue=NewQueue}}
    end.

waiting_chunks({chunk, _Pid, {ChunkSeq, ChunkReturnValue}}, #state{from=From,
                                                            blocks_left=Remaining,
                                                            manifest_uuid=UUID,
                                                            key=Key,
                                                            bucket_id=BucketId,
                                                            reader_pid=ReaderPid,
                                                            chunk_queue=ChunkQueue}=State) ->
    %% TODO:
    %% we don't deal with missing chunks
    %% at all here, so this pattern
    %% match will fail
    {ok, ChunkRiakObject} = ChunkReturnValue,

    NewRemaining = sets:del_element(ChunkSeq, Remaining),

    %% we currently only care about the binary
    %% data in the object
    ChunkValue = riakc_obj:get_value(ChunkRiakObject),

    %% this is just to be used
    %% in a guard later
    Empty = queue:is_empty(ChunkQueue),

    case sets:size(NewRemaining) of
        0 ->
            case From of
                undefined ->
                    NewQueue = queue:in({done, ChunkValue}, ChunkQueue),
                    NewState = State#state{blocks_left=NewRemaining,
                                           chunk_queue=NewQueue},
                    {next_state, sending_remaining, NewState};
                _ when Empty ->
                    gen_fsm:reply(From, {done, ChunkValue}),
                    NewState = State#state{blocks_left=NewRemaining, from=undefined},
                    {stop, normal, NewState}
            end;
        _ ->
            riak_moss_reader:get_chunk(ReaderPid, BucketId, Key, UUID, hd(lists:sort(sets:to_list(NewRemaining)))),
            case From of
                undefined ->
                    NewQueue = queue:in({chunk, ChunkValue}, ChunkQueue),
                    NewState = State#state{blocks_left=NewRemaining,
                                           chunk_queue=NewQueue},
                    {next_state, waiting_chunks, NewState};
                _ ->
                    {ReplyChunk, NewQueue2} = case queue:is_empty(ChunkQueue) of
                        true ->
                            {{chunk, ChunkValue}, ChunkQueue};
                        _ ->
                            {{value, ChunkToReplyWith}, NewQueue} = queue:out(ChunkQueue),
                            {ChunkToReplyWith, NewQueue}
                    end,
                    gen_fsm:reply(From, ReplyChunk),
                    NewState = State#state{blocks_left=NewRemaining,
                                           chunk_queue=NewQueue2,
                                           from=undefined},
                    {next_state, waiting_chunks, NewState}
            end
    end.

sending_remaining(get_next_chunk, _From, #state{chunk_queue=ChunkQueue}=State) ->
    {{value, Item}, Queue} = queue:out(ChunkQueue),
    NewState = State#state{chunk_queue=Queue},
    case queue:is_empty(Queue) of
        true ->
            {stop, normal, Item, NewState};
        _ ->
            {reply, Item, sending_remaining, NewState}
    end.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
%% TODO:
%% we don't want to just
%% stop whenever a reader is
%% killed once we have some concurrency
%% in our readers. But since we just
%% have one reader process now, if it dies,
%% we have no reason to stick around
handle_info({'EXIT', ReaderPid, _Reason}, _StateName, StateData=#state{reader_pid=ReaderPid}) ->
    {stop, normal, StateData};
%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(_Reason, _StateName, #state{reader_pid=ReaderPid,test=false}) ->
    riak_moss_reader_sup:terminate_reader(node(), ReaderPid);
terminate(_Reason, _StateName, #state{reader_pid=ReaderPid,test=true}) ->
    exit(ReaderPid, normal).

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

test_link(Bucket, BucketId, Key, ContentLength, BlockSize) ->
    gen_fsm:start_link(?MODULE, [test, Bucket, BucketId, Key, ContentLength, BlockSize], []).

-endif.
