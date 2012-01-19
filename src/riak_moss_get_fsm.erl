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
-export([test_link/2]).

-endif.

-include("riak_moss.hrl").

%% API
-export([start_link/2,
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
         %sending_remaining/2,
         sending_remaining/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {from :: pid(),
                reader_pid :: pid(),
                bucket :: term(),
                key :: term(),
                value_cache :: binary(),
                metadata_cache :: term(),
                chunk_queue :: term(),
                manifest :: term(),
                manifest_uuid :: term(),
                blocks_left :: list()}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [Bucket, Key], []).

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

init([Bucket, Key]) ->
    Queue = queue:new(),
    State = #state{bucket=Bucket, key=Key,
                   chunk_queue=Queue},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0};
init([test, Bucket, Key, ContentLength]) ->
    {ok, prepare, State1, 0} = init([Bucket, Key]),
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, ReaderPid} = riak_moss_dummy_reader:start_link([self(), ContentLength, riak_moss_lfs_utils:block_size()]),
    riak_moss_reader:get_manifest(ReaderPid, Bucket, Key),
    {ok, waiting_value, State1#state{reader_pid=ReaderPid}}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, #state{bucket=Bucket, key=Key, reader_pid=ReaderPid}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    {ok, ReaderPid} = riak_moss_reader_sup:start_reader(node(), self()),
    %% TODO:
    %% we need to tell the reader gen_server
    %% about our pid() another way
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
            lager:error("the value is ~p", [RawValue]),
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
                    ContentLength = 0,
                    ContentMd5 = <<>>,
                    NewState = State#state{value_cache=RawValue};
                true ->
                    DecodedValue = binary_to_term(RawValue),
                    Metadata = riak_moss_lfs_utils:metadata_from_manifest(DecodedValue),
                    ContentLength = riak_moss_lfs_utils:content_length(DecodedValue),
                    ContentMd5 = riak_moss_lfs_utils:content_md5(DecodedValue),
                    NewState = State#state{manifest=DecodedValue,
                                           manifest_uuid=riak_moss_lfs_utils:file_uuid(DecodedValue)}
            end,
            Meta1 = dict:store("content-type",
                                    riakc_obj:get_content_type(Value),
                                    Metadata),
            Meta2 = dict:store("content-md5",
                                    ContentMd5,
                                    Meta1),
            Meta3 = dict:store("content-length",
                                    ContentLength,
                                    Meta2),
            Meta3
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
                                          bucket=BucketName,
                                          key=Key,
                                          manifest_uuid=UUID,
                                          reader_pid=ReaderPid}=State) ->
    case CachedValue of
        undefined ->
            BlockSequences = riak_moss_lfs_utils:block_sequences_for_manifest(Manifest),
            BlocksLeft = sets:from_list(BlockSequences),

            %% start retrieving the first block
            riak_moss_reader:get_chunk(ReaderPid, BucketName, Key, UUID, hd(BlockSequences)),
            {next_state, waiting_chunks, State#state{blocks_left=BlocksLeft}};
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

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
blocks_retriever(Pid, GetModule, BucketName, BlockKeys) ->
    Func =
        fun({ChunkSeq, ChunkName}) ->
                case GetModule:get_object(
                       riak_moss_utils:to_bucket_name(blocks, BucketName),
                       ChunkName) of
                    {ok, Value} ->
                        chunk(Pid, ChunkSeq, Value);
                    {error, Reason} ->
                        DeconstructedBlock =
                            riak_moss_lfs_utils:block_name_to_term(ChunkName),
                        lager:error("block ~p couldn't be retrieved with reason ~p",
                                    [DeconstructedBlock, Reason]),
                        exit(Reason)
                end
        end,
    lists:foreach(Func, BlockKeys).


%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

test_link(Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [test, Bucket, Key], []).

-endif.
