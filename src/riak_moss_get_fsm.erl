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
-export([test_link/3]).

-endif.

-include("riak_moss.hrl").

%% API
-export([start_link/3,
         stop/1,
         continue/1,
         get_metadata/1,
         get_next_chunk/1]).

%% exported to be used by
%% spawn_link
-export([normal_retriever/4,
         blocks_retriever/4]).

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
                bucket :: term(),
                key :: term(),
                value_cache :: binary(), 
                metadata_cache :: term(), 
                chunk_queue :: term(),
                reply_pid :: pid(),
                manifest :: term(),
                blocks_left :: list(),
                get_module :: module()}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [From, Bucket, Key], []).

stop(Pid) ->
    gen_fsm:send_event(Pid, stop).

continue(Pid) ->
    gen_fsm:send_event(Pid, continue).

get_metadata(Pid) ->
    gen_fsm:sync_send_event(Pid, get_metadata).

get_next_chunk(Pid) ->
    gen_fsm:sync_send_event(Pid, get_next_chunk).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([_From, Bucket, Key]) ->
    Queue = queue:new(),
    State = #state{bucket=Bucket, key=Key,
                   get_module=riak_moss_riakc,
                   chunk_queue=Queue},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0};
init([test, From, Bucket, Key]) ->
    {ok, prepare, State1, 0} = init([From, Bucket, Key]),
    State2 = State1#state{get_module=riak_moss_dummy_gets},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State2, 0}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, #state{bucket=Bucket, key=Key, get_module=GetModule}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    spawn_link(?MODULE, normal_retriever, [self(), GetModule, Bucket, Key]),
    {next_state, waiting_value, State}.

waiting_value(get_metadata, From, State) ->
    NewState = State#state{from=From},
    {next_state, waiting_value, NewState}.

waiting_value({object, Value}, #state{from=From}=State) ->
    %% determine if the object is a normal
    %% object, or a manifest object
    RawValue = riakc_obj:get_value(Value),
    %% TODO:
    %% put binary_to_term in a catch statement
    DecodedValue = binary_to_term(RawValue),
    NextStateTimeout = 60000,
    case riak_moss_lfs_utils:is_manifest(DecodedValue) of
        false ->
            %% TODO:
            %% we don't deal with siblings here
            %% at all
            Metadata = riakc_obj:get_metadata(Value),
            NewState = State#state{value_cache=RawValue,
                                    metadata_cache=Metadata};
        true ->
            Metadata = riak_moss_lfs_utils:metadata_from_manifest(DecodedValue),
            NewState = State#state{manifest=DecodedValue,
                                    metadata_cache=Metadata}
    end,
    NextState = case From of
        undefined ->
            waiting_metadata_request;
        _ ->
            gen_fsm:reply(From, Metadata),
            waiting_continue_or_stop
    end,
    {next_state, NextState, NewState#state{from=undefined}, NextStateTimeout}.

waiting_metadata_request(get_metadata, _From, #state{metadata_cache=Metadata}=State) ->
    {reply, Metadata, waiting_continue_or_stop, State#state{metadata_cache=undefined}}.

waiting_continue_or_stop(timeout, State) ->
    {stop, normal, State};
waiting_continue_or_stop(stop, State) ->
    {stop, normal, State};
waiting_continue_or_stop(continue, #state{value_cache=CachedValue,
                                       manifest=Manifest,
                                       bucket=BucketName,
                                       get_module=GetModule}=State) ->
    case CachedValue of
        undefined ->
            %% TODO:
            %% now launch a process that
            %% will grab the chunks and
            %% start sending us
            %% chunk events
            BlockKeys = riak_moss_lfs_utils:initial_block_keynames(Manifest),
            BlocksLeft = sets:from_list([X || {X, _} <- BlockKeys]),
            spawn_link(?MODULE, blocks_retriever, [self(), GetModule, BucketName, BlockKeys]),
            {next_state, waiting_chunks, State#state{blocks_left=BlocksLeft}};
        _ ->
            %% we don't actually have to start
            %% retrieving chunks, as we already
            %% have the value cached in our State
            {next_state, waiting_chunk_request, State}
    end.

waiting_chunk_request(get_next_chunk, _From, #state{value_cache=CachedValue}=State) ->
    {stop, normal, CachedValue, State}.

waiting_chunks(get_next_chunk, From, #state{chunk_queue=ChunkQueue, from=PreviousFrom}=State) ->
    ?assertEqual(PreviousFrom, undefined),
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

waiting_chunks({chunk, {ChunkSeq, ChunkRiakObject}}, #state{from=From,
                                                            blocks_left=Remaining,
                                                            chunk_queue=ChunkQueue}=State) ->
    NewRemaining = sets:del_element(ChunkSeq, Remaining),

    %% we currently only care about the binary
    %% data in the object
    ChunkValue = riakc_obj:get_value(ChunkRiakObject),

    case sets:size(NewRemaining) of
        0 ->
            case From of
                undefined ->
                    NewQueue = queue:in({done, ChunkValue}, ChunkQueue),
                    NewState = State#state{blocks_left=NewRemaining,
                                           chunk_queue=NewQueue},
                    {next_state, sending_remaining, NewState};
                _ ->
                    ?assert(queue:is_empty(ChunkQueue)),
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
riak_object(Pid, Object) ->
    gen_fsm:send_event(Pid, {object, Object}).

%% @private
chunk(Pid, ChunkSeq, ChunkValue) ->
    gen_fsm:send_event(Pid, {chunk, {ChunkSeq, ChunkValue}}).

%% @private 
%% Retrieve the value at
%% Bucket, Key, whether it's a
%% manifest or regular object
normal_retriever(ReplyPid, GetModule, Bucket, Key) ->
    {ok, RiakObject} = GetModule:get_object(riak_moss:to_bucket_name(objects, Bucket), Key),
    riak_object(ReplyPid, RiakObject).

%% @private
blocks_retriever(Pid, GetModule, BucketName, BlockKeys) ->
    Func = fun({ChunkSeq, ChunkName}) ->
        {ok, Value} = GetModule:get_object(riak_moss:to_bucket_name(blocks, BucketName), ChunkName),
        chunk(Pid, ChunkSeq, Value)
    end,
    lists:foreach(Func, BlockKeys).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

test_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [test, From, Bucket, Key], []).

-endif.
