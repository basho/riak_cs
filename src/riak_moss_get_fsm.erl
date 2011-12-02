%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% get fsm for Riak Moss. 

-module(riak_moss_get_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/3]).

-endif.


-behaviour(gen_fsm).

-include("riak_moss.hrl").

-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

-export([start_link/3,
         stop/1,
         continue/1,
         prepare/2,
         normal_retriever/4,
         blocks_retriever/3,
         waiting_value/2,
         waiting_chunk_command/2,
         waiting_chunks/2]).

-record(state, {from :: pid(),
                bucket :: term(),
                key :: term(),
                value_cache :: binary(), 
                manifest :: term(),
                blocks_left :: list(),
                get_module :: module()}).

start_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [From, Bucket, Key], []).

riak_object(Pid, Object) ->
    gen_fsm:send_event(Pid, {object, Object}).

chunk(Pid, ChunkSeq, ChunkValue) ->
    gen_fsm:send_event(Pid, {chunk, {ChunkSeq, ChunkValue}}).

stop(Pid) ->
    gen_fsm:send_event(Pid, stop).

continue(Pid) ->
    gen_fsm:send_event(Pid, continue).

init([From, Bucket, Key]) ->
    State = #state{from=From, bucket=Bucket, key=Key,
                   get_module=riak_moss_riakc},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0};
init([test, From, Bucket, Key]) ->
    State = #state{from=From, bucket=Bucket, key=Key,
                   get_module=riak_moss_dummy_gets},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, #state{bucket=Bucket, key=Key, get_module=GetModule}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    spawn_link(?MODULE, normal_retriever, [self(), GetModule, Bucket, Key]),
    {next_state, waiting_value, State}.

waiting_value({object, Value}, #state{from=From}=State) ->
    %% determine if the object is a normal
    %% object, or a manifest object
    DecodedValue = binary_to_term(riakc_obj:get_value(Value)),
    %%DecodedValue = riakc_obj:get_value(Value),
    case riak_moss_lfs_utils:is_manifest(DecodedValue) of

    %% TODO:
    %% create a shared func for sending messages
    %% back to `From`. Each of these `From ! Metadata`
    %% calls shouldn't be concerned with the exact
    %% message format
        false ->
            %% send metadata back to
            %% the `from` part of
            %% state
            %% TODO:
            %% we don't deal with siblings here
            %% at all
            Metadata = riakc_obj:get_metadata(Value),
            CachedValue = riakc_obj:get_value(Value),
            From ! {metadata, Metadata},
            {next_state, waiting_chunk_command, State#state{value_cache=CachedValue}};
        true ->
            Metadata = riak_moss_lfs_utils:metadata_from_manifest(DecodedValue),
            From ! {metadata, Metadata},
            StateWithMani = State#state{manifest=DecodedValue},
            {next_state, waiting_chunk_command, StateWithMani}
    end.

waiting_chunk_command(stop, State) ->
    {stop, normal, State};
waiting_chunk_command(continue, #state{from=From,
                                            value_cache=CachedValue,
                                            manifest=Manifest,
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
            spawn_link(?MODULE, blocks_retriever, [self(), GetModule, BlockKeys]),
            {next_state, waiting_chunks, State#state{blocks_left=BlocksLeft}};
        _ ->
            %% we don't actually have to start
            %% retrieving chunks, as we already
            %% have the value cached in our State
            From ! {done, CachedValue},
            {stop, normal, State}
    end.

waiting_chunks({chunk, {ChunkSeq, ChunkValue}}, #state{from=From, blocks_left=Remaining}=State) ->
    %% we're assuming that we're receiving the
    %% chunks synchronously, and that we can
    %% send them back to WM as we get them
    NewRemaining = sets:del_element(ChunkSeq, Remaining),
    NewState = State#state{blocks_left=NewRemaining},
    case sets:size(NewRemaining) of
        0 ->
            From ! {done, ChunkValue},
            {stop, normal, NewState};
        _ ->
            From ! {chunk, ChunkValue},
            {next_state, waiting_chunks, NewState}
    end.

%% @doc Retrieve the value at
%%      Bucket, Key, whether it's a
%%      manifest or regular object
normal_retriever(ReplyPid, GetModule, Bucket, Key) ->
    {ok, RiakObject} = GetModule:get_object(Bucket, Key),
    riak_object(ReplyPid, RiakObject).

blocks_retriever(Pid, GetModule, BlockKeys) ->
    Func = fun({ChunkSeq, ChunkName}) ->
        %% TODO:
        %% replace the chunk_bucket
        %% with a real bucket name
        {ok, Value} = GetModule:get_object(<<"filebucket">>, ChunkName),
        chunk(Pid, ChunkSeq, Value)
    end,
    lists:foreach(Func, BlockKeys).

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

-ifdef(TEST).

test_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [test, From, Bucket, Key], []).

-endif.
