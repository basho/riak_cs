%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% get fsm for Riak Moss. 

-module(riak_moss_get_fsm).

-behaviour(gen_fsm).

-include("riak_moss.hrl").

-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

-export([start_link/3,
         prepare/2,
         waiting_value/2,
         normal_retriever/3,
         waiting_chunk_command/2,
         waiting_chunks/2]).

-record(state, {from :: pid(),
                bucket :: term(),
                key :: term(),
                value_cache :: binary(), 
                manifest :: term(),
                block_keys :: list()}).

start_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [From, Bucket, Key], []).

init([From, Bucket, Key]) ->
    State = #state{from=From, bucket=Bucket, key=Key},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, #state{bucket=Bucket, key=Key}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    spawn_link(?MODULE, normal_retriever, [self(), Bucket, Key]),
    {next_state, waiting_value, State}.

waiting_value({object, Value}, #state{from=From}=State) ->
    %% determine if the object is a normal
    %% object, or a manifest object
    case riak_moss_lfs_utils:is_manifest(Value) of

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
            From ! Metadata,
            {stop, normal, State#state{value_cache=CachedValue}};
        true ->
            Metadata = riak_moss_lfs_utils:metadata_from_manifest(Value),
            From ! Metadata,
            StateWithMani = State#state{manifest=Value},
            {next_state, waiting_chunk_command, StateWithMani}
    end.

waiting_chunk_command({stop, _}, State) ->
    {stop, normal, State};
waiting_chunk_command({continue, _}, State) ->
    %% TODO:
    %% now launch a process that
    %% will grab the chunks and
    %% start sending us
    %% chunk events
    {next_state, waiting_chunks, State}.

waiting_chunks({chunk, Chunk}, State) ->
    %% we're assuming that we're receiving the
    %% chunks synchronously, and that we can
    %% send them back to WM as we get them
    NewState = riak_moss_lfs_utils:remove_chunk(State, Chunk),
    case riak_moss_lfs_utils:still_waiting(NewState) of
        true ->
            {next_state, waiting_chunks, NewState};
        false ->
            {stop, normal, NewState}
    end.

%% @doc Retrieve the value at
%%      Bucket, Key, whether it's a
%%      manifest or regular object
normal_retriever(ReplyPid, Bucket, Key) ->
    {ok, RiakObject} = riak_moss_riakc:get_object(Bucket, Key),
    gen_fsm:send_event(ReplyPid, {object, RiakObject}).
        

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
