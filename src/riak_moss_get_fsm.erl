%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% get fsm for Riak Moss. 

-module(riak_moss_get_fsm).

-behaviour(gen_fsm).

-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

-export([start_link/3,
         prepare/2,
         waiting_value/2,
         retriever/3,
         waiting_chunks/2]).

start_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [From, Bucket, Key], []).

init([From, Bucket, Key]) ->
    %% TODO:
    %% turn this into a real
    %% record
    State = {From, Bucket, Key},
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, {_ReplyPid, Bucket, Key}=State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    spawn_link(?MODULE, retriever, [self(), Bucket, Key]),
    {next_state, waiting_value, State}.

waiting_value({object, Value}, State) ->
    %% determine if the object is a normal
    %% object, or a manifest object
    case object_or_manifest(Value) of
        object ->
            %% send object back to
            %% the `from` part of
            %% state
            {stop, done, State};
        manifest ->
            %% now launch a process that
            %% will grab the chunks and
            %% start sending us
            %% chunk events
            {next_state, waiting_chunks, State}
    end.

waiting_chunks({chunk, Chunk}, State) ->
    %% we're assuming that we're receiving the
    %% chunks synchronously, and that we can
    %% send them back to WM as we get them
    NewState = remove_chunk(State, Chunk),
    case still_waiting(NewState) of
        true ->
            {next_state, waiting_chunks, NewState};
        false ->
            {stop, done, NewState}
    end.

%% @doc Grabs the object for a key and
%%      checks to see if it's a manifest or
%%      a regular object. If it's a regular
%%      object, return it. If it's a manifest,
%%      start grabbing the chunks and returning them.
retriever(ReplyPid, Bucket, Key) ->
    gen_fsm:send_event(ReplyPid,
        riak_moss_riakc:get_object(Bucket, Key)).

%% @doc Returns whether or not
%%      a value is a normal object,
%%      or a manifest document
object_or_manifest(_Value) ->
    ok.

%% @doc Remove a chunk from the
%%      chunks field of State
remove_chunk(_State, _Chunk) ->
    ok.

%% @doc Return true or false
%%      depending on whether
%%      we're still waiting
%%      to accumulate more chunks
still_waiting(_State) ->
    ok.

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
