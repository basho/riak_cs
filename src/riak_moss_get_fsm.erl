%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% get fsm for Riak Moss. 

-module(riak_moss_get_fsm).

-behaviour(gen_fsm).

-export([start_link/3,
         init/1,
         prepare/2,
         waiting_value/2,
         waiting_chunks/2]).

start_link(From, Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [From, Bucket, Key], []).

init([From, Bucket, Key]) ->
    State = From,
    %% purposely have the timeout happen
    %% so that we get called in the prepare
    %% state
    {ok, prepare, State, 0}.

%% TODO:
%% could this func use
%% use a better name?
prepare(timeout, State) ->
    %% start the process that will
    %% fetch the value, be it manifest
    %% or regular object
    ok.

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
            {ok, waiting_chunks, State}
    end.

waiting_chunks({chunk, Chunk}, State) ->
    %% we're assuming that we're receiving the
    %% chunks synchronously, and that we can
    %% send them back to WM as we get them
    NewState = remove_chunk(State, Chunk),
    case still_waiting(NewState) of
        true ->
            {ok, waiting_chunks, NewState};
        false ->
            {stop, done, NewState}
    end.

%% @doc Grabs the object for a key and
%%      checks to see if it's a manifest or
%%      a regular object. If it's a regular
%%      object, return it. If it's a manifest,
%%      start grabbing the chunks and returning them.
retriever(ReplyPid, Bucket, Key) ->
    ok.

%% @doc Returns whether or not
%%      a value is a normal object,
%%      or a manifest document
object_or_manifest(Value) ->
    ok.

%% @doc Remove a chunk from the
%%      chunks field of State
remove_chunk(State, Chunk) ->
    ok.

%% @doc Return true or false
%%      depending on whether
%%      we're still waiting
%%      to accumulate more chunks
still_waiting(State) ->
    ok.
