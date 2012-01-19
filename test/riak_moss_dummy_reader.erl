%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_dummy_reader).

-behaviour(gen_server).

-include("riak_moss.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%% API
-export([start_link/1,
         get_manifest/3,
         get_chunk/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {content_length :: integer(),
                block_size :: integer(),
                caller_pid :: pid()}).

-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_reader'.
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_manifest(Pid, Bucket, Key) ->
    gen_server:cast(Pid, {get_manifest, Bucket, Key}).

get_chunk(Pid, Bucket, Key, UUID, ChunkSeq) ->
    gen_server:cast(Pid, {get_chunk, Bucket, Key, UUID, ChunkSeq}).

%% TODO:
%% Add shutdown public function

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([pid()] | {test, [pid()]}) -> {ok, state()} | {stop, term()}.
init([CallerPid, ContentLength, BlockSize]) ->
    %% Get a connection to riak
    {ok, #state{content_length=ContentLength,
                block_size=BlockSize,
                caller_pid=CallerPid}}.

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({get_manifest, Bucket, Key}, #state{content_length=ContentLength,
                                                caller_pid=CallerPid}=State) ->
    Manifest = riak_moss_lfs_utils:new_manifest(Bucket, Key,
                                                <<"uuid">>,
                                                ContentLength,
                                                <<"md5">>,
                                                dict:new()),
    RiakObject = riakc_obj:new_obj(Bucket,Key, [], [{dict:new(), term_to_binary(Manifest)}]),
    riak_moss_get_fsm:manifest(CallerPid, RiakObject),
    {noreply, State};
handle_cast({get_chunk, Bucket, Key, UUID, ChunkSeq}, #state{caller_pid=CallerPid, block_size=BlockSize}=State) ->
    FakeData = list_to_binary(lists:seq(1, BlockSize)),
    RiakObject = riakc_obj:new_obj(Bucket,riak_moss_lfs_utils:block_name(Key, UUID, ChunkSeq, [], [{dict:new(),FakeData}])),
    riak_moss_get_fsm:chunk(CallerPid, ChunkSeq, RiakObject),
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
