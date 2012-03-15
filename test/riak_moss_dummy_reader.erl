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
         get_manifest/1,
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
                bucket :: binary(),
                key :: binary(),
                caller_pid :: pid()}).

-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_reader'.
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

get_manifest(Pid) ->
    gen_server:call(Pid, get_manifest).

get_chunk(Pid, Bucket, Key, UUID, ChunkSeq) ->
    gen_server:cast(Pid, {get_chunk, Bucket, Key, UUID, ChunkSeq}).

%% TODO:
%% Add shutdown public function

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([pid()] | {test, [pid()]}) -> {ok, state()} | {stop, term()}.
init([CallerPid, Bucket, Key, ContentLength, BlockSize]) ->
    %% Get a connection to riak
    {ok, #state{content_length=ContentLength,
                bucket=Bucket,
                key=Key,
                block_size=BlockSize,
                caller_pid=CallerPid}}.

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(get_manifest, _From, #state{bucket=Bucket,
                                               key=Key,
                                               content_length=ContentLength}=State) ->
    Manifest = riak_moss_lfs_utils:new_manifest(Bucket,
                                                Key,
                                                <<"uuid">>,
                                                ContentLength,
                                                "application/test",
                                                <<"md5">>,
                                                orddict:new(),
                                                riak_moss_lfs_utils:block_size()),
    {reply, {ok, Manifest}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({get_block, _From, Bucket, Key, UUID, BlockNumber}, #state{caller_pid=CallerPid}=State) ->
    FakeData = <<BlockNumber:32/integer>>,
    RiakObject = riakc_obj:new_obj(Bucket,riak_moss_lfs_utils:block_name(Key, UUID, BlockNumber), [], [{dict:new(),FakeData}]),
    riak_moss_get_fsm:chunk(CallerPid, BlockNumber, {ok, RiakObject}),
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
