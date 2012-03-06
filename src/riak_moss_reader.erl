%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_reader).

-behaviour(gen_server).

-include("riak_moss.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0]).

-endif.

%% API
-export([start_link/1,
         get_manifest/3,
         get_chunk/5]).

%% gen_server callbacks
-export([init/1,
         init/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {riakc_pid :: pid(),
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

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([pid()] | {test, [pid()]}) -> {ok, state()} | {stop, term()}.
init([CallerPid]) ->
    %% Ensure the the terminate callback is called if the
    %% supervisor shuts us down.
    process_flag(trap_exit, true),

    %% Get a connection to riak
    case riak_moss_utils:riak_connection() of
        {ok, RiakPid} ->
            {ok, #state{riakc_pid=RiakPid,
                        caller_pid=CallerPid}};
        {error, Reason} ->
            lager:error("Failed to establish connection to Riak. Reason: ~p",
                        [Reason]),
            {stop, riak_connect_failed}
    end.
init(test, CallerPid) ->
    init([CallerPid]).

%% @doc Unused
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({get_manifest, Bucket, Key}, #state{riakc_pid=RiakcPid, caller_pid=CallerPid}=State) ->
    PrefixedBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    ManifestValue = riakc_pb_socket:get(RiakcPid, PrefixedBucket, Key),
    riak_moss_get_fsm:manifest(CallerPid, ManifestValue),
    {noreply, State};
handle_cast({get_chunk, Bucket, Key, UUID, ChunkSeq}, #state{riakc_pid=RiakcPid, caller_pid=CallerPid}=State) ->
    PrefixedBucket = riak_moss_utils:to_bucket_name(blocks, Bucket),
    BlockKey = riak_moss_lfs_utils:block_name(Key, UUID, ChunkSeq),
    ChunkValue = riakc_pb_socket:get(RiakcPid, PrefixedBucket, BlockKey),
    riak_moss_get_fsm:chunk(CallerPid, ChunkSeq, ChunkValue),
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
terminate(_Reason, #state{riakc_pid=RiakcPid}) ->
    riak_moss_utils:close_riak_connection(RiakcPid).

%% @doc Unused.
-spec code_change(term(), state(), term()) ->
                         {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start a `riak_moss_reader' for testing.
-spec test_link() -> {ok, pid()} | {error, term()}.
test_link() ->
    gen_server:start_link(?MODULE, test, []).

-endif.
