%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to write data to Riak.

-module(riak_moss_writer).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_writer'.
-spec start_link([term()]) ->
                        {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%% @doc Initialize the server.
-spec init([term()]) -> {ok, state()} | {stop, term()}.
init(Args) ->
    {ok, #state{}}.

%% @doc @TODO
-spec handle_call(term(), {pid(), term()}, state()) ->
                         {reply, ok, state()}.
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

%% @doc @TODO
-spec handle_cast(term(), state()) ->
                         {noreply, state()}.
handle_cast(_Event, State) ->
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
