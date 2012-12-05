%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc

-module(riak_cs_list_objects_ets_cache).

-behaviour(gen_server).
-include("riak_cs.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tid :: ets:tid()}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    %% named proc
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(list()) -> {ok, state()} | {error, term()}.
init([]) ->
    Tid = new_table(),
    NewState = #state{tid=Tid},
    {ok, NewState}.

handle_call(get_tid, _From, State=#state{tid=Tid}) ->
    {reply, Tid, State};
handle_call(Msg, _From, State) ->
    lager:debug("got unknown message: ~p", [Msg]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec new_table() -> ets:tid().
new_table() ->
    TableSpec = [public, set, {write_concurrency, true}],
    ets:new(list_objects_cache, TableSpec).
