%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_put_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         write_root/2,
         write_chunks/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).


-record(state, {timeout :: timeout()}).
-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a riak_moss_put_fsm
-spec start_link([term()]) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_fsm:start_link(?MODULE, [Args], []).


%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @doc Initialize the fsm.
-spec init([timeout()]) ->
                  {ok, initialize, state(), 0}.
               %% | {stop, {init_failed, term(), term()}}.
init([Timeout]) ->
    State = #state{timeout=Timeout},
    {ok, initialize, State, 0}.

%% @doc First state of the put fsm
initialize(timeout, State=#state{timeout=Timeout}) ->
    {next_state, waiting_results, State, Timeout}.

%% @doc @TODO
write_root(_Data, State=#state{timeout=Timeout}) ->
    {next_state, write_chunks, State, Timeout}.

%% @doc @TODO
write_chunks(_Data, State=#state{timeout=Timeout}) ->
    {next_state, write_root, State, Timeout}.

%% @doc Unused.
-spec handle_event(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Unused.
-spec handle_sync_event(term(), term(), atom(), state()) ->
         {reply, ok, atom(), state()}.
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @doc @TODO
-spec handle_info(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_info({'EXIT', _Pid, _Reason}, StateName, State=#state{timeout=Timeout}) ->
    {next_state, StateName, State, Timeout};
handle_info({_ReqId, {ok, _Pid}},
            StateName,
            State=#state{timeout=Timeout}) ->
    {next_state, StateName, State, Timeout};
handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Unused.
-spec terminate(term(), atom(), state()) -> ok.
terminate(Reason, _StateName, _State) ->
    Reason.

%% @doc Unused.
-spec code_change(term(), atom(), state(), term()) ->
         {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

