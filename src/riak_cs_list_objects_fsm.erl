%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc

-module(riak_cs_list_objects_fsm).

-behaviour(gen_fsm).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).


-record(state, {riakc_pid :: pid(),
                req :: list_object_request(),
                keys=[] :: list(),
                objects=[] :: list()}).

%%%===================================================================
%%% API
%%%===================================================================


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(_Args) ->
    %% just create the StateData
    {ok, prepare, #state{}, 0}.

prepare(timeout, State) ->
    %% make the list-keys request here
    {ok, waiting_list_keys, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, _StateName, State) ->
    {stop, unknown, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------


%% function to create a list keys request
