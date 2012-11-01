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
         waiting_list_keys/2,
         waiting_map_reduce/2,
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

waiting_list_keys(_Event, State) ->
    %% if we get an error while we're in this state,
    %% we still have the option to return `HTTP 500'
    %% to the client, since we haven't written
    %% any bytes yet


    %% depending on whether we're done listing keys
    %% or not, transition back into `waiting_list_keys'
    %% or make a m/r request and head into `waiting_map_reduce'.
    _ = {next_state, waiting_list_keys, State},
    {next_state, waiting_map_reduce, State}.

waiting_map_reduce(_Event, State) ->
    %% depending on whether we have enough results yet
    %% (some of the manifests we requested may be
    %% marked as delete) we will either have to
    %% make another map-reduce request.


    _ = {next_state, waiting_map_reduce, State},
    %% reply the results if we do end up being done
    {stop, normal, State}.

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
