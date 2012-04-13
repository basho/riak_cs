%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_delete_fsm).

-behaviour(gen_fsm).

-include("riak_moss.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%% API
-export([start_link/3,
         prepare/2,
         deleting/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {bucket :: binary(),
                key :: binary(),
                manifest :: lfs_manifest(),
                delete_blocks_remaining :: ordset:ordset(integer()),
                mani_pid :: pid(),
                all_delete_works :: list(pid()),
                free_deleters :: ordsets:new()}).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_delete_fsm'.
start_link(Bucket, Key, Options) ->
    Args = [Bucket, Key, Options],
    gen_fsm:start_link(?MODULE, Args, []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @doc Initialize the fsm.
-spec init([binary() | timeout()]) ->
                  {ok, initialize, state(), 0} |
                  {ok, write_root, state()}.
init([_Bucket, _Key, _Options]) ->
    %% Set up our state record
    %% and try to register
    %% ourselves with gproc,
    %% failing if we can't
    {ok, prepare, #state{}, 0}.

prepare(timeout, State) ->
    %% Get the latest version
    %% of the manifest we're going
    %% to delete.
    %% TODO: a get specific manifest
    %% function needs to be written
    %% for the manifest fsm

    %% Based on the manifest,
    %% fill in the delete_blocks_remaining
    %% set and then launch N delete
    %% workers (ie. riak_moss_block_server)

    %% start the save_manifest
    %% timer
    {next_state, deleting, State}.

deleting(_Event, State) ->
    %% We just received a delete
    %% acknowledgement, so depending
    %% on the value of delete_blocks_remaining
    %% (after removing this element,
    %% stay in the deleting state,
    %% or stop.
    %% If stopping, we should also
    %% sync wait to update the manifest.
    %% Cancel the timer

    %% or
    %% {stop, normal, State}.
    {next_state, deleting, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.
>>>>>>> Skeleton for new delete fsm

handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

handle_info(save_manifest, StateName, State=#state{mani_pid=ManiPid,
                                                   manifest=Manifest}) ->
    %% 1. save the manifest

    %% TODO:
    %% are there any times where
    %% we should be cancelling the
    %% timer here, depending on the
    %% state we're in?
    riak_moss_manifest_fsm:update_manifest(ManiPid, Manifest),
    {next_state, StateName, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

-endif.
