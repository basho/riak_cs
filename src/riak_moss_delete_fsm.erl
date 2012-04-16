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
-export([start_link/5,
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
                uuid :: binary(),
                manifest :: lfs_manifest(),
                riakc_pid :: pid(),
                delete_blocks_remaining :: ordsets:ordset(integer()),
                unacked_deletes=ordsets:new(),
                mani_pid :: pid(),
                all_delete_works :: list(pid()),
                free_deleters :: ordsets:new()}).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_delete_fsm'.
start_link(Bucket, Key, UUID, RiakcPid, Options) ->
    Args = [Bucket, Key, UUID, RiakcPid, Options],
    gen_fsm:start_link(?MODULE, Args, []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([Bucket, Key, UUID, RiakcPid, _Options]) ->
    %% Set up our state record
    %% and try to register
    %% ourselves with gproc,
    %% failing if we can't
    {ok, prepare, #state{bucket=Bucket,
                         key=Key,
                         uuid=UUID,
                         riakc_pid=RiakcPid},
                     0}.

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

deleting({block_delete, BlockID, DeleterPid},
    State=#state{manifest=Manifest=#lfs_manifest_v2{
                    delete_blocks_remaining=ManifestDeleteBlocksRemaining},
                 free_deleters=FreeDeleters,
                 unacked_deletes=UnackedDeletes}) ->

    NewFreeDeleters = ordsets:add_element(DeleterPid, FreeDeleters),
    NewUnackedDeletes = ordsets:del_element(BlockID, UnackedDeletes),

    NewDeleteBlocksRemaining = ordsets:del_element(BlockID,
        ManifestDeleteBlocksRemaining),

    NewManifest = Manifest#lfs_manifest_v2{delete_blocks_remaining=NewDeleteBlocksRemaining},

    State2 = State#state{free_deleters=NewFreeDeleters,
                         unacked_deletes=NewUnackedDeletes,
                         manifest=NewManifest},

    if
        NewDeleteBlocksRemaining==[] andalso NewUnackedDeletes==[] ->
            %% do stop things
            %% stop timer
            State3 = finish(State2),
            {stop, normal, State3};
        true ->
            %% maybe start more deletes
            State3 = maybe_delete_block(State2),
            {next_state, deleting, State3}
    end.

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

maybe_delete_block(State=#state{free_deleters=[]}) ->
    State;
maybe_delete_block(State=#state{bucket=Bucket,
                                key=Key,
                                uuid=UUID,
                                free_deleters=[DeleterPid | _RestDeleters],
                                unacked_deletes=UnackedDeletes,
                                manifest=#lfs_manifest_v2{
                                    delete_blocks_remaining=DeleteBlocksRemaining
                                    = [BlockID | _RestBlocks]}}) ->
    NewUnackedDeletes = ordsets:add_element(BlockID, UnackedDeletes),
    NewDeleteBlocksRemaining = ordsets:del_element(BlockID, DeleteBlocksRemaining),
    %% start the delete...
    riak_moss_block_server:delete_block(DeleterPid, Bucket, Key, UUID, BlockID),
    State#state{unacked_deletes=NewUnackedDeletes,
                delete_blocks_remaining=NewDeleteBlocksRemaining}.

finish(_State) ->
    ok.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

-endif.
