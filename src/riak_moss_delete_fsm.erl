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
         block_deleted/2]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         deleting/2,
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
                timer_ref :: timer:tref(),
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

-spec block_deleted(pid(), {ok, integer()} | {error, binary()}) -> ok.
block_deleted(Pid, Response) ->
    gen_fsm:send_event(Pid, {block_deleted, Response, self()}).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([Bucket, Key, UUID, RiakcPid, _Options]) ->
    try gproc:add_local_name(UUID) of
        true ->
            {ok, prepare, #state{bucket=Bucket,
                                 key=Key,
                                 uuid=UUID,
                                 riakc_pid=RiakcPid},
                             0}
    catch error:badarg ->
        %% TODO:
        %% should the exit
        %% reason here be normal?
        {stop, normal}
    end.

prepare(timeout, State=#state{bucket=Bucket,
                              key=Key,
                              uuid=UUID,
                              riakc_pid=RiakcPid}) ->

    ManiPid = riak_moss_manifest_fsm:start_link(Bucket, Key, RiakcPid),

    %% TODO:
    %% handle the case where the manifest
    %% is not found
    {ok, Manifest} = riak_moss_manifest_fsm:get_specific_manifest(UUID),

    %% Based on the manifest,
    %% fill in the delete_blocks_remaining
    {NewManifest, BlocksToDelete} = blocks_to_delete_from_manifest(Manifest),

    AllDeleteWorkers =
    riak_moss_block_server:start_block_servers(RiakcPid,
        riak_moss_lfs_utils:delete_concurrency()),

    %% start the save_manifest
    %% timer
    %% TODO:
    %% this time probably
    %% shouldn't be hardcoded,
    %% and if it is, what should
    %% it be?
    {ok, TRef} = timer:send_interval(60000, self(), save_manifest),

    {next_state, deleting, State#state{mani_pid=ManiPid,
                                       manifest=NewManifest,
                                       all_delete_works=AllDeleteWorkers,
                                       delete_blocks_remaining=BlocksToDelete,
                                       timer_ref=TRef}}.

deleting({block_deleted, {ok, BlockID}, DeleterPid},
    State=#state{manifest=Manifest,
                 free_deleters=FreeDeleters,
                 unacked_deletes=UnackedDeletes}) ->

    NewFreeDeleters = ordsets:add_element(DeleterPid, FreeDeleters),
    NewUnackedDeletes = ordsets:del_element(BlockID, UnackedDeletes),

    NewManifest = riak_moss_lfs_utils:remove_write_block(Manifest, BlockID),

    State2 = State#state{free_deleters=NewFreeDeleters,
                         unacked_deletes=NewUnackedDeletes,
                         manifest=NewManifest},

    if
        NewManifest#lfs_manifest_v2.state == deleted ->
            State3 = finish(State2),
            {stop, normal, State3};
        true ->
            %% maybe start more deletes
            State3 = maybe_delete_block(State2),
            {next_state, deleting, State3}
    end;
deleting({block_deleted, {error, Error}, _DeleterPid}, State) ->
    lager:debug("Stopping delete FSM because of block deleting error:",
        [Error]),
    finish(State),
    {stop, Error, State}.

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

-spec finish(state()) -> state().
finish(State=#state{timer_ref=TRef,
                    manifest=Manifest,
                    mani_pid=ManiPid}) ->
    timer:cancel(TRef),
    riak_moss_manifest_fsm:update_manifest_with_confirmation(ManiPid, Manifest),
    State.

%% TODO:
%% for now we're only dealing
%% with manifests that are in the
%% pending_delete state
-spec blocks_to_delete_from_manifest(lfs_manifest()) ->
    {lfs_manifest(), ordsets:ordset(integer())}.
blocks_to_delete_from_manifest(Manifest=#lfs_manifest_v2{state=pending_delete,
                                                         delete_blocks_remaining=undefined}) ->
    Blocks = riak_moss_lfs_utils:block_sequences_for_manifest(Manifest),
    {Manifest#lfs_manifest_v2{delete_blocks_remaining=Blocks},
        Blocks};
blocks_to_delete_from_manifest(Manifest) ->
    {Manifest,
        Manifest#lfs_manifest_v2.delete_blocks_remaining}.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

-endif.
