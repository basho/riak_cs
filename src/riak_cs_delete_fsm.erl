%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_cs_delete_fsm).

-behaviour(gen_fsm).

-include("riak_cs.hrl").

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
                riak_client :: riak_client(),
                finished_callback :: fun(),
                %% For GC, Key in GC bucket which this manifest belongs to.
                %% For active deletion, not used.
                %% Set only once at init and unchanged. Used only for logs.
                gc_key :: binary(),
                delete_blocks_remaining :: ordsets:ordset({binary(), integer()}),
                unacked_deletes=ordsets:new() :: ordsets:ordset(integer()),
                all_delete_workers=[] :: list(pid()),
                free_deleters = ordsets:new() :: ordsets:ordset(pid()),
                deleted_blocks = 0 :: non_neg_integer(),
                total_blocks = 0 :: non_neg_integer(),
                cleanup_manifests = true :: boolean()}).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_cs_delete_fsm'.
start_link(BagId, Manifest, GCWorkerPid, GCKey, Options) ->
    Args = [BagId, Manifest, GCWorkerPid, GCKey, Options],
    gen_fsm:start_link(?MODULE, Args, []).

-spec block_deleted(pid(), {ok, {binary(), integer()}} | {error, binary()}) -> ok.
block_deleted(Pid, Response) ->
    gen_fsm:send_event(Pid, {block_deleted, Response, self()}).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([BagId, {UUID, Manifest}, FinishedCallback, GCKey, Options]) ->
    {Bucket, Key} = Manifest?MANIFEST.bkey,
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    ok = riak_cs_riak_client:set_manifest_bag(RcPid, BagId),
    ok = riak_cs_riak_client:set_manifest(RcPid, Manifest),
    CleanupManifests = proplists:get_value(cleanup_manifests,
                                           Options, true),
    State = #state{bucket=Bucket,
                   key=Key,
                   manifest=Manifest,
                   uuid=UUID,
                   riak_client=RcPid,
                   finished_callback=FinishedCallback,
                   gc_key=GCKey,
                   cleanup_manifests=CleanupManifests},
    {ok, prepare, State, 0}.

%% @TODO Make sure we avoid any race conditions here
%% like we had in the other fsms.
prepare(timeout, State) ->
    handle_receiving_manifest(State).

deleting({block_deleted, {ok, BlockID}, DeleterPid},
         State=#state{deleted_blocks=DeletedBlocks}) ->
    UpdState = deleting_state_update(BlockID, DeleterPid, DeletedBlocks+1, State),
    ManifestState = UpdState#state.manifest?MANIFEST.state,
    deleting_state_result(ManifestState, UpdState);
deleting({block_deleted, {error, {unsatisfied_constraint, _, BlockID}}, DeleterPid},
         State=#state{deleted_blocks=DeletedBlocks}) ->
    UpdState = deleting_state_update(BlockID, DeleterPid, DeletedBlocks, State),
    ManifestState = UpdState#state.manifest?MANIFEST.state,
    deleting_state_result(ManifestState, UpdState);
deleting({block_deleted, {error, Error}, _DeleterPid}, State) ->
    {stop, Error, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, _StateName,
          #state{all_delete_workers=AllDeleteWorkers,
                 manifest=?MANIFEST{state=ManifestState},
                 bucket=Bucket,
                 key=Key,
                 uuid=UUID,
                 riak_client=RcPid,
                 cleanup_manifests=CleanupManifests} = State) ->
    if CleanupManifests ->
            manifest_cleanup(ManifestState, Bucket, Key, UUID, RcPid);
       true ->
            noop
    end,
    _ = [riak_cs_block_server:stop(P) || P <- AllDeleteWorkers],
    notify_requester(Reason, State),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Update the state record following notification of the
%% completion of a block deletion.
-spec deleting_state_update(pos_integer(), pid(), non_neg_integer(), #state{}) ->
                                   #state{}.
deleting_state_update(BlockID,
                      DeleterPid,
                      DeletedBlocks,
                      State=#state{manifest=Manifest,
                                   free_deleters=FreeDeleters,
                                   unacked_deletes=UnackedDeletes}) ->
    NewManifest = riak_cs_lfs_utils:remove_delete_block(Manifest, BlockID),
    State#state{free_deleters=ordsets:add_element(DeleterPid,
                                                  FreeDeleters),
                unacked_deletes=ordsets:del_element(BlockID,
                                                    UnackedDeletes),
                manifest=NewManifest,
                deleted_blocks=DeletedBlocks}.

%% @doc Determine the appropriate `deleting' state
%% fsm callback result based on the given manifest state.
-spec deleting_state_result(atom(), #state{}) -> {atom(), atom(), #state{}}.
deleting_state_result(deleted, State) ->
    {stop, normal, State};
deleting_state_result(_, State) ->
    UpdState = maybe_delete_blocks(State),
    {next_state, deleting, UpdState}.

-spec handle_receiving_manifest(state()) ->
    {next_state, atom(), state()} | {stop, normal, state()}.
handle_receiving_manifest(State=#state{manifest=Manifest,
                                       gc_key=GCKey}) ->
    case blocks_to_delete_from_manifest(Manifest) of
        {ok, {NewManifest, BlocksToDelete}} ->
            BlockCount = ordsets:size(BlocksToDelete),
            NewState = State#state{manifest=NewManifest,
                                   delete_blocks_remaining=BlocksToDelete,
                                   total_blocks=BlockCount},
            start_block_servers(NewState);
        {error, invalid_state} ->
            {Bucket, Key} = Manifest?MANIFEST.bkey,
            _ = lager:warning("Invalid state manifest in GC bucket at ~p, "
                              "bucket=~p key=~p: ~p",
                              [GCKey, Bucket, Key, Manifest]),
            %% If total blocks and deleted blocks are the same,
            %% gc worker attempt to delete the manifest in fileset.
            %% Then manifests and blocks becomes orphan.
            %% To avoid it, set total_blocks > 0 here.
            %% For now delete FSM stops with pseudo-normal termination to
            %% let other valid manifests be collected, as the root cause
            %% of #827 is still unidentified.
            {stop, normal, State#state{total_blocks=1}}
    end.

-spec start_block_servers(state()) -> {next_state, atom(), state()} |
                                      {stop, normal, state()}.
start_block_servers(#state{riak_client=RcPid,
                           manifest=Manifest,
                           delete_blocks_remaining=BlocksToDelete} = State) ->
    %% Handle the case where there are 0 blocks to delete,
    %% i.e. content length of 0,
    %% and can not check-out any workers.
    case ordsets:size(BlocksToDelete) > 0 of
        true ->
            AllDeleteWorkers =
                riak_cs_block_server:start_block_servers(
                  Manifest, RcPid,
                  riak_cs_lfs_utils:delete_concurrency()),
            case length(AllDeleteWorkers) of
                0 ->
                    {stop, normal, State};
                _ ->
                    FreeDeleters = ordsets:from_list(AllDeleteWorkers),
                    NewState = State#state{all_delete_workers=AllDeleteWorkers,
                                           free_deleters=FreeDeleters},
                    StateAfterDeleteStart = maybe_delete_blocks(NewState),
                    {next_state, deleting, StateAfterDeleteStart}
            end;
        false ->
            {stop, normal, State}
    end.

maybe_delete_blocks(State=#state{free_deleters=[]}) ->
    State;
maybe_delete_blocks(State=#state{delete_blocks_remaining=[]}) ->
    State;
maybe_delete_blocks(State=#state{bucket=Bucket,
                                 key=Key,
                                 free_deleters=FreeDeleters=[DeleterPid | _Rest],
                                 unacked_deletes=UnackedDeletes,
                                 delete_blocks_remaining=DeleteBlocksRemaining=
                                     [BlockID | _RestBlocks]}) ->
    NewUnackedDeletes = ordsets:add_element(BlockID, UnackedDeletes),
    NewDeleteBlocksRemaining = ordsets:del_element(BlockID, DeleteBlocksRemaining),
    {UUID, Seq} = BlockID,
    _ = lager:debug("Deleting block: ~p ~p ~p ~p", [Bucket, Key, UUID, Seq]),
    riak_cs_block_server:delete_block(DeleterPid, Bucket, Key, UUID, Seq),
    NewFreeDeleters = ordsets:del_element(DeleterPid, FreeDeleters),
    maybe_delete_blocks(State#state{unacked_deletes=NewUnackedDeletes,
                                    free_deleters=NewFreeDeleters,
                                    delete_blocks_remaining=NewDeleteBlocksRemaining}).

-spec notify_requester(term(), state()) -> term().
notify_requester(Reason, State = #state{finished_callback=FinishedCallback}) ->
    FinishedCallback(notification_msg(Reason, State)).

-spec notification_msg(term(), state()) -> {pid(),
                                            {ok, {non_neg_integer(), non_neg_integer()}} |
                                            {error, term()}}.
notification_msg(normal, #state{
                            bucket=Bucket,
                            key=Key,
                            uuid=UUID,
                            deleted_blocks = DeletedBlocks,
                            total_blocks = TotalBlocks}) ->
    Reply = {ok, {Bucket, Key, UUID, DeletedBlocks, TotalBlocks}},
    {self(), Reply};
notification_msg(Reason, _State) ->
    {self(), {error, Reason}}.

-spec manifest_cleanup(atom(), binary(), binary(), binary(), riak_client()) -> ok.
manifest_cleanup(deleted, Bucket, Key, UUID, RcPid) ->
    {ok, ManiFsmPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RcPid),
    _ = try
            _ = riak_cs_manifest_fsm:delete_specific_manifest(ManiFsmPid, UUID)
        after
            _ = riak_cs_manifest_fsm:stop(ManiFsmPid)
        end,
    ok;
manifest_cleanup(_, _, _, _, _) ->
    ok.

-spec blocks_to_delete_from_manifest(lfs_manifest()) ->
                                            {ok, {lfs_manifest(), ordsets:ordset(integer())}} |
                                            {error, term()}.
blocks_to_delete_from_manifest(Manifest=?MANIFEST{state=State,
                                                  delete_blocks_remaining=undefined})
  when State =:= pending_delete;State =:= writing; State =:= scheduled_delete ->
    {UpdState, Blocks} =
        case riak_cs_lfs_utils:block_sequences_for_manifest(Manifest) of
            [] ->
                {deleted, ordsets:new()};
            BlockSequence ->
                {State, BlockSequence}

        end,
    UpdManifest = Manifest?MANIFEST{delete_blocks_remaining=Blocks,
                                    state=UpdState},
    {ok, {UpdManifest, Blocks}};
blocks_to_delete_from_manifest(?MANIFEST{delete_blocks_remaining=undefined}) ->
    {error, invalid_state};
blocks_to_delete_from_manifest(Manifest) ->
    {ok, {Manifest, Manifest?MANIFEST.delete_blocks_remaining}}.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

-endif.
