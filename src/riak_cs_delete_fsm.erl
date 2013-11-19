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
-export([start_link/3,
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
                delete_blocks_remaining :: ordsets:ordset({binary(), integer()}),
                unacked_deletes=ordsets:new() :: ordsets:ordset(integer()),
                all_delete_workers=[] :: list(pid()),
                free_deleters = ordsets:new() :: ordsets:ordset(pid()),
                deleted_blocks = 0 :: non_neg_integer(),
                total_blocks = 0 :: non_neg_integer()}).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_cs_delete_fsm'.
start_link(RiakcPid, Manifest, Options) ->
    Args = [RiakcPid, Manifest, Options],
    gen_fsm:start_link(?MODULE, Args, []).

-spec block_deleted(pid(), {ok, {binary(), integer()}} | {error, binary()}) -> ok.
block_deleted(Pid, Response) ->
    gen_fsm:send_event(Pid, {block_deleted, Response, self()}).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([RiakcPid, {UUID, Manifest}, _Options]) ->
    {Bucket, Key} = Manifest?MANIFEST.bkey,
    State = #state{bucket=Bucket,
                   key=Key,
                   manifest=Manifest,
                   uuid=UUID,
                   riakc_pid=RiakcPid},
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

terminate(Reason, _StateName, #state{all_delete_workers=AllDeleteWorkers,
                                     manifest=?MANIFEST{state=ManifestState},
                                     bucket=Bucket,
                                     key=Key,
                                     uuid=UUID,
                                     riakc_pid=RiakcPid} = State) ->
    manifest_cleanup(ManifestState, Bucket, Key, UUID, RiakcPid),
    _ = [riak_cs_block_server:stop(P) || P <- AllDeleteWorkers],
    notify_gc_daemon(Reason, State),
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
    {next_state, atom(), state()}.
handle_receiving_manifest(State=#state{riakc_pid=RiakcPid,
                                       manifest=Manifest}) ->
    {NewManifest, BlocksToDelete} = blocks_to_delete_from_manifest(Manifest),
    BlockCount = ordsets:size(BlocksToDelete),
    NewState = State#state{manifest=NewManifest,
                           delete_blocks_remaining=BlocksToDelete,
                           total_blocks=BlockCount},

    %% Handle the case where there are 0 blocks to delete,
    %% i.e. content length of 0
    case ordsets:size(BlocksToDelete) > 0 of
        true ->
            AllDeleteWorkers =
                riak_cs_block_server:start_block_servers(
                  Manifest, RiakcPid,
                  riak_cs_lfs_utils:delete_concurrency()),
            FreeDeleters = ordsets:from_list(AllDeleteWorkers),

            NewState1 = NewState#state{all_delete_workers=AllDeleteWorkers,
                                       free_deleters=FreeDeleters},

            StateAfterDeleteStart = maybe_delete_blocks(NewState1),

            {next_state, deleting, StateAfterDeleteStart};
        false ->
            {stop, normal, NewState}
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

-spec notify_gc_daemon(term(), state()) -> term().
notify_gc_daemon(Reason, State) ->
    gen_fsm:sync_send_event(riak_cs_gc_d, notification_msg(Reason, State), infinity).

-spec notification_msg(term(), state()) -> {pid(),
                                            {ok, {non_neg_integer(), non_neg_integer()}} |
                                            {error, term()}}.
notification_msg(normal, #state{deleted_blocks = DeletedBlocks,
                                total_blocks = TotalBlocks}) ->
    {self(), {ok, {DeletedBlocks, TotalBlocks}}};
notification_msg(Reason, _State) ->
    {self(), {error, Reason}}.

-spec manifest_cleanup(atom(), binary(), binary(), binary(), pid()) -> ok.
manifest_cleanup(deleted, Bucket, Key, UUID, RiakcPid) ->
    {ok, ManiFsmPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RiakcPid),
    _ = try
            _ = riak_cs_manifest_fsm:delete_specific_manifest(ManiFsmPid, UUID)
        after
            _ = riak_cs_manifest_fsm:stop(ManiFsmPid)
        end,
    ok;
manifest_cleanup(_, _, _, _, _) ->
    ok.

-spec blocks_to_delete_from_manifest(lfs_manifest()) ->
    {lfs_manifest(), ordsets:ordset(integer())}.
blocks_to_delete_from_manifest(Manifest=?MANIFEST{state=State,
                                                  delete_blocks_remaining=undefined})
  when State =:= pending_delete;State =:= writing; State =:= scheduled_delete ->
    case riak_cs_lfs_utils:block_sequences_for_manifest(Manifest) of
        []=Blocks ->
            UpdManifest = Manifest?MANIFEST{delete_blocks_remaining=[],
                                            state=deleted};
        Blocks ->
            UpdManifest = Manifest?MANIFEST{delete_blocks_remaining=Blocks}
    end,
    {UpdManifest, Blocks};
blocks_to_delete_from_manifest(Manifest) ->
    {Manifest,
        Manifest?MANIFEST.delete_blocks_remaining}.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

-endif.
