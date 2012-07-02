%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_cs_delete_fsm).

-behaviour(gen_fsm).

-include("riak_moss.hrl").

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
                delete_blocks_remaining :: ordsets:ordset(integer()),
                unacked_deletes=ordsets:new() :: ordsets:ordset(integer()),
                all_delete_workers :: list(pid()),
                free_deleters = ordsets:new() :: ordsets:ordset(pid())}).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_cs_delete_fsm'.
start_link(RiakcPid, Manifest, Options) ->
    Args = [RiakcPid, Manifest, Options],
    gen_fsm:start_link(?MODULE, Args, []).

-spec block_deleted(pid(), {ok, integer()} | {error, binary()}) -> ok.
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
    State=#state{manifest=Manifest,
                 free_deleters=FreeDeleters,
                 unacked_deletes=UnackedDeletes}) ->

    NewFreeDeleters = ordsets:add_element(DeleterPid, FreeDeleters),
    NewUnackedDeletes = ordsets:del_element(BlockID, UnackedDeletes),

    NewManifest = riak_moss_lfs_utils:remove_delete_block(Manifest, BlockID),

    State2 = State#state{free_deleters=NewFreeDeleters,
                         unacked_deletes=NewUnackedDeletes,
                         manifest=NewManifest},

    if
        NewManifest?MANIFEST.state == deleted ->
            {stop, normal, State2};
        true ->
            %% maybe start more deletes
            State3 = maybe_delete_blocks(State2),
            {next_state, deleting, State3}
    end;
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
                                     riakc_pid=RiakcPid}) ->
    manifest_cleanup(ManifestState, Bucket, Key, UUID, RiakcPid),
    _ = [riak_moss_block_server:stop(P) || P <- AllDeleteWorkers],
    notify_gc_daemon(Reason),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec handle_receiving_manifest(state()) ->
    {next_state, atom(), state()}.
handle_receiving_manifest(State=#state{riakc_pid=RiakcPid,
                                       manifest=Manifest}) ->
    {NewManifest, BlocksToDelete} = blocks_to_delete_from_manifest(Manifest),

    NewState = State#state{manifest=NewManifest,
                           delete_blocks_remaining=BlocksToDelete},

    %% Handle the case where there are 0 blocks to delete,
    %% i.e. content length of 0
    case ordsets:size(BlocksToDelete) > 0 of
        true ->
            AllDeleteWorkers =
                riak_moss_block_server:start_block_servers(RiakcPid,
                    riak_moss_lfs_utils:delete_concurrency()),
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
                                 uuid=UUID,
                                 free_deleters=FreeDeleters=[DeleterPid | _Rest],
                                 unacked_deletes=UnackedDeletes,
                                 delete_blocks_remaining=DeleteBlocksRemaining=
                                     [BlockID | _RestBlocks]}) ->
    NewUnackedDeletes = ordsets:add_element(BlockID, UnackedDeletes),
    NewDeleteBlocksRemaining = ordsets:del_element(BlockID, DeleteBlocksRemaining),
    _ = lager:debug("Deleting block: ~p ~p ~p ~p", [Bucket, Key, UUID, BlockID]),
    riak_moss_block_server:delete_block(DeleterPid, Bucket, Key, UUID, BlockID),
    NewFreeDeleters = ordsets:del_element(DeleterPid, FreeDeleters),
    maybe_delete_blocks(State#state{unacked_deletes=NewUnackedDeletes,
                                    free_deleters=NewFreeDeleters,
                                    delete_blocks_remaining=NewDeleteBlocksRemaining}).

-spec notify_gc_daemon(term()) -> term().
notify_gc_daemon(Reason) ->
    gen_fsm:sync_send_event(riak_cs_gc_d, notification_msg(Reason), infinity).

-spec notification_msg(term()) -> {pid(), ok | {error, term()}}.
notification_msg(normal) ->
    {self(), ok};
notification_msg(Reason) ->
    {self(), {error, Reason}}.

-spec manifest_cleanup(atom(), binary(), binary(), binary(), pid()) -> ok.
manifest_cleanup(deleted, Bucket, Key, UUID, RiakcPid) ->
    {ok, ManiFsmPid} = riak_moss_manifest_fsm:start_link(Bucket, Key, RiakcPid),
    _ = riak_moss_manifest_fsm:delete_specific_manifest(ManiFsmPid, UUID),
    _ = riak_moss_manifest_fsm:stop(ManiFsmPid),
    ok;
manifest_cleanup(_, _, _, _, _) ->
    ok.

-spec blocks_to_delete_from_manifest(lfs_manifest()) ->
    {lfs_manifest(), ordsets:ordset(integer())}.
blocks_to_delete_from_manifest(Manifest=?MANIFEST{state=State,
                                                  delete_blocks_remaining=undefined})
  when State =:= pending_delete;State =:= writing; State =:= scheduled_delete ->
    case riak_moss_lfs_utils:block_sequences_for_manifest(Manifest) of
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
