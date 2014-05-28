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

%% @doc A worker module that handles garbage collection of deleted
%% file manifests and blocks at the direction of the garbace
%% collection daemon.

-module(riak_cs_gc_worker).

-behaviour(gen_fsm).

%% API
-export([start_link/2,
         stop/1]).

%% gen_fsm callbacks
-export([init/1,
         fetching_next_fileset/2,
         fetching_next_fileset/3,
         initiating_file_delete/2,
         initiating_file_delete/3,
         waiting_file_delete/2,
         waiting_file_delete/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_cs_gc_d.hrl").

-export([current_state/1]).

-define(STATE, #gc_worker_state).
-define(GC_D, riak_cs_gc_d).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the garbage collection server
start_link(BagId, Keys) ->
    gen_fsm:start_link(?MODULE, [BagId, Keys], []).

%% @doc Stop the daemon
-spec stop(pid()) -> ok | {error, term()}.
stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init([BagId, Keys]) ->
    %% this does not check out a worker from the riak
    %% connection pool; instead it creates a fresh new worker
    %% TODO: Newly start PBC clients of underlying bags
    {ok, RcPid} = riak_cs_riak_client:start_link([]),
    ok = riak_cs_riak_client:set_manifest_bag(RcPid, BagId),
    ok = continue(),
    {ok, fetching_next_fileset, ?STATE{batch=Keys, riak_client=RcPid}}.

%% Asynchronous events

%% @doc Async transitions from `fetching_next_fileset' are all due to
%% messages the FSM sends itself, in order to have opportunities to
%% handle messages from the outside world (like `status').
fetching_next_fileset(continue, ?STATE{batch=[]}=State) ->
    %% finished with this batch
    gen_fsm:send_event(?GC_D, {batch_complete, self(), State}),
    {stop, normal, State};
fetching_next_fileset(continue, State=?STATE{batch=[FileSetKey | RestKeys],
                                             batch_skips=BatchSkips,
                                             manif_count=ManifCount,
                                             riak_client=RcPid}) ->
    %% Fetch the next set of manifests for deletion
    {NextState, NextStateData} =
        case fetch_next_fileset(FileSetKey, RcPid) of
            {ok, FileSet, RiakObj} ->
                {initiating_file_delete,
                 State?STATE{current_files=twop_set:to_list(FileSet),
                             current_fileset=FileSet,
                             current_riak_object=RiakObj,
                             manif_count=ManifCount+twop_set:size(FileSet)}};
            {error, _Reason} ->
                {fetching_next_fileset,
                 State?STATE{batch=RestKeys,
                             batch_skips=BatchSkips+1}}
        end,
    ok = continue(),
    {next_state, NextState, NextStateData};
fetching_next_fileset(_, State) ->
    {next_state, fetching_next_fileset, State}.

%% @doc This state initiates the deletion of a file from
%% a set of manifests stored for a particular key in the
%% garbage collection bucket.
initiating_file_delete(continue, ?STATE{batch=[_ManiSetKey | RestKeys],
                                        batch_count=BatchCount,
                                        current_files=[],
                                        current_fileset=FileSet,
                                        current_riak_object=RiakObj,
                                        riak_client=RcPid}=State) ->
    finish_file_delete(twop_set:size(FileSet), FileSet, RiakObj, RcPid),
    ok = continue(),
    {next_state, fetching_next_fileset, State?STATE{batch=RestKeys,
                                                    batch_count=1+BatchCount}};
initiating_file_delete(continue, ?STATE{current_files=[Manifest | _RestManifests],
                                        riak_client=RcPid}=State) ->
    %% Use an instance of `riak_cs_delete_fsm' to handle the
    %% deletion of the file blocks.
    %% Don't worry about delete_fsm failures. Manifests are
    %% rescheduled after a certain time.
    Args = [RcPid, Manifest, self(), []],
    %% The delete FSM is hard-coded to send a sync event to our registered
    %% name upon terminate(), so we do not have to pass our pid to it
    %% in order to get a reply.
    {ok, Pid} = riak_cs_delete_fsm_sup:start_delete_fsm(node(), Args),

    %% Link to the delete fsm, so that if it dies,
    %% we go down too. In the future we might want to do
    %% something more complicated like retry
    %% a particular key N times before moving on, but for
    %% now this is the easiest thing to do. If we need to manually
    %% skip an object to GC, we can change the epoch start
    %% time in app.config
    link(Pid),
    {next_state, waiting_file_delete, State?STATE{delete_fsm_pid = Pid}};
initiating_file_delete(_, State) ->
    {next_state, initiating_file_delete, State}.

waiting_file_delete(_, State) ->
    {next_state, waiting_file_delete, State}.

%% Synchronous events

fetching_next_fileset(_Msg, _From, State) ->
    {next_state, fetching_next_fileset, State}.

initiating_file_delete(_Msg, _From, State) ->
    {next_state, initiating_file_delete, State}.

waiting_file_delete({Pid, DelFsmReply}, _From, State=?STATE{delete_fsm_pid=Pid}) ->
    ok_reply(initiating_file_delete, handle_delete_fsm_reply(DelFsmReply, State));
waiting_file_delete(_Msg, _From, State) ->
    {next_state, initiating_file_delete, State}.

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), ?STATE{}) ->
                               {reply, term(), atom(), ?STATE{}}.
handle_sync_event(current_state, _From, StateName, State) ->
    {reply, {StateName, State}, StateName, State};
handle_sync_event({change_state, NewStateName}, _From, _StateName, State) ->
    ok_reply(NewStateName, State);
handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    ok_reply(StateName, State).

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc TODO: log warnings if this fsm is asked to terminate in the
%% middle of running a gc batch
terminate(_Reason, _StateName, _State=?STATE{riak_client=RcPid}) ->
    riak_cs_riak_client:stop(RcPid),
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Send an asynchronous `continue' event. This is used to advance
%% the FSM to the next state and perform some blocking action. The blocking
%% actions are done in the beginning of the next state to give the FSM a
%% chance to respond to events from the outside world.
-spec continue() -> ok.
continue() ->
    gen_fsm:send_event(self(), continue).

%% @doc Delete the blocks for the next set of manifests in the batch
-spec fetch_next_fileset(binary(), riak_client()) ->
                                {ok, twop_set:twop_set(), riakc_obj:riakc_obj()} |
                                {error, term()}.
fetch_next_fileset(ManifestSetKey, RcPid) ->
    %% Get the set of manifests represented by the key
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    case riak_cs_pbc:get_object(ManifestPbc, ?GC_BUCKET, ManifestSetKey) of
        {ok, RiakObj} ->
            ManifestSet = riak_cs_gc:decode_and_merge_siblings(
                            RiakObj, twop_set:new()),
            {ok, ManifestSet, RiakObj};
        {error, notfound}=Error ->
            Error;
        {error, Reason}=Error ->
            _ = lager:info("Error occurred trying to read the fileset"
                           "for ~p for gc. Reason: ~p",
                           [ManifestSetKey, Reason]),
            Error
    end.

%% @doc Finish a file set delete process by either deleting the file
%% set from the GC bucket or updating the value to remove file set
%% members that were succesfully deleted.
-spec finish_file_delete(non_neg_integer(),
                         twop_set:twop_set(),
                         riakc_obj:riakc_obj(),
                         riak_client()) -> ok.
finish_file_delete(0, _, RiakObj, RcPid) ->
    %% Delete the key from the GC bucket
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    _ = riakc_pb_socket:delete_obj(ManifestPbc, RiakObj),
    ok;
finish_file_delete(_, FileSet, _RiakObj, _RcPid) ->
    _ = lager:debug("Remaining file keys: ~p", [twop_set:to_list(FileSet)]),
    %% NOTE: we used to do a PUT here, but now with multidc replication
    %% we run garbage collection seprarately on each cluster, so we don't
    %% want to send this update to another data center. When we delete this
    %% key in its entirety later, that delete will _not_ be replicated,
    %% as we explicitly do not replicate tombstones in Riak CS.
    ok.

-spec ok_reply(atom(), ?STATE{}) -> {reply, ok, atom(), ?STATE{}}.
ok_reply(NextState, NextStateData) ->
    {reply, ok, NextState, NextStateData}.

%% Refactor TODO:
%%   1. delete_fsm_pid=undefined is desirable in both ok & error cases?
%%   2. It's correct to *not* change pause_state?
handle_delete_fsm_reply({ok, {TotalBlocks, TotalBlocks}},
                        ?STATE{current_files=[CurrentManifest | RestManifests],
                               current_fileset=FileSet,
                               block_count=BlockCount} = State) ->
    ok = continue(),
    UpdFileSet = twop_set:del_element(CurrentManifest, FileSet),
    State?STATE{delete_fsm_pid=undefined,
                current_fileset=UpdFileSet,
                current_files=RestManifests,
                block_count=BlockCount+TotalBlocks};
handle_delete_fsm_reply({ok, {NumDeleted, _TotalBlocks}},
                        ?STATE{current_files=[_CurrentManifest | RestManifests],
                               block_count=BlockCount} = State) ->
    ok = continue(),
    State?STATE{delete_fsm_pid=undefined,
                current_files=RestManifests,
                block_count=BlockCount+NumDeleted};
handle_delete_fsm_reply({error, _}, ?STATE{current_files=[_ | RestManifests]} = State) ->
    ok = continue(),
    State?STATE{delete_fsm_pid=undefined,
                current_files=RestManifests}.

%% ===================================================================
%% For debug
%% ===================================================================

%% @doc Get the current state of the fsm for debugging inspection
-spec current_state(pid()) -> {atom(), ?STATE{}} | {error, term()}.
current_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_state).
