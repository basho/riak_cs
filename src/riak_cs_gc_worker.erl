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
%%

-module(riak_cs_gc_worker).

-behaviour(gen_fsm).

%% API
-export([start_link/1,
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
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0,
         test_link/1,
         current_state/0,
         change_state/1,
         status_data/1]).

-endif.

-define(STATE, #gc_worker_state).
-define(GC_D, riak_cs_gc_d).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the garbage collection server
start_link(Keys) ->
    gen_fsm:start_link(?MODULE, [Keys], []).

%% @doc Stop the daemon
-spec stop(pid()) -> ok | {error, term()}.
stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init([Keys]) ->
    %% this does not check out a worker from the riak
    %% connection pool; instead it creates a fresh new worker
    {ok, Pid} = riak_cs_riakc_pool_worker:start_link([]),
    {ok, fetching_next_fileset, ?STATE{batch=Keys, riak_pid=Pid}}.

%% Asynchronous events

%% @doc Async transitions from `fetching_next_fileset' are all due to
%% messages the FSM sends itself, in order to have opportunities to
%% handle messages from the outside world (like `status').
fetching_next_fileset(continue, ?STATE{batch=[]}=State) ->
    %% finished with this batch
    gen_fsm:send_event(?GC_D, {batch_complete, State}),
    {stop, normal, ok, State};
fetching_next_fileset(continue, State=?STATE{batch=[FileSetKey | RestKeys],
                                             batch_skips=BatchSkips,
                                             manif_count=ManifCount,
                                             riak_pid=RiakPid
                                            }) ->
    %% Fetch the next set of manifests for deletion
    {NextState, NextStateData} =
        case fetch_next_fileset(FileSetKey, RiakPid) of
            {ok, FileSet, RiakObj} ->
                {initiating_file_delete,
                 State?STATE{current_files=twop_set:to_list(FileSet),
                             current_fileset=FileSet,
                             current_riak_object=RiakObj,
                             manif_count=ManifCount+twop_set:size(FileSet)}};
            {error, _} ->
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
                                        riak_pid=RiakPid}=State) ->
    finish_file_delete(twop_set:size(FileSet), FileSet, RiakObj, RiakPid),
    ok = continue(),
    {next_state, fetching_next_fileset, State?STATE{batch=RestKeys,
                                                    batch_count=1+BatchCount}};
initiating_file_delete(continue, ?STATE{current_files=[Manifest | _RestManifests],
                                        riak_pid=RiakPid}=State) ->
    %% Use an instance of `riak_cs_delete_fsm' to handle the
    %% deletion of the file blocks.
    %% Don't worry about delete_fsm failures. Manifests are
    %% rescheduled after a certain time.
    Args = [RiakPid, Manifest, []],
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

fetching_next_fileset(Msg, _From, State) ->
    Common = [{manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), fetching_next_fileset, State}.

initiating_file_delete(Msg, _From, State) ->
    Common = [{manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), initiating_file_delete, State}.

waiting_file_delete({Pid, DelFsmReply}, _From, State=?STATE{delete_fsm_pid=Pid}) ->
    ok_reply(initiating_file_delete, handle_delete_fsm_reply(DelFsmReply, State));
waiting_file_delete(Msg, _From, State) ->
    Common = [{manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), waiting_file_delete, State}.

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
terminate(_Reason, _StateName, _State=?STATE{riak_pid=RiakPid}) ->
    riak_cs_riakc_pool_worker:stop(RiakPid),
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
-spec fetch_next_fileset(binary(), pid()) ->
                                {ok, twop_set:twop_set(), riakc_obj:riakc_obj()} |
                                {error, term()}.
fetch_next_fileset(ManifestSetKey, RiakPid) ->
    %% Get the set of manifests represented by the key
    case riak_cs_utils:get_object(?GC_BUCKET, ManifestSetKey, RiakPid) of
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
                         pid()) -> ok.
finish_file_delete(0, _, RiakObj, RiakPid) ->
    %% Delete the key from the GC bucket
    _ = riakc_pb_socket:delete_obj(RiakPid, RiakObj),
    ok;
finish_file_delete(_, FileSet, _RiakObj, _RiakPid) ->
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

handle_common_sync_reply(Msg, Common, _State) when is_atom(Msg) ->
    proplists:get_value(Msg, Common, unknown_command);
handle_common_sync_reply({MsgBase, _}, Common, State) when is_atom(MsgBase) ->
    handle_common_sync_reply(MsgBase, Common, State).

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
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start the garbage collection server
test_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [testing], []).

%% @doc Start the garbage collection server
test_link(Interval) ->
    application:set_env(riak_cs, gc_interval, Interval),
    test_link().

%% @doc Get the current state of the fsm for testing inspection
-spec current_state() -> {atom(), ?STATE{}} | {error, term()}.
current_state() ->
    gen_fsm:sync_send_all_state_event(?SERVER, current_state).

%% @doc Manipulate the current state of the fsm for testing
change_state(State) ->
    gen_fsm:sync_send_all_state_event(?SERVER, {change_state, State}).

-endif.
