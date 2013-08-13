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

%% @doc The daemon that handles garbage collection of deleted file
%% manifests and blocks.
%%
%% @TODO Differences in the fsm state and the state record
%% get confusing. Maybe s/State/StateData.

-module(riak_cs_gc_d).

-behaviour(gen_fsm).

%% API
-export([start_link/0,
         status/0,
         manual_batch/1,
         cancel_batch/0,
         pause/0,
         resume/0,
         set_interval/1,
         stop/0]).

%% gen_fsm callbacks
-export([init/1,
         idle/2,
         idle/3,
         fetching_next_fileset/2,
         fetching_next_fileset/3,
         initiating_file_delete/2,
         initiating_file_delete/3,
         waiting_file_delete/2,
         waiting_file_delete/3,
         paused/2,
         paused/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_cs.hrl").
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

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the garbage collection server
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Status is returned as a 2-tuple of `{State, Details}'.  State
%% should be `idle', `deleting', or `paused'.  When `idle' the
%% details (a proplist) will include the schedule, as well as the
%% times of the last calculation and the next planned calculation.
%% When `calculating' or `paused' details also the scheduled time of
%% the active calculation, the number of seconds the process has been
%% calculating so far, and counts of how many users have been
%% processed and how many are left.
status() ->
    gen_fsm:sync_send_event(?SERVER, status, infinity).

%% @doc Force a garbage collection sweep manually.
%%
%% Allowed options are:
%% <dl>
%%   <dt>`testing'</dt>
%%   <dd>Indicate the daemon is started as part of a test suite.</dd>
%% </dl>
manual_batch(Options) ->
    gen_fsm:sync_send_event(?SERVER, {manual_batch, Options}, infinity).

%% @doc Cancel the calculation currently in progress.  Returns `ok' if
%% a batch was canceled, or `{error, no_batch}' if there was no batch
%% in progress.
cancel_batch() ->
    gen_fsm:sync_send_event(?SERVER, cancel_batch, infinity).

%% @doc Pause the garbage collection daemon.  Returns `ok' if
%% the daemon was paused, or `{error, already_paused}' if the daemon
%% was already paused.
pause() ->
    gen_fsm:sync_send_event(?SERVER, pause, infinity).

%% @doc Resume the garbage collection daemon.  Returns `ok' if the
%% daemon was resumed, or `{error, not_paused}' if the daemon was
%% not paused.
resume() ->
    gen_fsm:sync_send_event(?SERVER, resume, infinity).

%% @doc Adjust the interval at which the daemon attempts to perform
%% a garbage collection sweep. Setting the interval to a value of
%% `infinity' effectively disable garbage collection. The daemon still
%% runs, but does not carry out any file deletion.
-spec set_interval(infinity | non_neg_integer()) -> ok | {error, term()}.
set_interval(Interval) when is_integer(Interval) orelse Interval == infinity ->
    gen_fsm:sync_send_event(?SERVER, {set_interval, Interval}, infinity).

%% @doc Stop the daemon
-spec stop() -> ok | {error, term()}.
stop() ->
    gen_fsm:sync_send_all_state_event(?SERVER, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init(_Args) ->
    Interval = riak_cs_gc:gc_interval(),
    SchedState = schedule_next(#state{interval=Interval}),
    {ok, idle, SchedState}.

%% Asynchronous events

%% @doc Transitions out of idle are all synchronous events
idle(_, State=#state{interval_remaining=undefined}) ->
    {next_state, idle, State};
idle(_, State=#state{interval_remaining=IntervalRemaining}) ->
    TimerRef = erlang:send_after(IntervalRemaining, self(), start_batch),
    {next_state, idle, State#state{timer_ref=TimerRef}}.

%% @doc Async transitions from `fetching_next_fileset' are all due to
%% messages the FSM sends itself, in order to have opportunities to
%% handle messages from the outside world (like `status').
fetching_next_fileset(continue, #state{batch=[]}=State) ->
    %% finished with this batch
    _ = lager:info("Finished garbage collection: "
                   "~b seconds, ~p batch_count, ~p batch_skips, "
                   "~p manif_count, ~p block_count\n",
                   [elapsed(State#state.batch_start), State#state.batch_count,
                    State#state.batch_skips, State#state.manif_count,
                    State#state.block_count]),
    riak_cs_riakc_pool_worker:stop(State#state.riak),
    NewState = schedule_next(State#state{riak=undefined}),
    {next_state, idle, NewState};
fetching_next_fileset(continue, State=#state{batch=[FileSetKey | RestKeys],
                                             batch_skips=BatchSkips,
                                             manif_count=ManifCount,
                                             riak=RiakPid
                                            }) ->
    %% Fetch the next set of manifests for deletion
    case fetch_next_fileset(FileSetKey, RiakPid) of
        {ok, FileSet, RiakObj} ->
            NewStateData = State#state{current_files=twop_set:to_list(FileSet),
                                       current_fileset=FileSet,
                                       current_riak_object=RiakObj,
                                       manif_count=ManifCount+twop_set:size(FileSet)},
            NextState = initiating_file_delete;
        {error, _} ->
            NewStateData = State#state{batch=RestKeys,
                                       batch_skips=BatchSkips+1},
            NextState = fetching_next_fileset
    end,
    ok = continue(),
    {next_state, NextState, NewStateData};
fetching_next_fileset(_, State) ->
    {next_state, fetching_next_fileset, State}.

%% @doc This state initiates the deletion of a file from
%% a set of manifests stored for a particular key in the
%% garbage collection bucket.
initiating_file_delete(continue, #state{batch=[_ManiSetKey | RestKeys],
                                        batch_count=BatchCount,
                                        current_files=[],
                                        current_fileset=FileSet,
                                        current_riak_object=RiakObj,
                                        riak=RiakPid}=State) ->
    finish_file_delete(twop_set:size(FileSet), FileSet, RiakObj, RiakPid),
    ok = continue(),
    {next_state, fetching_next_fileset, State#state{batch=RestKeys,
                                                    batch_count=1+BatchCount}};
initiating_file_delete(continue, #state{current_files=[Manifest | _RestManifests],
                                        riak=RiakPid}=State) ->
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
    {next_state, waiting_file_delete, State#state{delete_fsm_pid = Pid}};
initiating_file_delete(_, State) ->
    {next_state, initiating_file_delete, State}.

waiting_file_delete(_, State) ->
    {next_state, waiting_file_delete, State}.

paused(_, State) ->
    {next_state, paused, State}.

%% Synchronous events

idle({manual_batch, Options}, _From, State) ->
    ok_reply(fetching_next_fileset, start_manual_batch(
                                       lists:member(testing, Options),
                                       State));
idle(pause, _From, State) ->
    ok_reply(paused, pause_gc(idle, State));
idle({set_interval, Interval}, _From, State)
  when is_integer(Interval) orelse Interval == infinity ->
    ok_reply(idle, State#state{interval=Interval});
idle(Msg, _From, State) ->
    Common = [{status, {ok, {idle, [{interval, State#state.interval},
                                    {last, State#state.last},
                                    {next, State#state.next}]}}},
              {cancel_batch, {error, no_batch}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), idle, State}.

fetching_next_fileset(pause, _From, State) ->
    ok_reply(paused, pause_gc(fetching_next_fileset, State));
fetching_next_fileset(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
fetching_next_fileset({set_interval, Interval}, _From, State) ->
    ok_reply(fetching_next_fileset, State#state{interval=Interval});
fetching_next_fileset(Msg, _From, State) ->
    Common = [{status, {ok, {fetching_next_fileset, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), fetching_next_fileset, State}.

initiating_file_delete(pause, _From, State) ->
    ok_reply(paused, pause_gc(initiating_file_delete, State));
initiating_file_delete(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
initiating_file_delete({set_interval, Interval}, _From, State) ->
    ok_reply(initiating_file_delete, State#state{interval=Interval});
initiating_file_delete(Msg, _From, State) ->
    Common = [{status, {ok, {initiating_file_delete, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), initiating_file_delete, State}.

waiting_file_delete({Pid, DelFsmReply}, _From, State=#state{delete_fsm_pid=Pid}) ->
    ok_reply(initiating_file_delete, handle_delete_fsm_reply(DelFsmReply, State));
waiting_file_delete(pause, _From, State) ->
    ok_reply(paused, pause_gc(waiting_file_delete, State));
waiting_file_delete(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
waiting_file_delete({set_interval, Interval}, _From, State) ->
    ok_reply(waiting_file_delete, State#state{interval=Interval});
waiting_file_delete(Msg, _From, State) ->
    Common = [{status, {ok, {waiting_file_delete, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), waiting_file_delete, State}.

paused({Pid, DelFsmReply}, _From, State=#state{delete_fsm_pid=Pid}) ->
    ok_reply(paused, handle_delete_fsm_reply(DelFsmReply, State));
paused(resume, _From, State=#state{pause_state=PauseState}) ->
    ok_reply(PauseState, resume_gc(State));
paused(cancel_batch, _From, State) ->
    ok_reply(paused, cancel_batch(State#state{pause_state=idle}));
paused({set_interval, Interval}, _From, State) ->
    ok_reply(paused, State#state{interval=Interval});
paused(Msg, _From, State) ->
    Common = [{status, {ok, {paused, status_data(State)}}},
              {pause, {error, already_paused}},
              {manual_batch, ok}],
    {reply, handle_common_sync_reply(Msg, Common, State), paused, State}.

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), #state{}) ->
                               {reply, term(), atom(), #state{}}.
handle_sync_event(current_state, _From, StateName, State) ->
    {reply, {StateName, State}, StateName, State};
handle_sync_event({change_state, NewStateName}, _From, _StateName, State) ->
    ok_reply(NewStateName, State);
handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    ok_reply(StateName, State).

handle_info(start_batch, idle, State) ->
    NewState = start_batch(State),
    {next_state, fetching_next_fileset, NewState};
handle_info(start_batch, InBatch, State) ->
    _ = lager:info("Unable to start garbage collection batch"
                    " because a previous batch is still working."),
    {next_state, InBatch, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc TODO: log warnings if this fsm is asked to terminate in the
%% middle of running a gc batch
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Cancel the current batch of files set for garbage collection.
-spec cancel_batch(#state{}) -> #state{}.
cancel_batch(#state{batch_start=BatchStart,
                    riak=RiakPid}=State) ->
    %% Interrupt the batch of deletes
    _ = lager:info("Canceled garbage collection batch after ~b seconds.",
                   [elapsed(BatchStart)]),
    riak_cs_riakc_pool_worker:stop(RiakPid),
    schedule_next(State#state{batch=[],
                              riak=undefined}).

-spec continue() -> ok.
%% @private
%% @doc Send an asynchronous `continue' event. This is used to advance
%% the FSM to the next state and perform some blocking action. The blocking
%% actions are done in the beginning of the next state to give the FSM a
%% chance to respond to events from the outside world.
continue() ->
    gen_fsm:send_event(self(), continue).

%% @doc How many seconds have passed from `Time' to now.
-spec elapsed(undefined | non_neg_integer()) -> non_neg_integer().
elapsed(undefined) ->
    riak_cs_gc:timestamp();
elapsed(Time) ->
    Now = riak_cs_gc:timestamp(),
    case (Diff = Now - Time) > 0 of
        true ->
            Diff;
        false ->
            0
    end.

%% @doc Fetch the list of keys for file manifests that are eligible
%% for delete.
-spec fetch_eligible_manifest_keys(pid(), non_neg_integer()) -> [binary()].
fetch_eligible_manifest_keys(RiakPid, IntervalStart) ->
    EndTime = list_to_binary(integer_to_list(IntervalStart)),
    eligible_manifest_keys(gc_index_query(RiakPid, EndTime)).

eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}},
                        _EndTime}) ->
    Keys;
eligible_manifest_keys({{error, Reason}, EndTime}) ->
    _ = lager:warning("Error occurred trying to query from time 0 to ~p"
                      "in gc key index. Reason: ~p",
                      [EndTime, Reason]),
    [].

gc_index_query(RiakPid, EndTime) ->
    QueryResult = riakc_pb_socket:get_index(RiakPid,
                                            ?GC_BUCKET,
                                            ?KEY_INDEX,
                                            riak_cs_gc:epoch_start(),
                                            EndTime),
    {QueryResult, EndTime}.

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

%% @doc Take required actions to pause garbage collection and update
%% the state record for the transition to `paused'.
-spec pause_gc(atom(), #state{}) -> #state{}.
pause_gc(idle, State=#state{interval=Interval,
                            timer_ref=TimerRef}) ->
    _ = lager:info("Pausing garbage collection"),
    Remainder = cancel_timer(Interval, TimerRef),
    State#state{pause_state=idle,
                interval_remaining=Remainder};
pause_gc(State, StateData) ->
    _ = lager:info("Pausing garbage collection"),
    StateData#state{pause_state=State}.

-spec cancel_timer(timeout(), 'undefined' | timer:tref()) -> 'undefined' | integer().
cancel_timer(_, undefined) ->
    undefined;
cancel_timer(infinity, TimerRef) ->
    %% Cancel the timer in case the interval has
    %% recently be set to `infinity'.
    _ = erlang:cancel_timer(TimerRef),
    undefined;
cancel_timer(_, TimerRef) ->
    handle_cancel_timer(erlang:cancel_timer(TimerRef)).

handle_cancel_timer(false) ->
    0;
handle_cancel_timer(RemainderMillis) ->
    RemainderMillis.

-spec resume_gc(#state{}) -> #state{}.
resume_gc(State) ->
    _ = lager:info("Resuming garbage collection"),
    ok = continue(),
    State#state{pause_state=undefined}.

-spec ok_reply(atom(), #state{}) -> {reply, ok, atom(), #state{}}.
ok_reply(NextState, NextStateData) ->
    {reply, ok, NextState, NextStateData}.

%% @doc Setup the automatic trigger to start the next
%% scheduled batch calculation.
-spec schedule_next(#state{}) -> #state{}.
schedule_next(#state{interval=infinity}=State) ->
    %% nothing to schedule, all triggers manual
    State;
schedule_next(#state{batch_start=Current,
                     interval=Interval}=State) ->
    Next = calendar:gregorian_seconds_to_datetime(
             riak_cs_gc:timestamp() + Interval),
    _ = lager:debug("Scheduling next garbage collection for ~p",
                    [Next]),
    TimerRef = erlang:send_after(Interval*1000, self(), start_batch),
    State#state{batch_start=undefined,
                last=Current,
                next=Next,
                timer_ref=TimerRef}.

%% @doc Actually kick off the batch.  After calling this function, you
%% must advance the FSM state to `fetching_next_fileset'.
%% Intentionally pattern match on an undefined Riak handle.
start_batch(State=#state{riak=undefined}) ->
    %% this does not check out a worker from the riak
    %% connection pool; instead it creates a fresh new worker,
    %% the idea being that we don't want to delay deletion
    %% just because the normal request pool is empty; pool
    %% workers just happen to be literally the socket process,
    %% so "starting" one here is the same as opening a
    %% connection, and avoids duplicating the configuration
    %% lookup code
    {ok, Riak} = riak_cs_riakc_pool_worker:start_link([]),
    BatchStart = riak_cs_gc:timestamp(),
    Batch = fetch_eligible_manifest_keys(Riak, BatchStart),
    _ = lager:debug("Batch keys: ~p", [Batch]),
    ok = continue(),
    State#state{batch_start=BatchStart,
                batch=Batch,
                batch_count=0,
                batch_skips=0,
                manif_count=0,
                block_count=0,
                riak=Riak}.

-spec start_manual_batch(boolean(), #state{}) -> #state{}.
start_manual_batch(true, State) ->
    State#state{batch=undefined};
start_manual_batch(false, State) ->
    start_batch(State).

%% @doc Extract a list of status information from a state record.
%%
%% CAUTION: Do not add side-effects to this function: it is called specutively.
-spec status_data(#state{}) -> [{atom(), term()}].
status_data(State) ->
    [{interval, State#state.interval},
     {last, State#state.last},
     {current, State#state.batch_start},
     {next, State#state.next},
     {elapsed, elapsed(State#state.batch_start)},
     {files_deleted, State#state.batch_count},
     {files_skipped, State#state.batch_skips},
     {files_left, if is_list(State#state.batch) -> length(State#state.batch);
                     true                       -> 0
                  end}].

handle_common_sync_reply(Msg, Common, _State) when is_atom(Msg) ->
    proplists:get_value(Msg, Common, unknown_command);
handle_common_sync_reply({MsgBase, _}, Common, State) when is_atom(MsgBase) ->
    handle_common_sync_reply(MsgBase, Common, State).

%% Refactor TODO:
%%   1. delete_fsm_pid=undefined is desirable in both ok & error cases?
%%   2. It's correct to *not* change pause_state?
handle_delete_fsm_reply({ok, {TotalBlocks, TotalBlocks}},
                        #state{current_files=[CurrentManifest | RestManifests],
                               current_fileset=FileSet,
                               block_count=BlockCount} = State) ->
    ok = continue(),
    UpdFileSet = twop_set:del_element(CurrentManifest, FileSet),
    State#state{delete_fsm_pid=undefined,
                current_fileset=UpdFileSet,
                current_files=RestManifests,
                block_count=BlockCount+TotalBlocks};
handle_delete_fsm_reply({ok, {NumDeleted, _TotalBlocks}},
                        #state{current_files=[_CurrentManifest | RestManifests],
                               block_count=BlockCount} = State) ->
    ok = continue(),
    State#state{delete_fsm_pid=undefined,
                current_files=RestManifests,
                block_count=BlockCount+NumDeleted};
handle_delete_fsm_reply({error, _}, #state{current_files=[_ | RestManifests]} = State) ->
    ok = continue(),
    State#state{delete_fsm_pid=undefined,
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
-spec current_state() -> {atom(), #state{}} | {error, term()}.
current_state() ->
    gen_fsm:sync_send_all_state_event(?SERVER, current_state).

%% @doc Manipulate the current state of the fsm for testing
change_state(State) ->
    gen_fsm:sync_send_all_state_event(?SERVER, {change_state, State}).

-endif.
