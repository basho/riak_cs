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
         fetching_next_batch/2,
         fetching_next_batch/3,
         feeding_workers/2,
         feeding_workers/3,
         waiting_for_workers/2,
         waiting_for_workers/3,
         paused/2,
         paused/3,
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

-export([current_state/0]).

-define(SERVER, ?MODULE).
-define(STATE, #gc_d_state).
-define(GC_WORKER, riak_cs_gc_worker).

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
    InitialDelay = riak_cs_gc:initial_gc_delay(),
    SchedState = schedule_next(?STATE{interval=Interval,
                                      initial_delay=InitialDelay}),
    {ok, idle, SchedState}.

%% Asynchronous events

%% @doc Transitions out of idle are all synchronous events
idle(_, State=?STATE{interval_remaining=undefined}) ->
    {next_state, idle, State};
idle(_, State=?STATE{interval_remaining=IntervalRemaining}) ->
    TimerRef = erlang:send_after(IntervalRemaining, self(), start_batch),
    {next_state, idle, State?STATE{timer_ref=TimerRef}}.

%% @doc Async transitions from `fetching_next_batch' are all due to
%% messages the FSM sends itself, in order to have opportunities to
%% handle messages from the outside world (like `status').
fetching_next_batch(_, State=?STATE{batch=undefined}) ->
    %% This clause is for testing only
    {next_state, fetching_next_batch, State};
fetching_next_batch(continue, ?STATE{batch_start=undefined,
                                     continuation=undefined,
                                     riak=RiakPid}=State) ->
    BatchStart = riak_cs_gc:timestamp(),
    %% Fetch the next set of manifests for deletion
    {Batch, Continuation} =
        fetch_eligible_manifest_keys(RiakPid, BatchStart, undefined),
    _ = lager:debug("Batch keys: ~p", [Batch]),
    NewStateData = State?STATE{batch=Batch,
                               batch_start=BatchStart,
                               continuation=Continuation},
    ok = continue(),
    {next_state, feeding_workers, NewStateData};
fetching_next_batch(continue, ?STATE{active_workers=0,
                                     batch=[],
                                     continuation=undefined}=State) ->
    %% finished with this batch
    _ = lager:info("Finished garbage collection: "
                   "~b seconds, ~p batch_count, ~p batch_skips, "
                   "~p manif_count, ~p block_count\n",
                   [elapsed(State?STATE.batch_start), State?STATE.batch_count,
                    State?STATE.batch_skips, State?STATE.manif_count,
                    State?STATE.block_count]),
    riak_cs_riakc_pool_worker:stop(State?STATE.riak),
    NewState = schedule_next(State?STATE{riak=undefined,
                                         batch_start=undefined}),
    {next_state, idle, NewState};
fetching_next_batch(continue, ?STATE{batch=[],
                                     continuation=undefined}=State) ->
    {next_state, waiting_for_workers, State};
fetching_next_batch(continue, ?STATE{continuation=undefined}=State) ->
    {next_state, feeding_workers, State};
fetching_next_batch(continue, ?STATE{batch_start=BatchStart,
                                     continuation=Continuation,
                                     riak=RiakPid}=State) ->
    %% Fetch the next set of manifests for deletion
    {Batch, UpdContinuation} =
        fetch_eligible_manifest_keys(RiakPid, BatchStart, Continuation),
    _ = lager:debug("Batch keys: ~p", [Batch]),
    NewStateData = State?STATE{batch=Batch,
                               continuation=UpdContinuation},
    ok = continue(),
    {next_state, feeding_workers, NewStateData};
fetching_next_batch({batch_complete, WorkerPid, WorkerState}, State) ->
    NewStateData = handle_batch_complete(WorkerPid, WorkerState, State),
    ok = continue(),
    {next_state, fetching_next_batch, NewStateData};
fetching_next_batch(_, State) ->
    {next_state, fetching_next_batch, State}.

%% @doc Async transitions from `feeding_workers' are all due to
%% messages the FSM sends itself, in order to have opportunities to
%% handle messages from the outside world (like `status').
feeding_workers(continue, ?STATE{max_workers=WorkerCount,
                                 active_workers=WorkerCount}=State)
  when WorkerCount > 0 ->
    %% Worker capacity has been reached so must wait for a worker to
    %% finish before assigning more work.
    {next_state, waiting_for_workers, State};
feeding_workers(continue, ?STATE{batch=[]}=State) ->
    %% No outstanding work to hand out
    ok = continue(),
    {next_state, fetching_next_batch, State};
feeding_workers(continue, State) ->
    %% Start worker process
    NextStateData = start_worker(State),
    ok = continue(),
    {next_state, feeding_workers, NextStateData};
feeding_workers({batch_complete, WorkerPid, WorkerState}, State) ->
    NewStateData = handle_batch_complete(WorkerPid, WorkerState, State),
    ok = continue(),
    {next_state, feeding_workers, NewStateData};
feeding_workers(_, State) ->
    {next_state, feeding_workers, State}.

%% @doc This state initiates the deletion of a file from
%% a set of manifests stored for a particular key in the
%% garbage collection bucket.
waiting_for_workers({batch_complete, WorkerPid, WorkerState}, State=?STATE{batch=[]}) ->
    NewStateData = handle_batch_complete(WorkerPid, WorkerState, State),
    ok = continue(),
    {next_state, fetching_next_batch, NewStateData};
waiting_for_workers({batch_complete, WorkerPid, WorkerState}, State) ->
    NewStateData = handle_batch_complete(WorkerPid, WorkerState, State),
    ok = continue(),
    {next_state, feeding_workers, NewStateData};
waiting_for_workers(_, State) ->
    {next_state, waiting_for_workers, State}.

paused({batch_complete, WorkerPid, WorkerState}, State=?STATE{batch=[]}) ->
    NewStateData = handle_batch_complete(WorkerPid, WorkerState, State),
    {next_state, paused, NewStateData};
paused(_, State) ->
    {next_state, paused, State}.

%% Synchronous events

idle({manual_batch, Options}, _From, State) ->
    ok_reply(fetching_next_batch, start_manual_batch(
                                    lists:member(testing, Options),
                                    State));
idle(pause, _From, State) ->
    ok_reply(paused, pause_gc(idle, State));
idle({set_interval, Interval}, _From, State)
  when is_integer(Interval) orelse Interval == infinity ->
    ok_reply(idle, State?STATE{interval=Interval});
idle(Msg, _From, State) ->
    Common = [{status, {ok, {idle, [{interval, State?STATE.interval},
                                    {last, State?STATE.last},
                                    {next, State?STATE.next}]}}},
              {cancel_batch, {error, no_batch}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), idle, State}.

fetching_next_batch(pause, _From, State) ->
    ok_reply(paused, pause_gc(fetching_next_batch, State));
fetching_next_batch(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
fetching_next_batch({set_interval, Interval}, _From, State) ->
    ok_reply(fetching_next_batch, State?STATE{interval=Interval});
fetching_next_batch(Msg, _From, State) ->
    Common = [{status, {ok, {fetching_next_batch, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), fetching_next_batch, State}.

feeding_workers(pause, _From, State) ->
    ok_reply(paused, pause_gc(feeding_workers, State));
feeding_workers(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
feeding_workers({set_interval, Interval}, _From, State) ->
    ok_reply(feeding_workers, State?STATE{interval=Interval});
feeding_workers(Msg, _From, State) ->
    Common = [{status, {ok, {feeding_workers, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), feeding_workers, State}.

waiting_for_workers(pause, _From, State) ->
    ok_reply(paused, pause_gc(waiting_for_workers, State));
waiting_for_workers(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
waiting_for_workers({set_interval, Interval}, _From, State) ->
    ok_reply(waiting_for_workers, State?STATE{interval=Interval});
waiting_for_workers(Msg, _From, State) ->
    Common = [{status, {ok, {waiting_for_workers, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), waiting_for_workers, State}.

paused(resume, _From, State=?STATE{pause_state=PauseState}) ->
    ok_reply(PauseState, resume_gc(State));
paused(cancel_batch, _From, State) ->
    ok_reply(paused, cancel_batch(State?STATE{pause_state=idle}));
paused({set_interval, Interval}, _From, State) ->
    ok_reply(paused, State?STATE{interval=Interval});
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

handle_info(start_batch, idle, State) ->
    NewState = start_batch(State),
    {next_state, fetching_next_batch, NewState};
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

%% @doc Handle a `batch_complete' event from a GC worker process.
-spec handle_batch_complete(pid(), #gc_worker_state{}, ?STATE{}) -> ?STATE{}.
handle_batch_complete(WorkerPid, WorkerState, State) ->
    ?STATE{active_workers=ActiveWorkers,
           worker_pids=WorkerPids,
           batch_count=BatchCount,
           batch_skips=BatchSkips,
           manif_count=ManifestCount,
           block_count=BlockCount} = State,
    #gc_worker_state{batch_count=WorkerBatchCount,
                     batch_skips=WorkerBatchSkips,
                     manif_count=WorkerManifestCount,
                     block_count=WorkerBlockCount} = WorkerState,
    UpdWorkerPids = lists:delete(WorkerPid, WorkerPids),
    %% @TODO Workout the terminiology for these stats. i.e. Is batch
    %% count just an increment or represenative of something else.
    State?STATE{active_workers=ActiveWorkers - 1,
                worker_pids=UpdWorkerPids,
                batch_count=BatchCount + WorkerBatchCount,
                batch_skips=BatchSkips + WorkerBatchSkips,
                manif_count=ManifestCount + WorkerManifestCount,
                block_count=BlockCount + WorkerBlockCount}.

%% @doc Start a GC worker and return the apprpriate next state and
%% updated state record.
-spec start_worker(?STATE{}) -> ?STATE{}.
start_worker(State=?STATE{batch=[NextBatch | RestBatches],
                          active_workers=ActiveWorkers,
                          worker_pids=WorkerPids}) ->
     case ?GC_WORKER:start_link(NextBatch) of
         {ok, Pid} ->
             _ = lager:debug("Starting worker: ~p", [Pid]),
             State?STATE{batch=RestBatches,
                         active_workers=ActiveWorkers + 1,
                         worker_pids=[Pid | WorkerPids]};
         {error, _Reason} ->
             State
     end.

%% @doc Cancel the current batch of files set for garbage collection.
-spec cancel_batch(?STATE{}) -> ?STATE{}.
cancel_batch(?STATE{batch_start=BatchStart,
                    riak=RiakPid}=State) ->
    %% Interrupt the batch of deletes
    _ = lager:info("Canceled garbage collection batch after ~b seconds.",
                   [elapsed(BatchStart)]),
    riak_cs_riakc_pool_worker:stop(RiakPid),
    schedule_next(State?STATE{batch=[],
                              riak=undefined}).

%% @private
%% @doc Send an asynchronous `continue' event. This is used to advance
%% the FSM to the next state and perform some blocking action. The blocking
%% actions are done in the beginning of the next state to give the FSM a
%% chance to respond to events from the outside world.
-spec continue() -> ok.
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
-spec fetch_eligible_manifest_keys(pid(), non_neg_integer(), undefined | binary()) ->
                                          {[[binary()]], undefined | binary()}.
fetch_eligible_manifest_keys(RiakPid, IntervalStart, Continuation) ->
    EndTime = list_to_binary(integer_to_list(IntervalStart)),
    UsePaginatedIndexes = riak_cs_config:paginated_indexes(),
    QueryResults = gc_index_query(RiakPid,
                                  EndTime,
                                  riak_cs_config:gc_batch_size(),
                                  Continuation,
                                  UsePaginatedIndexes),
    {eligible_manifest_keys(QueryResults, UsePaginatedIndexes), continuation(QueryResults)}.

-spec eligible_manifest_keys({{ok, riakc_pb_socket:index_results()} | {error, term()},
                              binary()}, boolean()) -> [binary()].
eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}},
                        _EndTime},
                       true) ->
    [Keys];
eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}},
                        _EndTime},
                       false) ->
    eligible_manifest_key_sets(Keys);
eligible_manifest_keys({{error, Reason}, EndTime}, _) ->
    _ = lager:warning("Error occurred trying to query from time 0 to ~p"
                      "in gc key index. Reason: ~p",
                      [EndTime, Reason]),
    [].

%% @doc Break a list of gc-eligible keys from the GC bucket into smaller sets
%% to be processed by different GC workers. This is primarily used when paginated
%%
-spec eligible_manifest_key_sets([binary()]) -> [[binary()]].
eligible_manifest_key_sets(Keys) ->
    BatchSize = riak_cs_config:gc_batch_size(),
    [lists:sublist(Keys, Index, BatchSize) || Index <- lists:seq(1, length(Keys), BatchSize)].

-spec continuation({{ok, riakc_pb_socket:index_results()} | {error, term()},
                    binary()}) -> undefined | binary().
continuation({{ok, ?INDEX_RESULTS{continuation=Continuation}},
              _EndTime}) ->
    Continuation;
continuation({{error, _}, _EndTime}) ->
    undefined.

-spec gc_index_query(pid(), binary(), non_neg_integer(), binary(), boolean()) ->
                            {riakc_pb_socket:index_results(), binary()}.
gc_index_query(RiakPid, EndTime, BatchSize, Continuation, true) ->
    Options = [{max_results, BatchSize},
               {continuation, Continuation}],
    QueryResult = riakc_pb_socket:get_index_range(RiakPid,
                                                  ?GC_BUCKET,
                                                  ?KEY_INDEX,
                                                  riak_cs_gc:epoch_start(),
                                                  EndTime,
                                                  Options),
    {QueryResult, EndTime};
gc_index_query(RiakPid, EndTime, _, _, false) ->
    QueryResult = riakc_pb_socket:get_index(RiakPid,
                                            ?GC_BUCKET,
                                            ?KEY_INDEX,
                                            riak_cs_gc:epoch_start(),
                                            EndTime),
    {QueryResult, EndTime}.

%% @doc Take required actions to pause garbage collection and update
%% the state record for the transition to `paused'.
-spec pause_gc(atom(), ?STATE{}) -> ?STATE{}.
pause_gc(idle, State=?STATE{interval=Interval,
                            timer_ref=TimerRef}) ->
    _ = lager:info("Pausing garbage collection"),
    Remainder = cancel_timer(Interval, TimerRef),
    State?STATE{pause_state=idle,
                interval_remaining=Remainder};
pause_gc(State, StateData) ->
    _ = lager:info("Pausing garbage collection"),
    StateData?STATE{pause_state=State}.

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

-spec resume_gc(?STATE{}) -> ?STATE{}.
resume_gc(State) ->
    _ = lager:info("Resuming garbage collection"),
    ok = continue(),
    State?STATE{pause_state=undefined}.

-spec ok_reply(atom(), ?STATE{}) -> {reply, ok, atom(), ?STATE{}}.
ok_reply(NextState, NextStateData) ->
    {reply, ok, NextState, NextStateData}.

%% @doc Setup the automatic trigger to start the next
%% scheduled batch calculation.
-spec schedule_next(?STATE{}) -> ?STATE{}.
schedule_next(?STATE{interval=infinity}=State) ->
    %% nothing to schedule, all triggers manual
    State;
schedule_next(?STATE{batch_start=Current,
                     interval=Interval,
                     initial_delay=undefined}=State) ->
    Next = calendar:gregorian_seconds_to_datetime(
             riak_cs_gc:timestamp() + Interval),
    _ = lager:debug("Scheduling next garbage collection for ~p",
                    [Next]),
    TimerRef = erlang:send_after(Interval*1000, self(), start_batch),
    State?STATE{batch_start=undefined,
                last=Current,
                next=Next,
                timer_ref=TimerRef};
schedule_next(?STATE{batch_start=Current,
                     interval=Interval,
                     initial_delay=InitialDelay}=State) ->
    Next = calendar:gregorian_seconds_to_datetime(
             riak_cs_gc:timestamp() + Interval),
    _ = lager:debug("Scheduling next garbage collection for ~p",
                    [Next]),
    TimerValue = Interval * 1000 + InitialDelay * 1000,
    TimerRef = erlang:send_after(TimerValue, self(), start_batch),
    State?STATE{batch_start=undefined,
                last=Current,
                next=Next,
                timer_ref=TimerRef,
                initial_delay=undefined}.

%% @doc Actually kick off the batch.  After calling this function, you
%% must advance the FSM state to `fetching_next_fileset'.
%% Intentionally pattern match on an undefined Riak handle.
start_batch(State=?STATE{riak=undefined}) ->
    %% this does not check out a worker from the riak
    %% connection pool; instead it creates a fresh new worker,
    %% the idea being that we don't want to delay deletion
    %% just because the normal request pool is empty; pool
    %% workers just happen to be literally the socket process,
    %% so "starting" one here is the same as opening a
    %% connection, and avoids duplicating the configuration
    %% lookup code
    {ok, Riak} = riak_cs_riakc_pool_worker:start_link([]),
    ok = continue(),
    State?STATE{batch_count=0,
                batch_skips=0,
                manif_count=0,
                block_count=0,
                riak=Riak}.

-spec start_manual_batch(boolean(), ?STATE{}) -> ?STATE{}.
start_manual_batch(true, State) ->
    State?STATE{batch=undefined};
start_manual_batch(false, State) ->
    start_batch(State?STATE{batch=[]}).

%% @doc Extract a list of status information from a state record.
%%
%% CAUTION: Do not add side-effects to this function: it is called specutively.
-spec status_data(?STATE{}) -> [{atom(), term()}].
status_data(State) ->
    [{interval, State?STATE.interval},
     {last, State?STATE.last},
     {current, State?STATE.batch_start},
     {next, State?STATE.next},
     {elapsed, elapsed(State?STATE.batch_start)},
     {files_deleted, State?STATE.batch_count},
     {files_skipped, State?STATE.batch_skips},
     {files_left, if is_list(State?STATE.batch) -> length(State?STATE.batch);
                     true                       -> 0
                  end}].

handle_common_sync_reply(Msg, Common, _State) when is_atom(Msg) ->
    proplists:get_value(Msg, Common, unknown_command);
handle_common_sync_reply({MsgBase, _}, Common, State) when is_atom(MsgBase) ->
    handle_common_sync_reply(MsgBase, Common, State).

%% ===================================================================
%% Test API
%% ===================================================================

%% @doc Get the current state of the fsm for testing inspection
-spec current_state() -> {atom(), ?STATE{}} | {error, term()}.
current_state() ->
    gen_fsm:sync_send_all_state_event(?SERVER, current_state).

-ifdef(TEST).

%% @doc Start the garbage collection server
test_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [testing], []).

%% @doc Start the garbage collection server
test_link(Interval) ->
    application:set_env(riak_cs, gc_interval, Interval),
    test_link().



%% @doc Manipulate the current state of the fsm for testing
change_state(State) ->
    gen_fsm:sync_send_all_state_event(?SERVER, {change_state, State}).

-endif.
