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

%% State transitions by event (not including transitions by command)
%% 1. There is outer loop of `idle' <==> `fetching_next_batch'.
%%    Single GC run starts by `idle'->`fetching_next_batch' and ends by
%%    `fetching_next_batch'->`idle'.
%% 2. `fetching_next_batch', `feeding_workers' and `waiting_for_workers'
%%    make up inner loop. If there is a room for workers, this process fetches
%%    fileset keys by 2i. Then it moves to `feeding_workers' and feeds sets of
%%    fileset keys to workers. If workers are full or 2i reaches the end,
%%    it collects results from workers at `waiting_for_workers'.
%%
%%                   idle <---------------+         Extra:{paused, StateToResume}
%%                    |                   |
%%       [manual_batch or timer]          | [no fileset keys AND
%%                    |                   |  no worker]
%%                    V                   |
%%              fetching_next_batch ------+
%%                    |        ^
%%                    |        |
%%                    |        +----------------------+
%% [no fileset keys]  |                               |
%%   +----------------+                               |
%%   |                |                       [no fileset keys]
%%   |          [fileset keys]                        |
%%   |                |                               |
%%   |           feeding_workers ---------------------+
%%   |                |     ^                         |
%%   |                |     |                         |
%%   |                |     +---------+--+            |
%%   |                |               |  |            |
%%   |                |        [more workers AND      |
%%   |       +--------+         more fileset keys]    |
%%   |       |        |               |  |            |
%%   |  [all active]  |               |  |            |
%%   |       |        +---------------+  |            |
%%   |       V                           |            |
%%   +--> waiting_for_workers -----------+------------+

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

-export([current_state/0]).

-include("riak_cs_gc_d.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0,
         test_link/1,
         change_state/1,
         status_data/1]).

-endif.

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
%% times of the last GC and the next planned GC.
%% When `deleting' or `paused' details also the scheduled time of
%% the active GC, the number of seconds the process has been
%% running so far, and counts of how many filesets have been
%% processed.
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

%% @doc Cancel the garbage collection currently in progress.  Returns `ok' if
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
-spec set_interval(term()) -> ok | {error, term()}.
set_interval(Interval) ->
    gen_fsm:sync_send_all_state_event(?SERVER, {set_interval, Interval}, infinity).

%% @doc Stop the daemon
-spec stop() -> ok | {error, term()}.
stop() ->
    gen_fsm:sync_send_all_state_event(?SERVER, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init(Args) ->
    Interval = riak_cs_gc:gc_interval(),
    InitialDelay = riak_cs_gc:initial_gc_delay(),
    MaxWorkers =  riak_cs_gc:gc_max_workers(),
    Testing = lists:member(testing, Args),
    SchedState = schedule_next(?STATE{interval=Interval,
                                      initial_delay=InitialDelay,
                                      max_workers=MaxWorkers,
                                      testing=Testing}),
    {ok, idle, SchedState}.

%% Asynchronous events

%% @doc Transitions out of idle are all synchronous events
idle(_, State=?STATE{interval_remaining=undefined}) ->
    {next_state, idle, State};
idle(_, State=?STATE{interval_remaining=IntervalRemaining}) ->
    TimerRef = erlang:send_after(IntervalRemaining,
                                 self(),
                                 {start_batch, riak_cs_gc:leeway_seconds()}),
    {next_state, idle, State?STATE{timer_ref=TimerRef}}.

%% @doc Async transitions from `fetching_next_batch' are all due to
%% messages the FSM sends itself, in order to have opportunities to
%% handle messages from the outside world (like `status').
fetching_next_batch(_, State=?STATE{batch=undefined}) ->
    %% This clause is for testing only
    {next_state, fetching_next_batch, State};
fetching_next_batch(continue, ?STATE{batch_start=undefined,
                                     leeway=Leeway}=State) ->
    BatchStart = riak_cs_gc:timestamp(),
    %% Fetch the next set of manifests for deletion
    {KeyListRes, KeyListState} =
        riak_cs_gc_key_list:new(BatchStart, Leeway),
    #gc_key_list_result{bag_id=BagId, batch=Batch} = KeyListRes,
    _ = lager:debug("Initial batch keys: ~p", [Batch]),
    NewStateData = State?STATE{batch=Batch,
                               batch_start=BatchStart,
                               key_list_state=KeyListState,
                               bag_id=BagId},
    ok = continue(),
    {next_state, feeding_workers, NewStateData};
fetching_next_batch(continue, ?STATE{active_workers=0,
                                     batch=[],
                                     key_list_state=undefined,
                                     batch_caller=Caller} = State) ->
    %% finished with this GC run
    _ = case Caller of
            undefined -> ok;
            _ -> Caller ! {batch_finished, State}
        end,
    _ = lager:info("Finished garbage collection: "
                   "~b seconds, ~p batch_count, ~p batch_skips, "
                   "~p manif_count, ~p block_count\n",
                   [elapsed(State?STATE.batch_start), State?STATE.batch_count,
                    State?STATE.batch_skips, State?STATE.manif_count,
                    State?STATE.block_count]),
    NewState = schedule_next(State?STATE{batch_start=undefined,
                                         batch_caller=undefined}),
    {next_state, idle, NewState};
fetching_next_batch(continue, ?STATE{batch=[],
                                     key_list_state=undefined}=State) ->
    {next_state, waiting_for_workers, State};
fetching_next_batch(continue, ?STATE{key_list_state=undefined}=State) ->
    {next_state, feeding_workers, State};
fetching_next_batch(continue, ?STATE{key_list_state=KeyListState}=State) ->
    %% Fetch the next set of manifests for deletion
    {KeyListRes, UpdKeyListState} = riak_cs_gc_key_list:next(KeyListState),
    #gc_key_list_result{bag_id=BagId, batch=Batch} = KeyListRes,
    _ = lager:debug("Next batch keys: ~p", [Batch]),
    NewStateData = State?STATE{batch=Batch,
                               key_list_state=UpdKeyListState,
                               bag_id=BagId},
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

idle({manual_batch, Options}, {CallerPid, _Tag}=_From, State) ->
    Leeway = leeway_option(Options),
    ok_reply(fetching_next_batch, start_manual_batch(
                                    lists:member(testing, Options),
                                    Leeway,
                                    State?STATE{batch_caller=CallerPid}));
idle(pause, _From, State) ->
    ok_reply(paused, pause_gc(idle, State));
idle(Msg, _From, State) ->
    Common = [{status, {ok, {idle, [{interval, State?STATE.interval},
                                    {leeway, riak_cs_gc:leeway_seconds()},
                                    {last, State?STATE.last},
                                    {next, State?STATE.next}]}}},
              {cancel_batch, {error, no_batch}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), idle, State}.

fetching_next_batch(pause, _From, State) ->
    ok_reply(paused, pause_gc(fetching_next_batch, State));
fetching_next_batch(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
fetching_next_batch(Msg, _From, State) ->
    Common = [{status, {ok, {fetching_next_batch, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), fetching_next_batch, State}.

feeding_workers(pause, _From, State) ->
    ok_reply(paused, pause_gc(feeding_workers, State));
feeding_workers(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
feeding_workers(Msg, _From, State) ->
    Common = [{status, {ok, {feeding_workers, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), feeding_workers, State}.

waiting_for_workers(pause, _From, State) ->
    ok_reply(paused, pause_gc(waiting_for_workers, State));
waiting_for_workers(cancel_batch, _From, State) ->
    ok_reply(idle, cancel_batch(State));
waiting_for_workers(Msg, _From, State) ->
    Common = [{status, {ok, {waiting_for_workers, status_data(State)}}},
              {manual_batch, {error, already_deleting}},
              {resume, {error, not_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), waiting_for_workers, State}.

paused(resume, _From, State=?STATE{pause_state=PauseState}) ->
    ok_reply(PauseState, resume_gc(State));
paused(cancel_batch, _From, State) ->
    ok_reply(paused, cancel_batch(State?STATE{pause_state=idle}));
paused(Msg, _From, State) ->
    Common = [{status, {ok, {paused, status_data(State)}}},
              {pause, {error, already_paused}},
              {manual_batch, {error, already_paused}}],
    {reply, handle_common_sync_reply(Msg, Common, State), paused, State}.

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), ?STATE{}) ->
                               {reply, term(), atom(), ?STATE{}}.
handle_sync_event({set_interval, Interval}, _From, StateName, State) ->
    {Reply, NewState} = case riak_cs_gc:set_gc_interval(Interval) of
                            ok -> {ok, State?STATE{interval=Interval}};
                            {error, Reason} -> {{error, Reason}, State}
                        end,
    {reply, Reply, StateName, NewState};
handle_sync_event(current_state, _From, StateName, State) ->
    {reply, {StateName, State}, StateName, State};
handle_sync_event({change_state, NewStateName}, _From, _StateName, State) ->
    ok_reply(NewStateName, State);
handle_sync_event(stop, _From, _StateName, State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    ok_reply(StateName, State).

handle_info({start_batch, Leeway}, idle, State) ->
    NewState = start_batch(Leeway, State),
    {next_state, fetching_next_batch, NewState};
handle_info({start_batch, _}, InBatch, State) ->
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
start_worker(State=?STATE{testing=true}) ->
    State;
start_worker(State=?STATE{batch=[NextBatch | RestBatches],
                          bag_id=BagId,
                          active_workers=ActiveWorkers,
                          worker_pids=WorkerPids}) ->
     case ?GC_WORKER:start_link(BagId, NextBatch) of
         {ok, Pid} ->
             State?STATE{batch=RestBatches,
                         active_workers=ActiveWorkers + 1,
                         worker_pids=[Pid | WorkerPids]};
         {error, _Reason} ->
             State
     end.

%% @doc Cancel the current batch of files set for garbage collection.
-spec cancel_batch(?STATE{}) -> ?STATE{}.
cancel_batch(?STATE{batch_start=BatchStart,
                    worker_pids=WorkerPids}=State) ->
    %% Interrupt the batch of deletes
    _ = lager:info("Canceled garbage collection batch after ~b seconds.",
                   [elapsed(BatchStart)]),
    _ = [riak_cs_gc_worker:stop(P) || P <- WorkerPids],
    schedule_next(State?STATE{batch=[],
                              worker_pids=[],
                              active_workers=0}).

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
schedule_next(?STATE{initial_delay=undefined}=State) ->
    schedule_next(State, 0);
schedule_next(?STATE{initial_delay=InitialDelay}=State) ->
    schedule_next(State?STATE{initial_delay=undefined}, InitialDelay).

schedule_next(?STATE{next=Last, interval=Interval}=State, InitialDelay) ->
    RevisedNext = riak_cs_gc:timestamp() + Interval,
    TimerValue = Interval * 1000 + InitialDelay * 1000,
    TimerRef = erlang:send_after(TimerValue, self(),
                                 {start_batch, riak_cs_gc:leeway_seconds()}),
    State?STATE{batch_start=undefined,
                last=Last,
                next=RevisedNext,
                timer_ref=TimerRef}.

%% @doc Actually kick off the batch.  After calling this function, you
%% must advance the FSM state to `fetching_next_batch'.
%% Intentionally pattern match on an undefined Riak handle.
start_batch(Leeway, State) ->
    %% this does not check out a worker from the riak
    %% connection pool; instead it creates a fresh new worker,
    %% the idea being that we don't want to delay deletion
    %% just because the normal request pool is empty; pool
    %% workers just happen to be literally the socket process,
    %% so "starting" one here is the same as opening a
    %% connection, and avoids duplicating the configuration
    %% lookup code
    ok = continue(),
    State?STATE{batch_count=0,
                batch_skips=0,
                manif_count=0,
                block_count=0,
                leeway=Leeway}.

-spec start_manual_batch(Testing::boolean(), non_neg_integer(), ?STATE{}) -> ?STATE{}.
start_manual_batch(true, _, State) ->
    State?STATE{batch=undefined};
start_manual_batch(false, Leeway, State) ->
    start_batch(Leeway, State?STATE{batch=[]}).

%% @doc Extract a list of status information from a state record.
%%
%% CAUTION: Do not add side-effects to this function: it is called specutively.
-spec status_data(?STATE{}) -> [{atom(), term()}].
status_data(State) ->
    [{interval, State?STATE.interval},
     {leeway, riak_cs_gc:leeway_seconds()},
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

-spec leeway_option(list()) -> non_neg_integer().
leeway_option(Options) ->
    case lists:keyfind(leeway, 1, Options) of
        {leeway, Leeway} ->
            Leeway;
        false ->
            riak_cs_gc:leeway_seconds()
    end.

%% ===================================================================
%% Test API and tests
%% ===================================================================

%% @doc Get the current state of the fsm for testing inspection
-spec current_state() -> {atom(), ?STATE{}} | {error, term()}.
current_state() ->
    gen_fsm:sync_send_all_state_event(?SERVER, current_state).

-ifdef(TEST).

%% Start the garbage collection server
test_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [testing], []).

%% Start the garbage collection server
test_link(Interval) ->
    application:set_env(riak_cs, gc_interval, Interval),
    test_link().

%% Manipulate the current state of the fsm for testing
change_state(State) ->
    gen_fsm:sync_send_all_state_event(?SERVER, {change_state, State}).

-endif.
