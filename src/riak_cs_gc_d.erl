%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc The daemon that handles garbage collection of deleted file
%% manifests and blocks.

-module(riak_cs_gc_d).

-behaviour(gen_fsm).

%% API
-export([start_link/0,
         status/0,
         manual_batch/1,
         cancel_batch/0,
         pause/0,
         resume/0,
         set_interval/1]).

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

-include("riak_moss.hrl").
-include("riak_cs_gc_d.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/0,
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
set_interval(Interval) ->
    gen_fsm:sync_send_event(?SERVER, {set_interval, Interval}, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init(Args) ->
    Interval = riak_cs_gc:gc_interval(),
    SchedState = schedule_next(#state{interval=Interval}),
    _ = check_bucket_props(Args),
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
    _ = lager:info("Finished garbage collection in ~b seconds.",
                   [elapsed(State#state.batch_start)]),
    riak_moss_riakc_pool_worker:stop(State#state.riak),
    NewState = schedule_next(State#state{riak=undefined}),
    {next_state, idle, NewState};
fetching_next_fileset(continue, State=#state{batch=[FileSetKey | RestKeys],
                                             batch_skips=BatchSkips,
                                             riak=RiakPid
                                            }) ->
    %% Fetch the next set of manifests for deletion
    case fetch_next_fileset(FileSetKey, RiakPid) of
        {ok, FileSet, RiakObj} ->
            NewStateData = State#state{current_files=twop_set:to_list(FileSet),
                                       current_fileset=FileSet,
                                       current_riak_object=RiakObj},
            NextState = initiating_file_delete;
        {error, _} ->
            NewStateData = State#state{batch=RestKeys,
                                       batch_skips=BatchSkips+1},
            NextState = fetching_next_fileset
    end,
    gen_fsm:send_event(?SERVER, continue),
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
    gen_fsm:send_event(?SERVER, continue),
    {next_state, fetching_next_fileset, State#state{batch=RestKeys,
                                                    batch_count=1+BatchCount}};
initiating_file_delete(continue, #state{current_files=[NextManifest | _RestManifests],
                                        riak=RiakPid}=State) ->
    %% Use an instance of `riak_cs_delete_fsm' to handle the
    %% deletion of the file blocks.
    %% Don't worry about delete_fsm failures. Manifests are
    %% rescheduled after a certain time.
    Args = [RiakPid, NextManifest, []],
    {ok, Pid} = riak_cs_delete_fsm_sup:start_delete_fsm(node(), Args),
    {next_state, waiting_file_delete, State#state{delete_fsm_pid = Pid}};
initiating_file_delete(_, State) ->
    {next_state, initiating_file_delete, State}.

waiting_file_delete(_, State) ->
    {next_state, waiting_file_delete, State}.

paused(_, State) ->
    {next_state, paused, State}.

%% Synchronous events

idle(status, _From, State) ->
    Props = [{interval, State#state.interval},
             {last, State#state.last},
             {next, State#state.next}],
    {reply, {ok, {idle, Props}}, idle, State};
idle({manual_batch, Options}, _From, State) ->
    case lists:member(testing, Options) of
        true ->
            NewState = State#state{batch=undefined};
        false ->
            NewState = start_batch(State)
    end,
    {reply, ok, fetching_next_fileset, NewState};
idle(cancel_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle(pause, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    UpdState = idle_pause_state(State),
    {reply, ok, paused, UpdState};
idle(resume, _From, State) ->
    {reply, {error, not_paused}, idle, State};
idle({set_interval, Interval}, _From, State) ->
    {reply, ok, idle, State#state{interval=Interval}};
idle(_, _From, State) ->
    {reply, ok, idle, State}.

fetching_next_fileset(status, _From, State) ->
    Reply = {ok, {fetching_next_fileset, status_data(State)}},
    {reply, Reply, fetching_next_fileset, State};
fetching_next_fileset({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, fetching_next_fileset, State};
fetching_next_fileset(pause, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    {reply, ok, paused, State#state{pause_state=fetching_next_fileset}};
fetching_next_fileset(resume, _From, State) ->
    {reply, {error, not_paused}, fetching_next_fileset, State};
fetching_next_fileset(cancel_batch, _From, State) ->
    cancel_batch(State);
fetching_next_fileset({set_interval, Interval}, _From, State) ->
    {reply, ok, fetching_next_fileset, State#state{interval=Interval}};
fetching_next_fileset(_, _From, State) ->
    {reply, ok, fetching_next_fileset, State}.

initiating_file_delete(status, _From, State) ->
    Reply = {ok, {initiating_file_delete, status_data(State)}},
    {reply, Reply, initiating_file_delete, State};
initiating_file_delete({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, initiating_file_delete, State};
initiating_file_delete(pause, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    {reply, ok, paused, State#state{pause_state=initiating_file_delete}};
initiating_file_delete(resume, _From, State) ->
    {reply, {error, not_paused}, initiating_file_delete, State};
initiating_file_delete(cancel_batch, _From, State) ->
    cancel_batch(State);
initiating_file_delete({set_interval, Interval}, _From, State) ->
    {reply, ok, initiating_file_delete, State#state{interval=Interval}};
initiating_file_delete(_, _From, State) ->
    {reply, ok, initiating_file_delete, State}.

waiting_file_delete({Pid, ok},
                    _From,
                    State=#state{delete_fsm_pid=Pid,
                                 current_files=[CurrentManifest | RestManifests],
                                 current_fileset=FileSet}) ->
    UpdFileSet = twop_set:del_element(CurrentManifest, FileSet),
    UpdState = State#state{current_fileset=UpdFileSet,
                           current_files=RestManifests},
    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, initiating_file_delete, UpdState};
waiting_file_delete({Pid, {error, _Reason}},
                    _From,
                    State=#state{delete_fsm_pid=Pid,
                                 current_files=[_ | RestManifests]}) ->
    UpdState = State#state{current_files=RestManifests},
    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, initiating_file_delete, UpdState};
waiting_file_delete(status, _From, State) ->
    Reply = {ok, {waiting_file_delete, status_data(State)}},
    {reply, Reply, waiting_file_delete, State};
waiting_file_delete({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, waiting_file_delete, State};
waiting_file_delete(pause, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    {reply, ok, paused, State#state{pause_state=waiting_file_delete}};
waiting_file_delete(resume, _From, State) ->
    {reply, {error, not_paused}, waiting_file_delete, State};
waiting_file_delete(cancel_batch, _From, State) ->
    cancel_batch(State);
waiting_file_delete({set_interval, Interval}, _From, State) ->
    {reply, ok, waiting_file_delete, State#state{interval=Interval}};
waiting_file_delete(_, _From, State) ->
    {reply, ok, waiting_file_delete, State}.

paused({Pid, ok}, _From, State=#state{delete_fsm_pid=Pid,
                                      pause_state=waiting_file_delete,
                                      current_files=[CurrentManifest | RestManifests],
                                      current_fileset=FileSet}) ->
    UpdFileSet = twop_set:del_element(CurrentManifest, FileSet),
    UpdState = State#state{delete_fsm_pid=undefined,
                           pause_state=initiating_file_delete,
                           current_fileset=UpdFileSet,
                           current_files=RestManifests},
    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, paused, UpdState};
paused({Pid, {error, _Reason}}, _From, State=#state{delete_fsm_pid=Pid,
                                                    pause_state=waiting_file_delete,
                                                    current_files=[_ | RestManifests]}) ->
    UpdState = State#state{delete_fsm_pid=undefined,
                           pause_state=initiating_file_delete,
                           current_files=RestManifests},
    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, paused, UpdState};
paused(status, _From, State) ->
    Reply = {ok, {paused, status_data(State)}},
    {reply, Reply, paused, State};
paused(pause, _From, State) ->
    {reply, {error, already_paused}, paused, State};
paused(resume, _From, State=#state{pause_state=PauseState}) ->
    _ = lager:info("Resuming garbage collection"),
    gen_fsm:send_event(?SERVER, continue),
    %% @TODO Differences in the fsm state and the state record
    %% get confusing. Maybe s/State/StateData.
    {reply, ok, PauseState, State};
paused(cancel_batch, _From, State) ->
    cancel_batch(State);
paused({set_interval, Interval}, _From, State) ->
    {reply, ok, paused, State#state{interval=Interval}};
paused(_, _From, State) ->
    {reply, ok, paused, State}.

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
    {reply, ok, NewStateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

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
-spec cancel_batch(#state{}) -> {reply, ok, idle, #state{}}.
cancel_batch(#state{batch_start=BatchStart,
                    riak=RiakPid}=State) ->
    %% Interrupt the batch of deletes
    _ = lager:info("Canceled garbage collection batch after ~b seconds.",
                   [elapsed(BatchStart)]),
    riak_moss_riakc_pool_worker:stop(RiakPid),
    NewState = schedule_next(State#state{batch=[],
                                         riak=undefined}),
    {reply, ok, idle, NewState}.


-spec check_bucket_props([term()]) -> ok | {error, term()}.
check_bucket_props([testing]) ->
    ok;
check_bucket_props([]) ->
    %% @TODO Handle this in more general way. Maybe break out the
    %% function from the rts module?
    rts:check_bucket_props(?GC_BUCKET).

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

eligible_manifest_keys({{ok, BKeys}, _}) ->
    [Key || [_, Key] <- BKeys];
eligible_manifest_keys({{error, Reason}, EndTime}) ->
    _ = lager:warning("Error occurred trying to query from time 0 to ~p"
                      "in gc key index. Reason: ~p",
                      [EndTime, Reason]),
    [].

gc_index_query(RiakPid, EndTime) ->
    QueryResult = riakc_pb_socket:get_index(RiakPid,
                                            ?GC_BUCKET,
                                            ?KEY_INDEX,
                                            ?EPOCH_START,
                                            EndTime),
    {QueryResult, EndTime}.

%% @doc Delete the blocks for the next set of manifests in the batch
-spec fetch_next_fileset(binary(), pid()) ->
                                {ok, twop_set:twop_set(), riakc_obj:riakc_obj()} |
                                {error, term()}.
fetch_next_fileset(ManifestSetKey, RiakPid) ->
    %% Get the set of manifests represented by the key
    case riak_moss_utils:get_object(?GC_BUCKET, ManifestSetKey, RiakPid) of
        {ok, RiakObj} ->
            %% Get any values from the riak object and resolve them
            %% into a single set.
            BinValues = riakc_obj:get_values(RiakObj),
            Values = [binary_to_term(BinValue) || BinValue <- BinValues],
            ManifestSet = twop_set:resolve(Values),
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
finish_file_delete(_, FileSet, RiakObj, RiakPid) ->
    _ = lager:debug("Remaining file keys: ~p", [twop_set:to_list(FileSet)]),
    UpdRiakObj = riakc_obj:update_value(RiakObj, FileSet),
    %% Not a big deal if the put fails. Worst case is attempts
    %% to delete some of the file set members are already deleted.
    _ = riak_moss_utils:put_with_no_meta(RiakPid, UpdRiakObj),
    ok.

%% @doc Update the state record for a transition from `idle' to
%% `paused'.
-spec idle_pause_state(#state{}) -> #state{}.
idle_pause_state(State=#state{interval=infinity,
                              timer_ref=TimerRef}) ->
    %% Cancel the timer in case the interval has
    %% recently be set to `infinity'.
    erlang:cancel_timer(TimerRef),
    State#state{pause_state=idle,
                interval_remaining=undefined};
idle_pause_state(State=#state{timer_ref=TimerRef}) ->
    case erlang:cancel_timer(TimerRef) of
        false ->
            Remainder = 0;
        Remainder ->
            ok
    end,
    State#state{pause_state=idle,
                interval_remaining=Remainder}.

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
    {ok, Riak} = riak_moss_riakc_pool_worker:start_link([]),
    BatchStart = riak_cs_gc:timestamp(),
    Batch = fetch_eligible_manifest_keys(Riak, BatchStart),
    _ = lager:debug("Batch keys: ~p", [Batch]),
    gen_fsm:send_event(?SERVER, continue),
    State#state{batch_start=BatchStart,
                batch=Batch,
                batch_count=0,
                batch_skips=0,
                riak=Riak}.

%% @doc Extract a list of status information from a state record.
-spec status_data(#state{}) -> [{atom(), term()}].
status_data(State) ->
    [{interval, State#state.interval},
     {last, State#state.last},
     {current, State#state.batch_start},
     {next, State#state.next},
     {elapsed, elapsed(State#state.batch_start)},
     {files_deleted, State#state.batch_count},
     {files_skipped, State#state.batch_skips},
     {files_left, length(State#state.batch)}].

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start the garbage collection server
test_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [testing], []).

%% @doc Get the current state of the fsm for testing inspection
-spec current_state() -> {atom(), #state{}} | {error, term()}.
current_state() ->
    gen_fsm:sync_send_all_state_event(?SERVER, current_state).

%% @doc Manipulate the current state of the fsm for testing
change_state(State) ->
    gen_fsm:sync_send_all_state_event(?SERVER, {change_state, State}).

-endif.
