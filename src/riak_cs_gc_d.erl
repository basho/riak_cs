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
         start_batch/1,
         cancel_batch/0,
         pause_batch/0,
         resume_batch/0,
         set_interval/1]).

%% gen_fsm callbacks
-export([init/1,
         idle/2, idle/3,
         fetching_next_fileset/2,
         fetching_next_fileset/3,
         initiating_file_delete/2,
         initiating_file_delete/3,
         waiting_file_delete/3,
         paused/2,
         paused/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_moss.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([current_state/0,
         status_data/1]).

-endif.

-define(SERVER, ?MODULE).

-record(state, {
          interval :: non_neg_integer(),
          last :: undefined | calendar:datetime(), % the last time a deletion was scheduled
          next :: undefined | calendar:datetime(), % the next scheduled gc time
          riak :: pid(), % Riak connection pid
          current_fileset :: [lfs_manifest()],
          batch_start :: undefined | calendar:datetime(), % start of the current gc interval
          batch_count=0 :: non_neg_integer(),
          batch_skips=0 :: non_neg_integer(),
          batch=[] :: [twop_set:twop_set()],
          pause_state :: atom(), % state of the fsm when a delete batch was paused
          delete_fsm_pid :: pid()
         }).

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
    gen_fsm:sync_send_event(?SERVER, status).

%% @doc Force a calculation and archival manually.  The `current'
%% property returned from a {@link status/0} call will show the most
%% recently passed schedule time, but calculations will be stored with
%% the time at which they happen, as expected.
%%
%% Allowed options are:
%% <dl>
%%   <dt>`recalc'</dt>
%%   <dd>Recalculate the storage for each user, even if that user
%%   already has a calculation stored for this time period. Default is
%%   `false', such that restarting a canceled batch does not require
%%   redoing the work that happened before cancellation.</dd>
%% </dl>
start_batch(Options) ->
    gen_fsm:sync_send_event(?SERVER, {manual_batch, Options}, infinity).

%% @doc Cancel the calculation currently in progress.  Returns `ok' if
%% a batch was canceled, or `{error, no_batch}' if there was no batch
%% in progress.
cancel_batch() ->
    gen_fsm:sync_send_event(?SERVER, cancel_batch, infinity).

%% @doc Pause the calculation currently in progress.  Returns `ok' if
%% a batch was paused, or `{error, no_batch}' if there was no batch in
%% progress.  Also returns `ok' if there was a batch in progress that
%% was already paused.
pause_batch() ->
    gen_fsm:sync_send_event(?SERVER, pause_batch, infinity).

%% @doc Resume the batch currently in progress.  Returns `ok' if a
%% batch was resumed, or `{error, no_batch}' if there was no batch in
%% progress.  Also returns `ok' if there was a batch in progress that
%% was not paused.
resume_batch() ->
    gen_fsm:sync_send_event(?SERVER, resume_batch, infinity).

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
init([]) ->
    Interval = riak_cs_gc:gc_interval(),
    SchedState = schedule_next(#state{interval=Interval}),
    %% @TODO Handle this in more general way. Maybe break out the
    %% function from the rts module?
    %% ok = rts:check_bucket_props(?GC_BUCKET),
    {ok, idle, SchedState}.

%% Asynchronous events

%% @doc Transitions out of idle are all synchronous events
idle(_, State) ->
    {next_state, idle, State}.

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
        {ok, FileSet} ->
            NewStateData = State#state{current_fileset=FileSet},
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
initiating_file_delete(continue, #state{batch=[ManiSetKey | RestKeys],
                                        batch_count=BatchCount,
                                        current_fileset=[],
                                        riak=RiakPid}=State) ->
    %% Delete the key from the GC bucket
    riakc_pb_socket:delete(RiakPid, ?GC_BUCKET, ManiSetKey),
    gen_fsm:send_event(?SERVER, continue),
    {next_state, fetching_next_fileset, State#state{batch=RestKeys,
                                                    batch_count=1+BatchCount}};
initiating_file_delete(continue, #state{current_fileset=[NextManifest | RestManifests],
                                        riak=RiakPid}=State) ->
    %% @TODO Don't worry about delete_fsm failures, will handle retry
    %% in by allowing manifests to be rescheduled after a certain
    %% time.

    %% Use an instance of `riak_cs_delete_fsm' to handle the
    %% deletion of the file blocks.
    {Bucket, Key} = NextManifest?MANIFEST.bkey,
    UUID = NextManifest?MANIFEST.uuid,
    Args = [Bucket, Key, UUID, RiakPid, []],
    {ok, Pid} = riak_cs_delete_fsm_sup:start_delete_fsm(node(), Args),
    {next_state, waiting_file_delete, State#state{current_fileset=RestManifests,
                                                  delete_fsm_pid = Pid}};
initiating_file_delete(_, State) ->
    {next_state, initiating_file_delete, State}.

paused(_, State) ->
    {next_state, paused, State}.

%% Synchronous events

idle(status, _From, State) ->
    Props = [{interval, State#state.interval},
             {last, State#state.last},
             {next, State#state.next}],
    {reply, {ok, {idle, Props}}, idle, State};
idle({manual_batch, Options}, _From, State) ->
    NewState = start_batch(Options, State),
    {reply, ok, fetching_next_fileset, NewState};
idle(cancel_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle(pause_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle(resume_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle({set_interval, Interval}, _From, State) ->
    {reply, {error, no_batch}, idle, State#state{interval=Interval}};
idle(_, _From, State) ->
    {reply, ok, idle, State}.

fetching_next_fileset(status, _From, State) ->
    Reply = {ok, {fetching_next_fileset, status_data(State)}},
    {reply, Reply, fetching_next_fileset, State};
fetching_next_fileset({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, fetching_next_fileset, State};
fetching_next_fileset(pause_batch, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    {reply, ok, paused, State#state{pause_state=fetching_next_fileset}};
fetching_next_fileset(cancel_batch, _From, State) ->
    cancel_batch(State);
fetching_next_fileset({set_interval, Interval}, _From, State) ->
    {reply, {error, no_batch}, fetching_next_fileset, State#state{interval=Interval}};
fetching_next_fileset(_, _From, State) ->
    {reply, ok, fetching_next_fileset, State}.

initiating_file_delete(status, _From, State) ->
    Reply = {ok, {initiating_file_delete, status_data(State)}},
    {reply, Reply, initiating_file_delete, State};
initiating_file_delete({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, initiating_file_delete, State};
initiating_file_delete(pause_batch, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    {reply, ok, paused, State#state{pause_state=initiating_file_delete}};
initiating_file_delete(cancel_batch, _From, State) ->
    cancel_batch(State);
initiating_file_delete({set_interval, Interval}, _From, State) ->
    {reply, {error, no_batch}, initiating_file_delete, State#state{interval=Interval}};
initiating_file_delete(_, _From, State) ->
    {reply, ok, initiating_file_delete, State}.

waiting_file_delete({Pid, done}, _From, State=#state{delete_fsm_pid=Pid}) ->
    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, initiating_file_delete, State};
waiting_file_delete(status, _From, State) ->
    Reply = {ok, {waiting_file_delete, status_data(State)}},
    {reply, Reply, waiting_file_delete, State};
waiting_file_delete({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, waiting_file_delete, State};
waiting_file_delete(pause_batch, _From, State) ->
    _ = lager:info("Pausing garbage collection"),
    {reply, ok, paused, State#state{pause_state=waiting_file_delete}};
waiting_file_delete(cancel_batch, _From, State) ->
    cancel_batch(State);
waiting_file_delete({set_interval, Interval}, _From, State) ->
    {reply, {error, no_batch}, waiting_file_delete, State#state{interval=Interval}};
waiting_file_delete(_, _From, State) ->
    {reply, ok, waiting_file_delete, State}.

paused(status, _From, State) ->
    Reply = {ok, {paused, status_data(State)}},
    {reply, Reply, paused, State};
paused(resume_batch, _From, State=#state{pause_state=PauseState}) ->
    _ = lager:info("Resuming garbage collection"),
    gen_fsm:send_event(?SERVER, continue),
    %% @TODO Differences in the fsm state and the state record
    %% get confusing. Maybe s/State/StateData.
    {reply, ok, PauseState, State};
paused(cancel_batch, From, State) ->
    fetching_next_fileset(cancel_batch, From, State);
paused({set_interval, Interval}, _From, State) ->
    {reply, {error, no_batch}, paused, State#state{interval=Interval}};
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
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(start_batch, idle, State) ->
    NewState = start_batch([], State),
    {next_state, fetching_next_fileset, NewState};
handle_info(start_batch, InBatch, State) ->
    _ = lager:error("Unable to start garbage collection batch"
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

%% @doc How many seconds have passed from `Time' to now.
-spec elapsed(non_neg_integer()) -> integer().
elapsed(Time) ->
    riak_cs_gc:timestamp() - Time.

%% @doc Fetch the list of keys for file manifests that are eligible
%% for delete.
-spec fetch_eligible_manifest_keys(pid(), non_neg_integer()) -> [binary()].
fetch_eligible_manifest_keys(RiakPid, IntervalStart) ->
    EndTime = list_to_binary(integer_to_list(IntervalStart)),
    case riakc_pb_socket:get_index(RiakPid,
                                   ?GC_BUCKET,
                                   ?KEY_INDEX,
                                   ?EPOCH_START,
                                   EndTime) of
        {ok, BKeys} ->
            [Key || [_, Key] <- BKeys];
        {error, Reason} ->
            _ = lager:warning("Error occurred trying to query from time 0 to ~p"
                              "in gc key index. Reason: ~p",
                              [EndTime, Reason]),
            []
    end.

%% @doc Delete the blocks for the next set of manifests in the batch
-spec fetch_next_fileset(non_neg_integer(), pid()) -> {ok, [lfs_manifest()]} | {error, term()}.
fetch_next_fileset(ManifestSetKey, RiakPid) ->
    %% Get the set of manifests represented by the key
    case riak_moss_utils:get_object(?GC_BUCKET, ManifestSetKey, RiakPid) of
        {ok, RiakObj} ->
            %% Get any values from the riak object and resolve them
            %% into a single set.
            BinValues = riakc_obj:get_values(RiakObj),
            Values = [binary_to_term(BinValue) || BinValue <- BinValues],
            ManifestSet = twop_set:resolve(Values),
            {ok, twop_set:to_list(ManifestSet)};
        {error, notfound}=Error ->
            Error;
        {error, Reason}=Error ->
            _ = lager:warning("Error occurred trying to read the fileset"
                              "for ~p for gc. Reason: ~p",
                              [ManifestSetKey, Reason]),
            Error
    end.

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
    erlang:send_after(Interval*1000, self(), start_batch),
    State#state{batch_start=undefined,
                last=Current,
                next=Next}.

%% @doc Actually kick off the batch.  After calling this function, you
%% must advance the FSM state to `fetching_next_fileset'.
start_batch(_Options, State) ->
    BatchStart = riak_cs_gc:timestamp(),

    %% this does not check out a worker from the riak connection pool;
    %% instead it creates a fresh new worker, the idea being that we
    %% don't want to delay deletion just because the normal request
    %% pool is empty; pool workers just happen to be literally the
    %% socket process, so "starting" one here is the same as opening a
    %% connection, and avoids duplicating the configuration lookup code
    {ok, Riak} = riak_moss_riakc_pool_worker:start_link([]),
    Batch = fetch_eligible_manifest_keys(Riak, BatchStart),
    _ = lager:debug("Batch keys: ~p", [Batch]),
    gen_fsm:send_event(?SERVER, continue),
    State#state{batch_start=BatchStart,
                riak=Riak,
                batch=Batch,
                batch_count=0,
                batch_skips=0}.

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

%% @doc Get the current state of the fsm for testing inspection
-spec current_state() -> {atom(), #state{}} | {error, term()}.
current_state() ->
    gen_fsm:sync_send_all_state_event(?SERVER, current_state).

-endif.
