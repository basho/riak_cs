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
         resume_batch/0]).

%% gen_fsm callbacks
-export([init/1,
         idle/2, idle/3,
         deleting/2, deleting/3,
         paused/2, paused/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_moss.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          schedule,      %% the times that storage is calculated
          last,          %% the last time a deletion was scheduled
          current,       %% what schedule we're calculating for now
          next,          %% the next scheduled time

          riak,          %% client we're currently using
          batch_start,   %% the time we actually started
          batch_count=0, %% count of objects processed so far
          batch_skips=0, %% count of objects skipped so far
          batch=[]      %% objects left to process in this batch
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starting the server also verifies the storage schedule.  If
%% the schedule contains invalid elements, an error will be printed in
%% the logs.
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Status is returned as a 2-tuple of `{State, Details}'.  State
%% should be `idle', `calculating', or `paused'.  When `idle' the
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


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.
init([]) ->
    Schedule = read_gc_schedule(),
    SchedState = schedule_next(#state{schedule=Schedule},
                               calendar:universal_time()),
    ok = rts:check_bucket_props(?STORAGE_BUCKET),
    {ok, idle, SchedState}.

%% Asynchronous events

%% @doc Transitions out of idle are all synchronous events
idle(_, State) ->
    {next_state, idle, State}.

%% @doc Async transitions from deleting are all due to messages the
%% FSM sends itself, in order to have opportunities to handle messages
%% from the outside world (like `status').
deleting(continue, #state{batch=[], current=Current}=State) ->
    %% finished with this batch
    _ = lager:info("Finished storage calculation in ~b seconds.",
                   [elapsed(State#state.batch_start)]),
    riak_moss_riakc_pool_worker:stop(State#state.riak),
    NewState = State#state{riak=undefined,
                           last=Current,
                           current=undefined},
    {next_state, idle, NewState};
deleting(continue, State) ->
    %% more to do yet
    %% @TODO Continue file deletion
    %% NewState = delete_next_file(State),
    NewState = State,
    gen_fsm:send_event(?SERVER, continue),
    {next_state, calculating, NewState};
deleting(_, State) ->
    {next_state, calculating, State}.

paused(_, State) ->
    {next_state, paused, State}.

%% Synchronous events

idle(status, _From, State) ->
    Props = [{schedule, State#state.schedule},
             {last, State#state.last},
             {next, State#state.next}],
    {reply, {ok, {idle, Props}}, idle, State};
idle({manual_batch, Options}, _From, State) ->
    NewState = start_batch(Options, calendar:universal_time(), State),
    {reply, ok, calculating, NewState};
idle(cancel_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle(pause_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle(resume_batch, _From, State) ->
    {reply, {error, no_batch}, idle, State};
idle(_, _From, State) ->
    {reply, ok, idle, State}.

deleting(status, _From, State) ->
    Props = [{schedule, State#state.schedule},
             {last, State#state.last},
             {current, State#state.current},
             {next, State#state.next},
             {elapsed, elapsed(State#state.batch_start)},
             {users_done, State#state.batch_count},
             {users_skipped, State#state.batch_skips},
             {users_left, length(State#state.batch)}],
    {reply, {ok, {calculating, Props}}, calculating, State};
deleting({manual_batch, _Options}, _From, State) ->
    %% this is the manual user request to begin a batch
    {reply, {error, already_deleting}, deleting, State};
deleting(pause_batch, _From, State) ->
    _ = lager:info("Pausing file block deletion"),
    {reply, ok, paused, State};
deleting(cancel_batch, _From, #state{current=Current}=State) ->
    %% finished with this batch
    _ = lager:info("Canceled deletion after ~b seconds.",
                   [elapsed(State#state.batch_start)]),
    riak_moss_riakc_pool_worker:stop(State#state.riak),
    NewState = State#state{riak=undefined,
                           last=Current,
                           current=undefined,
                           batch=[]},
    {reply, ok, idle, NewState};
deleting(_, _From, State) ->
    {reply, ok, deleting, State}.

paused(status, From, State) ->
    {reply, {ok, {_, Status}}, _, State} = deleting(status, From, State),
    {reply, {ok, {paused, Status}}, paused, State};
paused(resume_batch, _From, State) ->
    _ = lager:info("Resuming storage calculation"),
    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, calculating, State};
paused(cancel_batch, From, State) ->
    deleting(cancel_batch, From, State);
paused(_, _From, State) ->
    {reply, ok, paused, State}.

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc there are no all-state events for this fsm
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info({start_batch, Next}, idle, #state{next=Next}=State) ->
    %% next is scheduled immediately in order to generate warnings if
    %% the current calculation runs over time (see next clause)
    NewState = schedule_next(start_batch([], Next, State), Next),
    {next_state, calculating, NewState};
handle_info({start_batch, Next}, InBatch,
            #state{next=Next, current=Current}=State) ->
    _ = lager:error("Unable to start storage calculation for ~p"
                    " because ~p is still working. Skipping forward...",
                    [Next, Current]),
    NewState = schedule_next(State, Next),
    {next_state, InBatch, NewState};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc TODO: log warnings if this fsm is asked to terminate in the
%% middle of running a calculation
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc The schedule will contain all valid times found in the
%% configuration, and will be sorted in day order.
read_gc_schedule() ->
    lists:usort(read_gc_schedule1()).

read_gc_schedule1() ->
    case application:get_env(riak_moss, gc_schedule) of
        undefined ->
            _ = lager:warning("No storage schedule defined."
                              " Calculation must be triggered manually."),
            [];
        {ok, Sched} ->
            case catch parse_time(Sched) of
                {ok, Time} ->
                    %% user provided just one time
                    [Time];
                {'EXIT',_} when is_list(Sched) ->
                    Times = [ {S, catch parse_time(S)} || S <- Sched ],
                    _ = case [ X || {X,{'EXIT',_}} <- Times ] of
                            [] -> ok;
                            Bad ->
                                _ = lager:error(
                                      "Ignoring bad storage schedule elements ~p",
                                      [Bad])
                        end,
                    case [ Parsed || {_, {ok, Parsed}} <- Times] of
                        [] ->
                            _ = lager:warning(
                                  "No storage schedule defined."
                                  " Calculation must be triggered manually."),
                            [];
                        Good ->
                            Good
                    end;
                _ ->
                    _ = lager:error(
                          "Invalid storage schedule defined."
                          " Calculation must be triggered manually."),
                    []
            end
    end.

%% @doc Time is allowed as a `{Hour, Minute}' tuple, or as an `"HHMM"'
%% string.  This function purposely fails (with function or case
%% clause currently) to allow {@link read_gc_schedule1/0} to pick
%% out the bad eggs.
parse_time({Hour, Min}) when (Hour >= 0 andalso Hour =< 23),
                             (Min >= 0 andalso Min =< 59) ->
    {ok, {Hour, Min}};
parse_time(HHMM) when is_list(HHMM) ->
    case io_lib:fread("~2d~2d", HHMM) of
        {ok, [Hour, Min], []} ->
            %% make sure numeric bounds apply
            parse_time({Hour, Min})
    end.

%% @doc Actually kick off the batch.  After calling this function, you
%% must advance the FSM state to `calculating'.
start_batch(_Options, Time, State) ->
    BatchStart = calendar:universal_time(),
    %% TODO: probably want to do this fetch streaming, to avoid
    %% accidental memory pressure at other points

    %% this does not check out a worker from the riak connection pool;
    %% instead it creates a fresh new worker, the idea being that we
    %% don't want to foul up the storage calculation just because the
    %% pool is empty; pool workers just happen to be literally the
    %% socket process, so "starting" one here is the same as opening a
    %% connection, and avoids duplicating the configuration lookup code
    {ok, Riak} = riak_moss_riakc_pool_worker:start_link([]),
    %% @TODO Write function to fetch file list
    %% Batch = fetch_file_list(Riak),
    Batch = [],

    gen_fsm:send_event(?SERVER, continue),
    State#state{batch_start=BatchStart,
                current=Time,
                riak=Riak,
                batch=Batch,
                batch_count=0,
                batch_skips=0}.

%% @doc How many seconds have passed from `Time' to now.
elapsed(Time) ->
    elapsed(Time, calendar:universal_time()).

%% @doc How many seconds are between `Early' and `Late'.  Warning:
%% this will be negative if `Early' is later than `Late'.
elapsed(Early, Late) ->
    calendar:datetime_to_gregorian_seconds(Late)
        -calendar:datetime_to_gregorian_seconds(Early).

%% @doc Setup the automatic trigger to start the next scheduled batch
%% calculation.  "Next" is defined as the scheduled time occurring
%% soonest after the `Last' parameter, that has not also already
%% passed by the wall clock.  If the next scheduled time <em>has</em>
%% already passed, an error is printed to the logs, and the next time
%% that has not already passed is found and scheduled instead.
schedule_next(#state{schedule=[]}=State, _) ->
    %% nothing to schedule, all triggers manual
    State;
schedule_next(#state{schedule=Schedule}=State, Last) ->
    NextTime = next_target_time(Last, Schedule),
    case elapsed(calendar:universal_time(), NextTime) of
        D when D > 0 ->
            _ = lager:info("Scheduling next storage calculation for ~p",
                           [NextTime]),
            erlang:send_after(D*1000, self(), {start_batch, NextTime}),
            State#state{next=NextTime};
        _ ->
            _ = lager:error("Missed start time for storage calculation at ~p,"
                            " skipping to next scheduled time...",
                            [NextTime]),
            %% just skip everything until the next scheduled time from now
            schedule_next(State, calendar:universal_time())
    end.

%% @doc Find the next scheduled time after the given time.
next_target_time({Day, {LH, LM,_}}, Schedule) ->
    RemainingInDay = lists:dropwhile(
                       fun(Sched) -> Sched =< {LH, LM} end, Schedule),
    case RemainingInDay of
        [] ->
            [{NH, NM}|_] = Schedule,
            {next_day(Day), {NH, NM, 0}};
        [{NH, NM}|_] ->
            {Day, {NH, NM, 0}}
    end.

next_day(Day) ->
    {DayP,_} = calendar:gregorian_seconds_to_datetime(
                 86400+calendar:datetime_to_gregorian_seconds(
                         {Day, {0,0,1}})),
    DayP.
