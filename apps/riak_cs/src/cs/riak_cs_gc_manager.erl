%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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
%%
%% State Diagram
%%
%% start -> idle -(start)-> running
%%            ^               |
%%            +---(finish)----+
%%            +---(cancel)----+
%%
%% Message exchange chart (not a sequence, but just a list)
%%
%%  Message\  sdr/rcver  gc_manager      gc_batch
%%   start_link)                   ------>
%%   current_state)                -call->
%%   stop)                         -call->
%%   finished)                     <-cast-
%%   trapped EXIT)                 <------
%%
-module(riak_cs_gc_manager).

-behaviour(gen_fsm).

%% Console API
-export([start_batch/1,
         cancel_batch/0,
         set_interval/1,
         status/0,
         pp_status/0]).

%% FSM API
-export([start_link/0, finished/1]).

-ifdef(TEST).
-export([test_link/0]).
-endif.

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-export([idle/2, idle/3,
         running/2, running/3]).

-type statename() :: idle | running | finishing.
-type manager_statename() :: idle | running.
-type batch_statename() :: not_running | waiting_for_workers.
-export_type([statename/0]).

-include("riak_cs_gc.hrl").
-include_lib("kernel/include/logger.hrl").

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start_batch(Options) ->
    gen_fsm:sync_send_event(?SERVER, {start, Options}, infinity).

cancel_batch() ->
    gen_fsm:sync_send_event(?SERVER, cancel, infinity).

-spec status() -> {ok, {manager_statename(), batch_statename(), #gc_manager_state{}}}.
status() ->
    gen_fsm:sync_send_all_state_event(?SERVER, status, infinity).

-spec pp_status() -> {ok, {statename(), proplists:proplist()}}.
pp_status() ->
    {ok, {ManagerStateName, BatchStateName, State}} = status(),
    D = lists:zip(record_info(fields, gc_manager_state),
                  tl(tuple_to_list(State))),
    Details = lists:flatten(lists:map(fun({Type, Value}) ->
                                              translate(Type, Value)
                                      end, D)),
    StateName = case {ManagerStateName, BatchStateName} of
                    {idle, _} -> idle;
                    {running, not_running} ->
                        %% riak_cs_gc_batch process has terminated,
                        %% either successfully or abnormally, who knows?
                        finishing;
                    {running, _} -> running
                end,
    {ok, {StateName,
          [{leeway, riak_cs_gc:leeway_seconds()}] ++ Details}}.

%% @doc Adjust the interval at which the manager attempts to perform
%% a garbage collection sweep. Setting the interval to a value of
%% `infinity' effectively disable garbage collection. The manager still
%% runs, but does not carry out any file deletion.
-spec set_interval(term()) -> ok | {error, term()}.
set_interval(Interval) when is_integer(Interval)
                            orelse Interval =:= infinity ->
    gen_fsm:sync_send_all_state_event(?SERVER, {set_interval, Interval}, infinity).

start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

-ifdef(TEST).
test_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [testing], []).

-endif.


finished(Report) ->
    gen_fsm:send_all_state_event(?SERVER, {finished, Report}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-ifndef(TEST).

init([]) ->
    process_flag(trap_exit, true),
    InitialDelay = riak_cs_gc:initial_gc_delay(),
    State = case riak_cs_gc:gc_interval() of
                infinity ->
                    #gc_manager_state{};
                Interval when is_integer(Interval) ->
                    Interval2 = Interval + InitialDelay,
                    Next = riak_cs_gc:timestamp() + Interval2,
                    TimerRef = erlang:send_after(Interval2 * 1000, self(), {start, []}),
                    logger:info("Scheduled next batch at ~s",
                                [riak_cs_gc_console:human_time(Next)]),

                    #gc_manager_state{next=Next,
                                      interval=Interval,
                                      initial_delay=InitialDelay,
                                      timer_ref=TimerRef}
        end,
    {ok, idle, State}.

-else.

init([testing]) ->
    {ok, idle, #gc_manager_state{}}.

-endif.

%%--------------------------------------------------------------------
%% @private
%% @doc All gen_fsm:send_event/2 call should be ignored
idle(_Event, State) ->
    {next_state, idle, State}.
running(_Event, State) ->
    {next_state, running, State}.

%% @private
%% @doc
idle({start, Options}, _From, State) ->
    case start_batch(State, Options) of
        {ok, NextState} ->
            {reply, ok, running, NextState};
        Error ->
            {reply, Error, idle, State}
    end;
idle(_, _From, State) ->
    {reply, {error, idle}, idle, State}.

running(cancel, _From, State = #gc_manager_state{gc_batch_pid=Pid}) ->
    %% stop gc_batch here
    catch riak_cs_gc_batch:stop(Pid),
    NextState = schedule_next(State),
    {reply, ok, idle, NextState};
running(_Event, _From, State) ->
    Reply = {error, running},
    {reply, Reply, running, State}.

%% @private
%% @doc async notification from gc_batch - if this is synchronous
%% call, deadlock may happen with synchronous message like
%% `current_state' from manager to gc_batch.
handle_event({finished, Report}, _StateName, State) ->
    %% Add report to history
    NextState=schedule_next(State),
    {next_state, idle,
     NextState#gc_manager_state{batch_history=[Report]}};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc
handle_sync_event({set_interval, Initerval}, _From, StateName, State) ->
    NewState0 = maybe_cancel_timer(State#gc_manager_state{interval=Initerval}),
    NewState = schedule_next(NewState0),
    {reply, ok, StateName, NewState};
handle_sync_event(status, _From, StateName, #gc_manager_state{gc_batch_pid=Pid} = State) ->
    {BatchStateName, BatchState} = maybe_current_state(Pid),
    NewState = State#gc_manager_state{current_batch=BatchState},
    {reply, {ok, {StateName, BatchStateName, NewState}}, StateName, NewState};
handle_sync_event(stop, _, _, State) ->
    %% for tests
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = {error, illegal_sync_send_all_state_call},
    {reply, Reply, StateName, State}.

%% @private
%% @doc Because process flag of trap_exit is true, gc_batch
%% failure will be delivered as a message.
handle_info({'EXIT', Pid, Reason}, _StateName,
            #gc_manager_state{gc_batch_pid=Pid} = State) ->
    case Reason of
        Reason when Reason =/= normal andalso Reason =/= cancel ->
            logger:warning("GC batch has terminated for reason: ~p", [Reason]);
        _ ->
            ok
    end,
    NextState = schedule_next(State#gc_manager_state{gc_batch_pid=undefined}),
    {next_state, idle, NextState};
handle_info({start, Options}, idle, State) ->
    case start_batch(State, Options) of
        {ok, NextState} ->
            {next_state, running, NextState};
        Error ->
            logger:error("Cannot start batch. Reason: ~p", [Error]),
            NextState = schedule_next(State),
            {next_state, idle, NextState}
    end;
handle_info({start, _}, running, State) ->
    %% The batch has been already started, but no need to write warning log
    {next_state, running, State};
handle_info(Info, StateName, State) ->
    %% This is really unexpected and unknown - warning.
    logger:warning("Unexpected message received at GC process (~p): ~p",
                   [StateName, Info]),
    {next_state, StateName, State}.

%% @private
%% @doc
terminate(_Reason, _StateName, _State = #gc_manager_state{gc_batch_pid=Pid}) ->
    catch riak_cs_gc_batch:stop(Pid),
    ok.

%% @private
%% @doc Not used
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_batch(#gc_manager_state{}, proplists:proplist()) ->
                         {ok, #gc_manager_state{}} |
                         {error, term()}.
start_batch(State, Options) ->
    %% All these Items should be non_neg_integer()
    MaxWorkers =  proplists:get_value('max-workers', Options,
                                     riak_cs_gc:gc_max_workers()),
    BatchStart = riak_cs_gc:timestamp(),
    Leeway = proplists:get_value(leeway, Options,
                                 riak_cs_gc:leeway_seconds()),
    StartKey = proplists:get_value(start, Options,riak_cs_gc:epoch_start()),
    DefaultEndKey = riak_cs_gc:default_batch_end(BatchStart, Leeway),
    EndKey = proplists:get_value('end', Options, DefaultEndKey),
    BatchSize = proplists:get_value(batch_size, Options,
                                    riak_cs_config:gc_batch_size()),

    %% set many items to GC batch state here
    BatchState = #gc_batch_state{
                    batch_start=BatchStart,
                    start_key=StartKey,
                    end_key=EndKey,
                    leeway=Leeway,
                    max_workers=MaxWorkers,
                    batch_size=BatchSize},

    case riak_cs_gc_batch:start_link(BatchState) of
        {ok, Pid} ->
            {ok, State#gc_manager_state{gc_batch_pid=Pid,
                                        current_batch=BatchState}};
        Error ->
            Error
    end.

-ifdef(TEST).
maybe_current_state(undefined) -> {not_running, undefined};
maybe_current_state(mock_pid) -> {not_running, undefined}.
-else.
maybe_current_state(undefined) -> {not_running, undefined};
maybe_current_state(Pid) when is_pid(Pid) ->
    try
        riak_cs_gc_batch:current_state(Pid)
    catch
        exit:{noproc, _} ->
            {not_running, undefined};
        exit:{normal, _} ->
            {not_running, undefined}
    end.
-endif.

maybe_cancel_timer(#gc_manager_state{timer_ref=Ref}=State)
  when is_reference(Ref) ->
    _ = erlang:cancel_timer(Ref),
    State#gc_manager_state{timer_ref=undefined,
                           next=undefined};
maybe_cancel_timer(State) ->
    State.

%% @doc Setup the automatic trigger to start the next
%% scheduled batch calculation.
-spec schedule_next(#gc_manager_state{}) -> #gc_manager_state{}.
schedule_next(#gc_manager_state{timer_ref=Ref}=State)
  when Ref =/= undefined ->
    case erlang:read_timer(Ref) of
        false ->
            schedule_next(State#gc_manager_state{timer_ref=undefined});
        _ ->
            ?LOG_DEBUG("Timer is already scheduled, maybe manually triggered?"),
            %% Timer is already scheduled, do nothing
            State
    end;
schedule_next(#gc_manager_state{interval=infinity}=State) ->
    %% nothing to schedule, all triggers manual
    State#gc_manager_state{next=undefined};
schedule_next(#gc_manager_state{interval=Interval}=State) ->
    RevisedNext = riak_cs_gc:timestamp() + Interval,
    TimerValue = Interval * 1000,
    TimerRef = erlang:send_after(TimerValue, self(), {start, []}),
    logger:info("Scheduled next batch at ~s",
                [riak_cs_gc_console:human_time(RevisedNext)]),
    State#gc_manager_state{next=RevisedNext,
                           timer_ref=TimerRef}.

translate(gc_batch_pid, _) -> [];
translate(batch_history, []) -> [];
translate(batch_history, [H|_]) ->
    #gc_batch_state{batch_start = Last} = H,
    [{last, Last}];
translate(batch_history, #gc_batch_state{batch_start = Last}) ->
    [{last, Last}];
translate(current_batch, undefined) -> [];
translate(current_batch, BatchState) ->
    riak_cs_gc_batch:status_data(BatchState);
translate(initial_delay, _) -> [];
translate(timer_ref, _) -> [];
translate(T, V) -> {T, V}.
