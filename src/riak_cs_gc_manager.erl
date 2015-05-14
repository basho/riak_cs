%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
%%            +----(finish)---+
%%            +----(stop)-----+
%%
%% Message excange chart (not a sequence, but just a list)
%%
%%  Message\  sdr/rcver  gc_manager    gc_batch
%%   spawn_link)                   --->
%%   cancel)                       --->
%%   finished)                     <---
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

-type statename() :: idle | running.
-export_type([statename/0]).

-include("riak_cs_gc.hrl").

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start_batch(Options) ->
    gen_fsm:sync_send_event(?SERVER, {start, Options}, infinity).

cancel_batch() ->
    gen_fsm:sync_send_event(?SERVER, cancel, infinity).

-spec status() -> {ok, {statename(), #gc_manager_state{}}}.
status() ->
    gen_fsm:sync_send_all_state_event(?SERVER, status, infinity).

-spec pp_status() -> {ok, {statename(), proplists:proplist()}}.
pp_status() ->
    {ok, {StateName, State}} = status(),
    D = lists:zip(record_info(fields, gc_manager_state),
                  tl(tuple_to_list(State))),
    Details = lists:flatten(lists:map(fun({Type, Value}) ->
                                              translate(Type, Value)
                                      end, D)),
    {ok, {StateName,
          [{leeway, riak_cs_gc:leeway_seconds()}] ++ Details}}.

%% @doc Adjust the interval at which the daemon attempts to perform
%% a garbage collection sweep. Setting the interval to a value of
%% `infinity' effectively disable garbage collection. The daemon still
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
    gen_fsm:sync_send_event(?SERVER, {finished, Report}, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    InitialDelay = riak_cs_gc:initial_gc_delay(),
    State = case riak_cs_gc:gc_interval() of
                infinity ->
                    #gc_manager_state{};
                Interval when is_integer(Interval) ->
                    Next = riak_cs_gc:timestamp() + InitialDelay,
                    TimerRef = erlang:send_after(InitialDelay * 1000, self(), {start, []}),
                    #gc_manager_state{next=Next,
                                      interval=Interval,
                                      initial_delay=InitialDelay,
                                      timer_ref=TimerRef}
        end,
    {ok, idle, State};
init([testing]) ->
    {ok, idle, #gc_manager_state{}}.

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
    MaxWorkers =  riak_cs_gc:gc_max_workers(),
    %% StartKey = proplists:get_value(start, Options,
    %%                                riak_cs_gc:epoch_start()),
    BatchStart = riak_cs_gc:timestamp(),
    %% EndKey = proplists:get_value('end', Options, BatchStart),
    Leeway = proplists:get_value(leeway, Options,
                                 riak_cs_gc:leeway_seconds()),

    %% set many items to GCDState here
    GCDState = #gc_batch_state{
                  batch_start=BatchStart,
                  leeway=Leeway,
                  max_workers=MaxWorkers},

    case riak_cs_gc_batch:start_link(GCDState) of
        {ok, Pid} ->
            {reply, ok, running,
             State#gc_manager_state{gc_batch_pid=Pid,
                                    current_batch=GCDState}};
        Error ->
            {reply, Error, idle, State}
    end;
idle(_, _From, State) ->
    {reply, {error, idle}, idle, State}.

running(cancel, _From, State = #gc_manager_state{gc_batch_pid=Pid}) ->
    %% stop gc_batch here
    catch riak_cs_gc_batch:stop(Pid),
    NextState=schedule_next(State),
    {reply, ok, idle, NextState#gc_manager_state{gc_batch_pid=undefined}};
running({finished, Report}, _From, State = #gc_manager_state{batch_history=H}) ->
    %% Add report to history
    NextState=schedule_next(State),
    {reply, ok, idle,
     NextState#gc_manager_state{gc_batch_pid=undefined,
                                batch_history=[Report|H]}};
running(_Event, _From, State) ->
    Reply = {error, running},
    {reply, Reply, running, State}.

%% @private
%% @doc Not used.
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc
handle_sync_event({set_interval, Initerval}, _From, StateName, State) ->
    NewState0 = maybe_cancel_timer(State#gc_manager_state{interval=Initerval}),
    NewState = schedule_next(NewState0),
    {reply, ok, StateName, NewState};
handle_sync_event(status, _From, StateName, #gc_manager_state{gc_batch_pid=Pid} = State) ->
    {_GCDStateName, GCDState} = maybe_current_state(Pid),
    NewState = State#gc_manager_state{current_batch=GCDState},
    {reply, {ok, {StateName, NewState}}, StateName, NewState};
handle_sync_event(stop, _, _, State) ->
    %% for tests
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = {error, illegal_sync_send_all_state_call},
    {reply, Reply, StateName, State}.

%% @private
%% @doc Because process flag of trap_exit is true, gc_batch
%% failure will be delivered as a message.
handle_info({'EXIT', _Pid, normal}, StateName, State) ->
    {next_state, StateName, State#gc_manager_state{gc_batch_pid=undefined}};
handle_info(Info, StateName, State) ->
    _ = lager:warning("GC process error detected: ~p", [Info]),
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

-ifdef(TEST).
maybe_current_state(undefined) -> {not_running, undefined};
maybe_current_state(mock_pid) -> {not_running, undefined}.
-else.
maybe_current_state(undefined) -> {not_running, undefined};
maybe_current_state(Pid) when is_pid(Pid) ->
    riak_cs_gc_batch:current_state(Pid).
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
            _ = lager:debug("Timer is already scheduled, maybe manually triggered?"),
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
    State#gc_manager_state{next=RevisedNext,
                           timer_ref=TimerRef}.


translate(gc_batch_pid, _) -> [];
translate(batch_history, []) -> [];
translate(batch_history, [H|_]) ->
    #gc_batch_state{batch_start = Last} = H,
    [{last, Last}];
translate(current_batch, undefined) -> [];
translate(current_batch, GCDState) ->
    riak_cs_gc_batch:status_data(GCDState);
translate(initial_delay, _) -> [];
translate(timer_ref, _) -> [];
translate(T, V) -> {T, V}.
