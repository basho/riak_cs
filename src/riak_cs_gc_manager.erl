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
%%            +-(resume)-paused_finished < --(finish)
%%            v                                  |
%% start -> idle -(start)-> running -(pause)-> paused
%%            ^               | ^                |
%%            +----(finish)---+ |                |
%%            +----(stop)-----+ +----(resume)----+
%%            +-------------(stop)---------------+
%%
%% Message excange chart (not a sequence, but just a list)
%%
%%  Message\  sdr/rcver  gc_manager    gc_d
%%   spawn_link)                   --->
%%   pause)                        --->
%%   cancel)                       --->
%%   resume)                       --->
%%   finished)                     <---
%%
-module(riak_cs_gc_manager).

-behaviour(gen_fsm).

%% Console API
-export([start_batch/1,
         stop_batch/0,
         pause_batch/0,
         resume_batch/0,
         set_interval/1,
         status/0]).

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
         running/2, running/3,
         paused/2, paused/3,
         paused_finished/2, paused_finished/3]).

-export([pause_gc_d/1, cancel_gc_d/1, resume_gc_d/1]).

-include("riak_cs_gc_d.hrl").

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start_batch(Options) ->
    gen_fsm:sync_send_event(?SERVER, {start, Options}, infinity).

stop_batch() ->
    gen_fsm:sync_send_event(?SERVER, stop, infinity).

pause_batch() ->
    gen_fsm:sync_send_event(?SERVER, pause, infinity).

resume_batch() ->
    gen_fsm:sync_send_event(?SERVER, resume, infinity).

status() ->
    gen_fsm:sync_send_all_state_event(?SERVER, status, infinity).

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
    InitialDelay = riak_cs_gc:initial_gc_delay(),
    _ = case riak_cs_gc:gc_interval() of
            infinity ->
                ok;
            Leeway when is_integer(Leeway) ->
                _TimerRef = erlang:send_after(InitialDelay * 1000, self(), {start, []})
        end,
    {ok, idle, #gc_manager_state{}};
init([testing]) ->
    {ok, idle, #gc_manager_state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc All gen_fsm:send_event/2 call should be ignored
idle(_Event, State) ->
    {next_state, idle, State}.
running(_Event, State) ->
    {next_state, running, State}.
paused_finished(_Event, State) ->
    {next_state, paused_finished, State}.
paused(_Event, State) ->
    {next_state, paused, State}.

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
    GCDState = #gc_d_state{
                  batch_start=BatchStart,
                  leeway=Leeway,
                  max_workers=MaxWorkers},

    case riak_cs_gc_d:start_link(GCDState) of
        {ok, Pid} ->
            {reply, ok, running,
             State#gc_manager_state{gc_d_pid=Pid,
                                    current_batch=GCDState}};
        Error ->
            {reply, Error, idle, State}
    end;
idle(_, _From, State) ->
    {reply, {error, idle}, idle, State}.

running(pause, _From, State = #gc_manager_state{gc_d_pid=Pid}) ->
    %% Fully qualified call here to mock in tests
    case ?MODULE:pause_gc_d(Pid) of
        ok ->
            {reply, ok, paused, State};
        Error ->
            {reply, Error, running, State}
    end;
running(stop, _From, State = #gc_manager_state{gc_d_pid=Pid}) ->
    %% stop gc_d here
    catch riak_cs_gc_d:stop(Pid),
    NextState=schedule_next(State),
    {reply, ok, idle, NextState#gc_manager_state{gc_d_pid=undefined}};
running({finished, Report}, _From, State = #gc_manager_state{batch_history=H}) ->
    %% Add report to history
    NextState=schedule_next(State),
    {reply, ok, idle,
     NextState#gc_manager_state{gc_d_pid=undefined,
                                batch_history=[Report|H]}};
running(_Event, _From, State) ->
    Reply = {error, running},
    {reply, Reply, running, State}.

paused_finished(resume, _From, State = #gc_manager_state{gc_d_pid=undefined}) ->
    %% Only resume can move back to idle when paused-finished state
    NextState=schedule_next(State),
    {reply, ok, idle, NextState};
paused_finished(_Event, _From, State) ->
    Reply = {error, paused_finished},
    {reply, Reply, paused_finished, State}.
    
paused(resume, _From, State = #gc_manager_state{gc_d_pid=Pid}) ->
    %% Resume here
    case ?MODULE:resume_gc_d(Pid) of
        ok ->
            {reply, ok, running, State};
        Error ->
            {reply, Error, paused, State}
    end;

paused({finished, Report}, _From, State = #gc_manager_state{batch_history=H}) ->
    %% Add report to history, becoming paused-finished state,
    %% where gc_d_pid is set undefined and the state is paused
    {reply, ok, paused_finished,
     State#gc_manager_state{gc_d_pid=undefined,
                            batch_history=[Report|H]}};
paused(stop, _From, State) ->
    %% Stop here, only for testing
    {reply, ok, idle, State};
paused(_Event, _From, State) ->
    Reply = {error, paused},
    {reply, Reply, paused, State}.

%% @private
%% @doc Not used.
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc
handle_sync_event({set_interval, Initerval}, _From, StateName, State) ->
    {reply, ok, StateName, State#gc_manager_state{interval=Initerval}};
handle_sync_event(status, _From, StateName, State) ->
    {reply, {ok, {StateName, State}}, StateName, State};
handle_sync_event(stop, _, _, State) ->
    %% for tests
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = {error, illegal_sync_send_all_state_call},
    {reply, Reply, StateName, State}.

%% @private
%% @doc Not used
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc Not used
terminate(_Reason, _StateName, _State = #gc_manager_state{gc_d_pid=Pid}) ->
    catch riak_cs_gc_d:stop(Pid),
    ok.

%% @private
%% @doc Not used
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Pause the garbage collection daemon.  Returns `ok' if
%% the daemon was paused, or `{error, already_paused}' if the daemon
%% was already paused.
pause_gc_d(Pid) ->
    gen_fsm:sync_send_event(Pid, pause, infinity).

%% @doc Cancel the garbage collection currently in progress.  Returns `ok' if
%% a batch was canceled, or `{error, no_batch}' if there was no batch
%% in progress.
cancel_gc_d(Pid) ->
    riak_cs_gc_d:stop(Pid).

%% @doc Resume the garbage collection daemon.  Returns `ok' if the
%% daemon was resumed, or `{error, not_paused}' if the daemon was
%% not paused.
resume_gc_d(Pid) ->
    gen_fsm:sync_send_event(Pid, resume, infinity).

%% @doc Setup the automatic trigger to start the next
%% scheduled batch calculation.
-spec schedule_next(#gc_manager_state{}) -> #gc_manager_state{}.
schedule_next(#gc_manager_state{interval=infinity}=State) ->
    %% nothing to schedule, all triggers manual
    State;
schedule_next(#gc_manager_state{interval=Interval}=State) ->
    RevisedNext = riak_cs_gc:timestamp() + Interval,
    TimerValue = Interval * 1000,
    TimerRef = erlang:send_after(TimerValue, self(), {start, []}),
    State#gc_manager_state{next=RevisedNext,
                           timer_ref=TimerRef}.

