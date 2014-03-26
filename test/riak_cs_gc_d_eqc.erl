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

%% @doc Quickcheck test module for `riak_cs_gc_d'.

-module(riak_cs_gc_d_eqc).

-include("riak_cs_gc_d.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_set_interval/0,
         prop_manual_commands/0,
         prop_status/0]).

%% States
-export([idle/1,
         fetching_next_batch/1,
         feeding_workers/1,
         waiting_for_workers/1,
         paused/2]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% Helpers
-export([test/0,
         test/1]).

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(TEST_ITERATIONS, 500).
-define(GCD_MODULE, riak_cs_gc_d).

-define(P(EXPR), PPP = (EXPR),
                 case PPP of
                     true -> ok;
                     _ -> io:format(user, "PPP=~p at line ~p: ~s~n", [PPP, ?LINE, ??EXPR])
                 end,
                 PPP).

-define(STATE, #gc_d_state).
-record(mc_state, {current_state :: atom(),
                   previous_state :: atom()}).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 20, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(10, ?QC_OUT(prop_set_interval()))))},
      {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_manual_commands()))))},
      {timeout, 20, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(10, ?QC_OUT(prop_status()))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_set_interval() ->
    ?FORALL(Interval, int(),
            catch begin
                catch riak_cs_gc_d:stop(),
                {ok, _} = riak_cs_gc_d:test_link(?DEFAULT_GC_INTERVAL),
                {_, State1} = riak_cs_gc_d:current_state(),
                riak_cs_gc_d:set_interval(Interval),
                {_, State2} = riak_cs_gc_d:current_state(),
                riak_cs_gc_d:stop(),
                conjunction([{initial_interval, equals(?DEFAULT_GC_INTERVAL, State1?STATE.interval)},
                             {updated_interval, equals(Interval, State2?STATE.interval)}
                            ])
            end).

prop_manual_commands() ->
    ?FORALL(Cmds,
            commands(?MODULE),
            begin
                catch riak_cs_gc_d:stop(),
                {ok, _} = riak_cs_gc_d:test_link(infinity),
                {H, {_F, _S}, Res} = run_commands(?MODULE, Cmds),
                riak_cs_gc_d:stop(),
                aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 eqc:format("Cmds: ~p~n~n",
                                            [zip(state_names(H),
                                                 command_names(Cmds))]),
                                 eqc:format("Result: ~p~n~n", [Res]),
                                 eqc:format("History: ~p~n~n", [H])
                             end,
                             equals(ok, Res)))
            end
           ).


prop_status() ->
    ?FORALL({Interval, Last, Next,
             Start, Count,
             Skips, Batch},
            {eqc_gen:int(), riak_cs_gen:datetime(), riak_cs_gen:datetime(),
             eqc_gen:int(), eqc_gen:int(),
             eqc_gen:int(), eqc_gen:list(eqc_gen:int())},
            begin
                State = ?STATE{interval=Interval,
                               last=Last,
                               next=Next,
                               batch_start=Start,
                               batch_count=Count,
                               batch_skips=Skips,
                               batch=Batch},
                Status = orddict:from_list(riak_cs_gc_d:status_data(State)),
                conjunction([{interval, equals(orddict:fetch(interval, Status), Interval)},
                             {current, equals(orddict:fetch(current, Status), Start)},
                             {next, equals(orddict:fetch(next, Status), Next)},
                             {files_deleted, equals(orddict:fetch(files_deleted, Status), Count)},
                             {files_skipped, equals(orddict:fetch(files_skipped, Status), Skips)},
                             {files_left, equals(orddict:fetch(files_left, Status), length(Batch)) }
                            ])
            end).

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

idle(_S) ->
    [
     {history, {call, ?GCD_MODULE, cancel_batch, []}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {{paused, idle}, {call, ?GCD_MODULE, pause, []}},
     {fetching_next_batch, {call, ?GCD_MODULE, manual_batch, [[testing]]}}
    ].

fetching_next_batch(_S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {idle, {call, ?GCD_MODULE, cancel_batch, []}},
     {{paused, fetching_next_batch}, {call, ?GCD_MODULE, pause, []}},
     {feeding_workers, {call, ?GCD_MODULE, change_state, [feeding_workers]}}
    ].

feeding_workers(_S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {idle, {call, ?GCD_MODULE, cancel_batch, []}},
     {{paused, feeding_workers}, {call, ?GCD_MODULE, pause, []}},
     {fetching_next_batch, {call, ?GCD_MODULE, change_state, [fetching_next_batch]}},
     {waiting_for_workers, {call, ?GCD_MODULE, change_state, [waiting_for_workers]}}
    ].

waiting_for_workers(_S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {idle, {call, ?GCD_MODULE, cancel_batch, []}},
     {{paused, waiting_for_workers}, {call, ?GCD_MODULE, pause, []}},
     {feeding_workers, {call, ?GCD_MODULE, change_state, [feeding_workers]}}
    ].

paused(PrevState, _S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, pause, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {{paused, idle}, {call, ?GCD_MODULE, cancel_batch, []}},
     {PrevState, {call, ?GCD_MODULE, resume, []}}
    ].

initial_state() ->
    idle.

initial_state_data() ->
    #mc_state{previous_state = idle}.

next_state_data({paused, _}, {paused, _}, S, _R, {call, ?GCD_MODULE, cancel_batch, []}) ->
    S#mc_state{previous_state=idle};
next_state_data(From, To, S, _R, _C) when From == To ->
    S;
next_state_data(From, To, S, _R, _C) ->
    S#mc_state{current_state=To,
               previous_state=From}.

precondition(_From, _To, _S, _C) ->
    true.

%% `idle' state transitions
postcondition(idle, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= {error, no_batch});
postcondition(idle, {paused, _}, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(idle, fetching_next_batch, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= fetching_next_batch andalso R =:= ok);

%% `fetching_next_batch' state transitions
postcondition(fetching_next_batch, fetching_next_batch, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= fetching_next_batch andalso R =:= {error, already_deleting});
postcondition(fetching_next_batch, {paused, _}, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(fetching_next_batch, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= ok);
postcondition(fetching_next_batch, feeding_workers, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= feeding_workers andalso R =:= ok);

%% `feeding_workers' state transitions
postcondition(feeding_workers, feeding_workers, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= feeding_workers andalso R =:= {error, already_deleting});
postcondition(feeding_workers, {paused, _}, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(feeding_workers, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= ok);
postcondition(feeding_workers, fetching_next_batch, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= fetching_next_batch andalso R =:= ok);
postcondition(feeding_workers, waiting_for_workers, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= waiting_for_workers andalso R =:= ok);

%% `waiting_for_workers' state transitions
postcondition(waiting_for_workers, waiting_for_workers, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= waiting_for_workers andalso R =:= {error, already_deleting});
postcondition(waiting_for_workers, {paused, _}, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(waiting_for_workers, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= ok);
postcondition(waiting_for_workers, feeding_workers, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= waiting_file_delete andalso R =:= ok);

%% `paused' state transitions
postcondition({paused, _}, {paused, _}, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= {error, already_paused});
postcondition({paused, _}, {paused, _}, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition({paused, _}, {paused, _}, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= {error, already_paused});
postcondition({paused, _}, PrevState, #mc_state{previous_state=PrevState} ,{call, _M, resume, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= PrevState andalso R =:= ok);

%% General handling of `resume' calls when the `From' state
%% is not `paused'.
postcondition(_From, To, _S ,{call, _M, resume, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= To andalso R =:= {error, not_paused});
%% Handling of arbitrary calls that should return `ok'.
postcondition(_From, To, _S ,{call, _M, _F, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= actual_state(To) andalso R =:= ok);
%% Catch all
postcondition(_From, _To, _S , _C, _R) ->
    true.

actual_state({State, _}) -> State;
actual_state(S) -> S.

%% weight(fetching_next_batch,fetching_next_batch,{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 64;
%% weight(fetching_next_batch,fetching_next_batch,{call,riak_cs_gc_d,resume,[]}) -> 64;
%% weight(fetching_next_batch,fetching_next_batch,{call,riak_cs_gc_d,set_interval,[infinity]}) -> 64;
%% weight(fetching_next_batch,idle,{call,riak_cs_gc_d,cancel_batch,[]}) -> 64;
%% weight(fetching_next_batch,fetching_next_fileset,{call,riak_cs_gc_d,change_state,[fetching_next_fileset]}) -> 556;
%% weight(fetching_next_batch,{paused,fetching_next_batch},{call,riak_cs_gc_d,pause,[]}) -> 191;
%% weight(feeding_workers,feeding_workers,{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 64;
%% weight(feeding_workers,feeding_workers,{call,riak_cs_gc_d,resume,[]}) -> 64;
%% weight(feeding_workers,feeding_workers,{call,riak_cs_gc_d,set_interval,[infinity]}) -> 64;
%% weight(feeding_workers,idle,{call,riak_cs_gc_d,cancel_batch,[]}) -> 64;
%% weight(feeding_workers,initiating_file_delete,{call,riak_cs_gc_d,change_state,[initiating_file_delete]}) -> 556;
%% weight(feeding_workers,{paused,feeding_workers},{call,riak_cs_gc_d,pause,[]}) -> 191;
%% weight(idle,fetching_next_batch,{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 798;
%% weight(idle,idle,{call,riak_cs_gc_d,cancel_batch,[]}) -> 64;
%% weight(idle,idle,{call,riak_cs_gc_d,resume,[]}) -> 64;
%% weight(idle,idle,{call,riak_cs_gc_d,set_interval,[infinity]}) -> 64;
%% weight(idle,{paused,idle},{call,riak_cs_gc_d,pause,[]}) -> 127;
%% weight(waiting_for_workers,idle,{call,riak_cs_gc_d,cancel_batch,[]}) -> 127;
%% weight(waiting_for_workers,waiting_for_workers,{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 46;
%% weight(waiting_for_workers,waiting_for_workers,{call,riak_cs_gc_d,resume,[]}) -> 46;
%% weight(waiting_for_workers,waiting_for_workers,{call,riak_cs_gc_d,set_interval,[infinity]}) -> 46;
%% weight(waiting_for_workers,waiting_file_delete,{call,riak_cs_gc_d,change_state,[waiting_file_delete]}) -> 192;
%% weight(waiting_for_workers,{paused,waiting_for_workers},{call,riak_cs_gc_d,pause,[]}) -> 129;
%% weight({paused,fetching_next_batch},fetching_next_batch,{call,riak_cs_gc_d,resume,[]}) -> 149;
%% weight({paused,fetching_next_batch},{paused,fetching_next_batch},{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 124;
%% weight({paused,fetching_next_batch},{paused,fetching_next_batch},{call,riak_cs_gc_d,pause,[]}) -> 124;
%% weight({paused,fetching_next_batch},{paused,fetching_next_batch},{call,riak_cs_gc_d,set_interval,[infinity]}) -> 124;
%% weight({paused,fetching_next_batch},{paused,idle},{call,riak_cs_gc_d,cancel_batch,[]}) -> 178;
%% weight({paused,fetching_next_fileset},fetching_next_fileset,{call,riak_cs_gc_d,resume,[]}) -> 149;
%% weight({paused,fetching_next_fileset},{paused,fetching_next_fileset},{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 124;
%% weight({paused,fetching_next_fileset},{paused,fetching_next_fileset},{call,riak_cs_gc_d,pause,[]}) -> 124;
%% weight({paused,fetching_next_fileset},{paused,fetching_next_fileset},{call,riak_cs_gc_d,set_interval,[infinity]}) -> 124;
%% weight({paused,fetching_next_fileset},{paused,idle},{call,riak_cs_gc_d,cancel_batch,[]}) -> 178;
%% weight({paused,idle},idle,{call,riak_cs_gc_d,resume,[]}) -> 683;
%% weight({paused,idle},{paused,idle},{call,riak_cs_gc_d,cancel_batch,[]}) -> 128;
%% weight({paused,idle},{paused,idle},{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 128;
%% weight({paused,idle},{paused,idle},{call,riak_cs_gc_d,pause,[]}) -> 128;
%% weight({paused,idle},{paused,idle},{call,riak_cs_gc_d,set_interval,[infinity]}) -> 128;
%% weight({paused,initiating_file_delete},initiating_file_delete,{call,riak_cs_gc_d,resume,[]}) -> 128;
%% weight({paused,initiating_file_delete},{paused,idle},{call,riak_cs_gc_d,cancel_batch,[]}) -> 193;
%% weight({paused,initiating_file_delete},{paused,initiating_file_delete},{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 128;
%% weight({paused,initiating_file_delete},{paused,initiating_file_delete},{call,riak_cs_gc_d,pause,[]}) -> 128;
%% weight({paused,initiating_file_delete},{paused,initiating_file_delete},{call,riak_cs_gc_d,set_interval,[infinity]}) -> 128;
%% weight({paused,waiting_file_delete},waiting_file_delete,{call,riak_cs_gc_d,resume,[]}) -> 1;
%% weight({paused,waiting_file_delete},{paused,idle},{call,riak_cs_gc_d,cancel_batch,[]}) -> 1;
%% weight({paused,waiting_file_delete},{paused,waiting_file_delete},{call,riak_cs_gc_d,manual_batch,[[testing]]}) -> 1;
%% weight({paused,waiting_file_delete},{paused,waiting_file_delete},{call,riak_cs_gc_d,pause,[]}) -> 1;
%% weight({paused,waiting_file_delete},{paused,waiting_file_delete},{call,riak_cs_gc_d,set_interval,[infinity]}) -> 1.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_status())).

test(Iterations, Prop) ->
    eqc:quickcheck(eqc:numtests(Iterations, ?MODULE:Prop())).

-endif.
