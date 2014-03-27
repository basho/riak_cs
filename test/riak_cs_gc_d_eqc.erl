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
-record(mc_state, {}).

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
            end).

prop_status() ->
    ?FORALL({Interval, Last, Next,
             Start, Count, Skips, Batch},
            {int(), riak_cs_gen:datetime(), riak_cs_gen:datetime(),
             int(), int(), int(), list(int())},
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
    #mc_state{}.

next_state_data(_From, _To, S, _R, _C) ->
    S.

precondition(_From, _To, _S, _C) ->
    true.

postcondition(From, To, S , {call, _M, ManualCommad, _A}=C, R) ->
    {Actual, _} = riak_cs_gc_d:current_state(),
    ?assertEqual(actual_state(To), Actual),
    ExpectedRes = expected_result(From, To, ManualCommad),
    case R of
        ExpectedRes -> true;
        _ ->
            eqc:format("Result:   ~p~n", [R]),
            eqc:format("Expected: ~p~n", [ExpectedRes]),
            eqc:format("when {From, To, S, C}: ~p~n", [{From, To, S, C}]),
            false
    end.

actual_state({State, _}) -> State;
actual_state(S) -> S.

%% Handling of `set_interval' calls. Always succeeds.
expected_result(From, From, set_interval) ->
    ok;

%% Handling of `pause' and `resume' calls. Just becomes paused and back.
expected_result({paused, StateToResume}, {paused, StateToResume}, pause) ->
    {error, already_paused};
expected_result(From, {paused, From}, pause)  ->
    ok;
expected_result({paused, StateToResume}, StateToResume, resume) ->
    ok;
expected_result(From, From, resume) when is_atom(From) ->
    {error, not_paused};

%% Handling of `manual_batch' and `cancel_batch'.
%% Almost just becomes fetching_next_batch and back to idle,
%% only `cancel_batch' at paused is special.
expected_result({paused, From}, {paused, From}, manual_batch) ->
    {error, already_paused};
expected_result({paused, _StateAtPause}, {paused, idle}, cancel_batch) ->
    ok;
expected_result(idle, fetching_next_batch, manual_batch) ->
    ok;
expected_result(From, From, manual_batch) ->
    {error, already_deleting};
expected_result(idle, idle, cancel_batch) ->
    {error, no_batch};
expected_result(_From, idle, cancel_batch) ->
    ok;

%% `change_state'
expected_result(_From, _To, change_state) ->
    ok.

weight(idle, fetching_next_batch, _) -> 10;
weight(fetching_next_batch, feeding_workers, _) -> 5;
weight(feeding_workers, waiting_for_workers, _) -> 3;
weight(_, _, _) -> 1.

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
