%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc PropEr test module for `riak_cs_gc_manager'.

-module(prop_riak_cs_gc_manager).

-compile([{nowarn_deprecated_function, [{gen_fsm, sync_send_all_state_event, 2}]}]).

-include("include/riak_cs_gc.hrl").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% proper properties
-export([prop_set_interval/0,
         prop_manual_commands/0]).

%% States
-export([idle/1,
         running/1]).

%% proper_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

-define(QC_OUT(P),
        on_output(fun(Str, Args) ->
                          io:format(user, Str, Args) end, P)).

-define(TEST_ITERATIONS, 500).

-define(P(EXPR), PPP = (EXPR),
                 case PPP of
                     true -> ok;
                     _ -> io:format(user, "PPP=~p at line ~p: ~s~n", [PPP, ?LINE, ??EXPR])
                 end,
                 PPP).

-define(STATE, #gc_batch_state).
-record(mc_state, {}).

%%====================================================================
%% Eunit tests
%%====================================================================

proper_test_() ->
    {spawn,
     [{foreach,
       fun() ->
               meck:new(riak_cs_gc_batch, []),
               meck:expect(riak_cs_gc_batch, start_link,
                           fun(_GCDState) -> {ok, mock_pid} end),
               meck:expect(riak_cs_gc_batch, stop, fun(_) -> error(from_meck) end)
       end,
       fun(_) ->
               meck:unload()
       end,
       [
        {timeout, 20, ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_set_interval())))},
        {timeout, 60, ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_manual_commands())))}
       ]
      }]}.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_set_interval() ->
    ?FORALL(Interval,
            oneof([?LET(Nat, nat(), Nat + 1), infinity]),
            begin
                ok = application:set_env(riak_cs, initial_gc_delay, 0),
                {ok, Pid} = riak_cs_gc_manager:test_link(),
                try
                    {ok, {_, _, State1}} = riak_cs_gc_manager:status(),
                    ok = riak_cs_gc_manager:set_interval(Interval),
                    {ok, {_, _, State2}} = riak_cs_gc_manager:status(),
                    conjunction([{initial_interval,
                                  equals(?DEFAULT_GC_INTERVAL, State1#gc_manager_state.interval)},
                                 {updated_interval,
                                  equals(Interval, State2#gc_manager_state.interval)}
                                ])
                after
                    stop_and_hold_until_unregistered(Pid, riak_cs_gc_manager)
                end
            end).

prop_manual_commands() ->
    ?FORALL(Cmds,
            proper_fsm:commands(?MODULE),
            begin
                {ok, Pid} = riak_cs_gc_manager:test_link(),
                try
                    {H, _, Res} = proper_fsm:run_commands(?MODULE, Cmds),
                    aggregate(zip(proper_fsm:state_names(H), command_names(Cmds)),
                              ?WHENFAIL(
                                 begin
                                     io:format("Cmds: ~p~n~n",
                                               [zip(proper_fsm:state_names(H),
                                                    command_names(Cmds))]),
                                     io:format("Result: ~p~n~n", [Res]),
                                     io:format("History: ~p~n~n", [H])
                                 end,
                                 equals(ok, Res)))
                after
                    stop_and_hold_until_unregistered(Pid, riak_cs_gc_manager)
                end
            end).

%%====================================================================
%% proper_fsm callbacks
%%====================================================================

idle(_S) ->
    [
     {history, {call, riak_cs_gc_manager, cancel_batch, []}},
     {history, {call, riak_cs_gc_manager, set_interval, [infinity]}},
     {running, {call, riak_cs_gc_manager, start_batch, [[{leeway, 30}]]}},
     {idle, {call, riak_cs_gc_manager, finished, [report]}}
    ].

running(_S) ->
    [
     {history, {call, riak_cs_gc_manager, start_batch, [[{leeway, 30}]]}},
     {history, {call, riak_cs_gc_manager, set_interval, [infinity]}},
     {idle, {call, riak_cs_gc_manager, cancel_batch, []}},
     {idle, {call, riak_cs_gc_manager, finished, [report]}}
    ].

initial_state() ->
    idle.

initial_state_data() ->
    #mc_state{}.

next_state_data(_From, _To, S, _R, _C) ->
    S.

precondition(_From, _To, _S, _C) ->
    true.

postcondition(From, To, S, {call, _M, ManualCommad, _A}=C, R) ->
    {ok, {Actual, _, _}} = riak_cs_gc_manager:status(),
    ?assertEqual(To, Actual),
    ExpectedRes = expected_result(From, To, ManualCommad),
    case R of
        ExpectedRes when ManualCommad =/= status -> true;
        {ok, {S, _}} -> true;
        _ ->
            io:format("Result:   ~p~n", [R]),
            io:format("Expected: ~p~n", [ExpectedRes]),
            io:format("when {From, To, S, C}: ~p <- ~p~n", [{From, To, S, C}, ManualCommad]),
            false
    end.

expected_result(S, S, set_interval) ->
    ok;

expected_result(idle, running, start_batch) ->
    ok;
expected_result(idle, idle, finished) ->
    ok;
expected_result(idle, idle, _) ->
    {error, idle};

expected_result(running, idle, finished) ->
    ok;
expected_result(running, running, _) ->
    {error, running};

expected_result(_From, idle, cancel_batch) ->
    ok.

%% weight(idle, fetching_next_batch, _) -> 10;
%% weight(fetching_next_batch, feeding_workers, _) -> 5;
%% weight(feeding_workers, waiting_for_workers, _) -> 3;
%% weight(_, _, _) -> 1.

stop_and_hold_until_unregistered(Pid, RegName) ->
    ok = gen_fsm:sync_send_all_state_event(Pid, stop),
    hold_until_unregisterd(RegName, 50).

hold_until_unregisterd(_RegName, 0) ->
    {error, not_unregistered_so_long_time};
hold_until_unregisterd(RegName, N) ->
    case whereis(RegName) of
        undefined -> ok;
        _ ->
            timer:sleep(1),
            hold_until_unregisterd(RegName, N - 1)
    end.
