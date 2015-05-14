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

%% @doc Quickcheck test module for `riak_cs_gc_manager'.

-module(riak_cs_gc_manager_eqc).

-include("include/riak_cs_gc_d.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_set_interval/0,
         prop_manual_commands/0]).

%% States
-export([idle/1,
         running/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(TEST_ITERATIONS, 500).

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
     [{foreach,
       fun() ->
               meck:new(riak_cs_gc_d, []),
               meck:expect(riak_cs_gc_d, start_link,
                           fun(_GCDState) -> {ok, mock_pid} end),
               meck:expect(riak_cs_gc_d, stop, fun(_) -> error(from_meck) end),

               meck:new(riak_cs_gc_manager, [passthrough]),
               meck:expect(riak_cs_gc_manager, cancel_gc_d,
                           fun(mock_pid) -> ok end)
       end,
       fun(_) ->
               meck:unload()
       end,
       [
        {timeout, 20, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(10, ?QC_OUT(prop_set_interval()))))},
        {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_manual_commands()))))}
       ]
      }]}.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_set_interval() ->
    ?FORALL(Interval,
            oneof([?LET(Nat, nat(), Nat + 1), infinity]),
            begin
                ok = application:set_env(riak_cs, initial_gc_de6lay, 0),
                {ok, Pid} = riak_cs_gc_manager:test_link(),
                try
                    {ok, {_, State1}} = riak_cs_gc_manager:status(),
                    ok = riak_cs_gc_manager:set_interval(Interval),
                    {ok, {_, State2}} = riak_cs_gc_manager:status(),
                    conjunction([{initial_interval,
                                  equals(?DEFAULT_GC_INTERVAL, State1#gc_manager_state.interval)},
                                 {updated_interval,
                                  equals(Interval, State2#gc_manager_state.interval)}
                                ])
                after
                    ok = gen_fsm:sync_send_all_state_event(Pid, stop)
                end
            end).

prop_manual_commands() ->
    ?FORALL(Cmds,
            commands(?MODULE),
            begin
                {ok, Pid} = riak_cs_gc_manager:test_link(),
                try
                    {H, {_F, _S}, Res} = run_commands(?MODULE, Cmds),
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
                after
                    ok = gen_fsm:sync_send_all_state_event(Pid, stop)
                end
            end).

%%====================================================================
%% eqc_fsm callbacks
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

postcondition(From, To, S , {call, _M, ManualCommad, _A}=C, R) ->
    {ok, {Actual, _}} = riak_cs_gc_manager:status(),
    ?assertEqual(To, Actual),
    ExpectedRes = expected_result(From, To, ManualCommad),
    case R of
        ExpectedRes when ManualCommad =/= status -> true;
        {ok, {S, _}} -> true;
        _ ->
            eqc:format("Result:   ~p~n", [R]),
            eqc:format("Expected: ~p~n", [ExpectedRes]),
            eqc:format("when {From, To, S, C}: ~p <- ~p~n", [{From, To, S, C}, ManualCommad]),
            false
    end.

expected_result(S, S, set_interval) ->
    ok;

expected_result(idle, running, start_batch) ->
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

-endif.
