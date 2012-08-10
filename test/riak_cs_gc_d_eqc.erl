%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_cs_gc_d'.

-module(riak_cs_gc_d_eqc).

-include("riak_moss.hrl").
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
         fetching_next_fileset/1,
         initiating_file_delete/1,
         waiting_file_delete/1,
         paused/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 500).
-define(GCD_MODULE, riak_cs_gc_d).

-define(P(EXPR), PPP = (EXPR), case PPP of true -> ok; _ -> io:format(user, "PPP ~p at line ~p\n", [PPP, ?LINE]) end, PPP).

-record(mc_state, {current_state :: atom(),
                   previous_state :: atom()}).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 20, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_set_interval()))))},
      {timeout, 60, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_manual_commands()))))},
      {timeout, 20, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_status()))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_set_interval() ->
    ?FORALL(Interval, int(),
            begin
                case whereis(riak_cs_gc_d) of
                    undefined ->
                        {ok, _} = riak_cs_gc_d:test_link();
                    _Pid ->
                        riak_cs_gc_d:set_interval(?DEFAULT_GC_INTERVAL)
                end,
                {_, State1} = riak_cs_gc_d:current_state(),
                riak_cs_gc_d:set_interval(Interval),
                {_, State2} = riak_cs_gc_d:current_state(),
                riak_cs_gc_d:stop(),
                conjunction([{initial_interval, equals(?DEFAULT_GC_INTERVAL, State1#state.interval)},
                             {updated_interval, equals(Interval, State2#state.interval)}
                            ])
            end).

prop_manual_commands() ->
    ?FORALL(Cmds,
            commands(?MODULE),
            begin
                case whereis(riak_cs_gc_d) of
                    undefined ->
                        {ok, _} = riak_cs_gc_d:test_link(infinity);
                    _Pid ->
                        riak_cs_gc_d:set_interval(infinity),
                        riak_cs_gc_d:change_state(idle)
                end,
                {H, {_F, _S}, Res} = run_commands(?MODULE, Cmds),
                riak_cs_gc_d:stop(),
                aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 ?debugFmt("Cmds: ~p~n",
                                           [zip(state_names(H),
                                                command_names(Cmds))]),
                                 ?debugFmt("Result: ~p~n", [Res]),
                                 ?debugFmt("History: ~p~n", [H])
                             end,
                             equals(ok, Res)))
            end
           ).

prop_status() ->
    ?FORALL({Interval, Last, Next,
             Start, Count,
             Skips, Batch},
            {eqc_gen:int(), moss_gen:datetime(),
             moss_gen:datetime(), eqc_gen:int(), eqc_gen:int(),
             eqc_gen:int(), eqc_gen:list(eqc_gen:int())},
            begin
                State = #state{interval=Interval,
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
     {paused, {call, ?GCD_MODULE, pause, []}},
     {fetching_next_fileset, {call, ?GCD_MODULE, manual_batch, [[testing]]}}
    ].

fetching_next_fileset(_S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {idle, {call, ?GCD_MODULE, cancel_batch, []}},
     {paused, {call, ?GCD_MODULE, pause, []}},
     {initiating_file_delete, {call, ?GCD_MODULE, change_state, [initiating_file_delete]}}
    ].

initiating_file_delete(_S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {idle, {call, ?GCD_MODULE, cancel_batch, []}},
     {paused, {call, ?GCD_MODULE, pause, []}},
     {waiting_file_delete, {call, ?GCD_MODULE, change_state, [waiting_file_delete]}}
    ].

waiting_file_delete(_S) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, resume, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {idle, {call, ?GCD_MODULE, cancel_batch, []}},
     {paused, {call, ?GCD_MODULE, pause, []}},
     {initiating_file_delete, {call, ?GCD_MODULE, change_state, [initiating_file_delete]}}
    ].

paused(#mc_state{previous_state=PauseState}) ->
    [
     {history, {call, ?GCD_MODULE, manual_batch, [[testing]]}},
     {history, {call, ?GCD_MODULE, pause, []}},
     {history, {call, ?GCD_MODULE, set_interval, [infinity]}},
     {history, {call, ?GCD_MODULE, cancel_batch, []}},
     {PauseState, {call, ?GCD_MODULE, resume, []}}
    ].

initial_state() ->
    idle.

initial_state_data() ->
    #mc_state{}.

next_state_data(paused, paused, S, _R, {call, ?GCD_MODULE, cancel_batch, []}) ->
    S#mc_state{previous_state=idle};
next_state_data(_From, _From, S, _R, _C) ->
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
postcondition(idle, paused, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
%% `fetching_next_fileset' state transitions
postcondition(idle, fetching_next_fileset, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= fetching_next_fileset andalso R =:= ok);
postcondition(_From, fetching_next_fileset, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= fetching_next_fileset andalso R =:= {error, already_deleting});
postcondition(fetching_next_fileset, paused, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(fetching_next_fileset, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= ok);
postcondition(fetching_next_fileset, initiating_file_delete, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= initiating_file_delete andalso R =:= ok);
%% Transitions to `initiating_file_delete' state
postcondition(_From, initiating_file_delete, #mc_state{current_state=initiating_file_delete} ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= initiating_file_delete andalso R =:= {error, already_deleting});
postcondition(initiating_file_delete, paused, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(initiating_file_delete, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= ok);
postcondition(initiating_file_delete, waiting_file_delete, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= waiting_file_delete andalso R =:= ok);
%% `waiting_file_delete' transitions
postcondition(_From, waiting_file_delete, _S ,{call, _M, manual_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= waiting_file_delete andalso R =:= {error, already_deleting});
postcondition(waiting_file_delete, paused, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(waiting_file_delete, idle, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= idle andalso R =:= ok);
postcondition(waiting_file_delete, initiating_file_delete, _S ,{call, _M, change_state, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= initiating_file_delete andalso R =:= ok);
%% `paused' transitions
postcondition(paused, paused, _S ,{call, _M, cancel_batch, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= ok);
postcondition(paused, paused, _S ,{call, _M, pause, _}, R) ->
    {ActualState, _} = riak_cs_gc_d:current_state(),
    ?P(ActualState =:= paused andalso R =:= {error, already_paused});
postcondition(paused, PrevState, #mc_state{previous_state=PrevState} ,{call, _M, resume, _}, R) ->
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
    ?P(ActualState =:= To andalso R =:= ok);
%% Catch all
postcondition(_From, _To, _S , _C, _R) ->
    true.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_status())).

-endif.
