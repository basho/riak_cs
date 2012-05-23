%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_cs_gc_d'.

-module(riak_cs_gc_d_eqc).

-include("riak_moss.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_status/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 500).

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

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
        [
            {timeout, 20, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_set_interval()))))},
            %% {timeout, 20, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_manual_commands()))))},
            {timeout, 20, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_status()))))}
        ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_set_interval() ->
    ?FORALL(Interval, eqc_gen:int(),
            begin
                case whereis(riak_cs_gc_d) of
                    undefined ->
                        {ok, _} = riak_cs_gc_d:start_link();
                    _Pid ->
                        riak_cs_gc_d:set_interval(?DEFAULT_GC_INTERVAL)
                end,
                {_, State1} = riak_cs_gc_d:current_state(),
                riak_cs_gc_d:set_interval(Interval),
                {_, State2} = riak_cs_gc_d:current_state(),
                conjunction([{initial_interval, equals(?DEFAULT_GC_INTERVAL, State1#state.interval)},
                             {updated_interval, equals(Interval, State2#state.interval)}
                            ])
            end).

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
            Status = orddict:from_list(riak_cs_gc_d:status(State)),
            conjunction([{interval, equals(orddict:fetch(interval, Status), Interval)},
                         {current, equals(orddict:fetch(current, Status), Start)},
                         {next, equals(orddict:fetch(next, Status), Next)},
                         {files_deleted, equals(orddict:fetch(files_deleted, Status), Count)},
                         {files_skipped, equals(orddict:fetch(files_skipped, Status), Skips)},
                         {files_left, equals(orddict:fetch(files_left, Status), length(Batch)) }
                        ])
        end).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_status())).

-endif.
