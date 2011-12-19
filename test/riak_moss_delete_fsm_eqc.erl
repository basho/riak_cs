%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_delete_fsm'.

-module(riak_moss_delete_fsm_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_delete_fsm/0]).

%% Helpers
-export([test/0,
         test/1,
         dummy_deleter/2,
         start_fsm/1]).

-define(TEST_ITERATIONS, 500).
-define(TESTMODULE, riak_moss_delete_fsm).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-record(fsm_state, {block_count :: non_neg_integer(),
                    dummy_pid :: pid(),
                    fsm_pid :: pid(),
                    fsm_state :: atom()
                   }).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60,
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_delete_fsm()))))}
       ]
      }
     ]
    }.

setup() ->
    application:load(riak_moss),
    ok.

cleanup(_) ->
    application:stop(riak_moss).

%% ====================================================================
%% eqc property
%% ====================================================================

prop_delete_fsm() ->
    ?FORALL({Bucket, FileName, ContentLength, BlockSize},
            {g_bucket(), g_file_name(), noshrink(g_content_length()), g_block_size()},
            ?TRAPEXIT(
               begin
                   BlockCount = riak_moss_lfs_utils:block_count(ContentLength, BlockSize),
                   %% Calculate the expected state transitions
                   ExpectedStates = expected_states(BlockCount),
                   %% Start a dummy writer process
                   DummyPid = spawn_link(?MODULE, dummy_deleter, [BlockCount, undefined]),
                   %% Start the fsm
                   application:set_env(riak_moss, lfs_block_size, BlockSize),
                   {ok, FsmPid} =
                       ?TESTMODULE:test_link([{deleter_pid, DummyPid}],
                                             Bucket,
                                             FileName,
                                             10000),

                   FsmState = #fsm_state{block_count=BlockCount,
                                         dummy_pid=DummyPid,
                                         fsm_pid=FsmPid},

                   %% Start the fsm processing and capture the state transitions
                   ActualStates = start_fsm(FsmState),
                   ?WHENFAIL(
                      begin
                          ?debugFmt("Expected States ~p~nActual States: ~p~n", [ExpectedStates, ActualStates]),
                          ?debugFmt("Expected States Count ~p~nActual States Count: ~p~n",
                                    [length(ExpectedStates), length(ActualStates)])
                      end,
                      conjunction(
                        [
                         {results_length, equals(length(ExpectedStates), length(ActualStates))},
                         {results_match, equals(ExpectedStates, ActualStates)}
                        ]))
               end)).

%%====================================================================
%% Generators
%%====================================================================

g_bucket() ->
    non_blank_string().

g_file_name() ->
    non_blank_string().

g_block_size() ->
    elements([1024, 2048, 4096, 8192, 16384, 32768, 65536]).

g_content_length() ->
    choose(1024, 65536).

non_blank_string() ->
    ?LET(X,not_empty(list(lower_char())), list_to_binary(X)).

%% Generate a lower 7-bit ACSII character that should not cause any problems
%% with utf8 conversion.
lower_char() ->
    choose(16#20, 16#7f).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_delete_fsm())).

expected_states(1) ->
    [waiting_file_info, waiting_root_delete];
expected_states(BlockCount) ->
    [waiting_file_info, waiting_root_update] ++
        [waiting_blocks_delete || _ <- lists:seq(1, BlockCount)] ++
        [waiting_root_delete].

dummy_deleter(BlockCount, LastResponse) ->
    receive
        {get_last_response, TestPid} ->
            TestPid ! LastResponse,
            dummy_deleter(BlockCount, LastResponse);
        {get_next_event, TestPid} ->
            Event = next_event(LastResponse, BlockCount),
            TestPid ! Event,
            dummy_deleter(BlockCount, LastResponse);
        {_, {update_root, set_inactive}} ->
            dummy_deleter(BlockCount, update_root);
        {_, {delete_block, _BlockID}} ->
            dummy_deleter(BlockCount-1, delete_block);
        {_, delete_root} ->
            dummy_deleter(BlockCount, delete_root);
        finish ->
            ok
    end.

next_event(Response, BlockCount) ->
    case Response of
        undefined ->
            case BlockCount == 1 of
                true ->
                    ObjDetails = object;
                false ->
                    ObjDetails = {file, BlockCount}
            end,
            {deleter_ready, ObjDetails};
        update_root ->
            root_inactive;
        delete_block ->
            {block_deleted, BlockCount};
        delete_root ->
            root_deleted
    end.

get_next_event(Pid) ->
    Pid ! {get_next_event, self()},
    receive
        undefined ->
            get_next_event(Pid);
        Event ->
            Event
    end.

start_fsm(State) ->
    execute_fsm(State, []).

execute_fsm(#fsm_state{dummy_pid=DummyPid,
                       fsm_pid=FsmPid,
                       fsm_state=waiting_root_delete},
            States) ->
    Event = get_next_event(DummyPid),
    ?TESTMODULE:send_event(FsmPid, Event),
    lists:reverse(States);
execute_fsm(FsmState=#fsm_state{dummy_pid=DummyPid,
                                fsm_pid=FsmPid},
            States) ->
    %% Get the current state
    CurrentState = current_state(FsmPid),
    Event = get_next_event(DummyPid),
    ?TESTMODULE:send_event(FsmPid, Event),
    execute_fsm(FsmState#fsm_state{fsm_state=CurrentState},
                [CurrentState | States]).

%% block_count(WriterPid) ->
%%     WriterPid ! {block_count, self()},
%%     receive
%%         BlockCount ->
%%             BlockCount
%%     end.

current_state(Pid) ->
    ?TESTMODULE:current_state(Pid).

-endif.
