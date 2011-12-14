%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_put_fsm'.

-module(riak_moss_put_fsm_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_put_fsm/0]).

%% Helpers
-export([test/0,
         test/1,
         dummy_writer/2,
         start_fsm/1]).

-define(TEST_ITERATIONS, 500).
-define(TESTMODULE, riak_moss_put_fsm).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-record(fsm_state, {block_count :: non_neg_integer(),
                    dummy_pid :: pid(),
                    fsm_pid :: pid(),
                    fsm_state :: atom(),
                    last_response :: term(),
                    next_event :: term()
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
         ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_put_fsm()))))}
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

prop_put_fsm() ->
    ?FORALL({Bucket, FileName, ContentLength, BlockSize},
            {g_bucket(), g_file_name(), noshrink(g_content_length()), g_block_size()},
            ?TRAPEXIT(
               begin
                   BlockCount = riak_moss_lfs_utils:block_count(ContentLength, BlockSize),
                   %% Calculate the expected state transitions
                   ExpectedStates = expected_states(BlockCount),
                   %% Start a dummy writer process
                   DummyPid = spawn_link(?MODULE, dummy_writer, [BlockCount, undefined]),
                   %% Start the fsm
                   application:set_env(riak_moss, lfs_block_size, BlockSize),
                   Data = list_to_binary(string:chars($a, ContentLength)),
                   {ok, FsmPid} =
                       ?TESTMODULE:test_link([{writer_pid, DummyPid}],
                                             Bucket,
                                             FileName,
                                             ContentLength,
                                             "text/plain",
                                             Data,
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
                         {results, equals(ExpectedStates, ActualStates)}
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
    eqc:quickcheck(eqc:numtests(Iterations, prop_put_fsm())).

expected_states(BlockCount) ->
    lists:flatten([[write_root | [write_block]] || _ <- lists:seq(1, BlockCount+1)]).

dummy_writer(BlockCount, LastResponse) ->
    receive
        {get_last_response, TestPid} ->
            TestPid ! LastResponse,
            dummy_writer(BlockCount, LastResponse);
        {get_next_event, TestPid} ->
            Event = next_event(LastResponse, BlockCount),
            TestPid ! Event,
            dummy_writer(BlockCount, LastResponse);
        {block_count, RequestPid} ->
            RequestPid ! BlockCount,
            dummy_writer(BlockCount, LastResponse);
        {_, write_root} ->
            dummy_writer(BlockCount, write_root);
        {_, {update_root, {block_ready, _BlockID}}} ->
            dummy_writer(BlockCount-1, update_root);
        {_, {write_block, BlockID, _Data}} ->
            dummy_writer(BlockCount, {write_block, BlockID});
        finish ->
            ok
    end.

next_event(Response, BlockCount) ->
    case Response of
        undefined ->
            writer_ready;
        write_root ->
            root_ready;
        update_root when BlockCount > 0 ->
            root_ready;
        update_root ->
            {all_blocks_written, manifest};
        {write_block, BlockID} ->
            {block_written, BlockID}
    end.

get_response(Pid) ->
    Pid ! {get_last_response, self()},
    receive
        undefined ->
            get_response(Pid);
        Response ->
            Response
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

execute_fsm(#fsm_state{block_count=0,
                       dummy_pid=DummyPid},
            States) ->
            lists:reverse(States);
execute_fsm(FsmState=#fsm_state{dummy_pid=DummyPid,
                                fsm_pid=FsmPid},
            States) ->
    %% Get the current state
    CurrentState = current_state(FsmPid),
    Event = get_next_event(DummyPid),
    UpdBlockCount = block_count(DummyPid),
    ?TESTMODULE:send_event(FsmPid, Event),
    execute_fsm(FsmState#fsm_state{block_count=UpdBlockCount},
                [CurrentState | States]).

block_count(WriterPid) ->
    WriterPid ! {block_count, self()},
    receive
        BlockCount ->
            BlockCount
    end.

current_state(Pid) ->
    ?TESTMODULE:current_state(Pid).

-endif.
