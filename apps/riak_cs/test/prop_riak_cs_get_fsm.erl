%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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

%% @doc PropEr test module for `riak_cs_get_fsm'.

-module(prop_riak_cs_get_fsm).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([test/0, test/1]).

%% proper_fsm callbacks
-export([initial_state/0,
         next_state_data/5,
         precondition/4,
         postcondition/5,
         start_fsm/2,
         stop_fsm/1]).

%% proper property
-export([prop_get_fsm/0]).

%% States
-export([start/1,
         waiting_chunk/1,
         stop/1,
         nop/0,
         get_chunk/1]).

-define(QC_OUT(P),
        on_output(fun(Str, Args) ->
                          io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 500).

-record(state, {fsm_pid        :: pid(),       %% real pid of riak_cs_get_fsm}
                content_length :: integer(),   %% not symbolic
                total_blocks   :: integer(),   %% not symbolic
                counter=0      :: integer()}). %% not symbolic

%%====================================================================
%% Eunit tests
%%====================================================================

proper_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60 * 10,
         ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_get_fsm())))}
       ]
      }
     ]
    }.

setup() ->
    ok.

cleanup(_) ->
    ok.

%% ====================================================================
%% Public API
%% ====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    proper:quickcheck(numtests(Iterations, prop_get_fsm())).


%% ====================================================================
%% proper property
%% ====================================================================

prop_get_fsm() ->
    application:set_env(riak_cs, lfs_block_size, 1048576),
    ?FORALL(State, #state{content_length = ?LET(X, riak_cs_gen:bounded_content_length(), X * 10)},
            ?FORALL(Cmds, proper_statem:more_commands(10, proper_fsm:commands(?MODULE, {start, State})),
                    begin
                        {H, {_F, FinalState}, Res} = proper_fsm:run_commands(?MODULE, Cmds),
                        #state{fsm_pid=FsmPid, content_length=ContentLength,
                               total_blocks=TotalBlocks, counter=Counter} = FinalState,
                        stop_fsm(FsmPid),
                        %% Collect stats how much percentages of blocks are consumed.
                        ConsumedPercentage =
                            case TotalBlocks of
                                undefined -> no_consumption;
                                _ -> min(100, trunc(100*(Counter)/TotalBlocks))
                            end,
                        collect(with_title(consumed_percentage), ConsumedPercentage,
                                collect(with_title(content_length_mm), ContentLength/1000000,
                                        collect(with_title(command_length), length(Cmds),
                                                ?WHENFAIL(io:format("history is ~p ~n", [[S || {S, _Rs} <- H]]),
                                                          equals(ok, Res)))))
                    end)).

%%====================================================================
%% Generators
%%====================================================================

start_fsm(ContentLength, BlockSize) ->
    %% these should probably turn into inputs for the EQC test
    FetchConcurrency = 2,
    BufferFactor = 32,
    {ok, FSMPid} = riak_cs_get_fsm:test_link(<<"bucket">>, <<"key">>,
                                             ContentLength, BlockSize,
                                             FetchConcurrency,
                                             BufferFactor),
    _Manifest = riak_cs_get_fsm:get_manifest(FSMPid),
    riak_cs_get_fsm:continue(FSMPid, {0, ContentLength-1}),
    FSMPid.

get_chunk(FSMPid) ->
    riak_cs_get_fsm:get_next_chunk(FSMPid).

stop_fsm(FsmPid) when is_pid(FsmPid) ->
    riak_cs_get_fsm:stop(FsmPid);
%% fsm is not spawned when length of history is zero or one
stop_fsm(_FsmPid) ->
    ok.

%%====================================================================
%% proper_fsm callbacks
%%====================================================================

initial_state() ->
    error(not_used).

next_state_data(start, waiting_chunk, #state{content_length=ContentLength}=S, R, _C) ->
    BlockCount = riak_cs_lfs_utils:block_count(ContentLength, block_size()),
    S#state{total_blocks=BlockCount, fsm_pid=R};
next_state_data(waiting_chunk, waiting_chunk, #state{counter=Counter}=S, _R, _C) ->
    S#state{counter=Counter+1};
next_state_data(waiting_chunk, stop, S, _R, _C) ->
    S;
next_state_data(stop, stop, S, _R, _C) ->
    S.


start(#state{content_length=ContentLength}) ->
    [{waiting_chunk, {call, ?MODULE, start_fsm, [ContentLength, block_size()]}}].

waiting_chunk(#state{fsm_pid=Pid}) ->
    [{waiting_chunk, {call, ?MODULE, get_chunk, [Pid]}},
     {stop, {call, ?MODULE, stop_fsm, [Pid]}}].

stop(_S) ->
    [{history, {call, ?MODULE, nop, []}}].
nop() ->
    true.

precondition(start, waiting_chunk, _S, _C) ->
    true;
precondition(waiting_chunk, waiting_chunk,
             #state{counter=Counter, total_blocks=TotalBlocks}, _C) ->
    Counter =< TotalBlocks;
precondition(waiting_chunk, stop,
             #state{counter=Counter, total_blocks=TotalBlocks}, _C) ->
    Counter =:= TotalBlocks + 1;
precondition(stop, stop, _S, _C) ->
    true.


postcondition(start, waiting_chunk, _State, _C, _R) ->
    true;
postcondition(waiting_chunk, waiting_chunk, #state{counter=Counter}, _C, Response) ->
    validate_waiting_chunk_response(Counter, Response);
postcondition(waiting_chunk, stop, #state{counter=Counter, total_blocks=TotalBlocks}, _C, _R) ->
    Counter =:= TotalBlocks + 1;
postcondition(stop, stop, _, _, _) ->
    true.


%%====================================================================
%% Helpers
%%====================================================================

validate_waiting_chunk_response(Counter, {chunk, Chunk}) ->
    <<CounterInChunk:32/little, _/binary>> = Chunk,
    Counter =:= CounterInChunk;
validate_waiting_chunk_response(_Counter, {done, Chunk}) ->
    Chunk =:= <<>>.

block_size() ->
    riak_cs_lfs_utils:block_size().
