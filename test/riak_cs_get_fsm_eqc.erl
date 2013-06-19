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

%% @doc Quickcheck test module for `riak_cs_get_fsm'.

-module(riak_cs_get_fsm_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-compile(export_all).
-export([test/0, test/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% eqc property
-export([prop_get_fsm/0]).

%% States
-export([start/1,
         waiting_chunk/1,
         stop/1]).

%% Helpers
%% TODO: export!
-export([]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 500).

-record(state, {fsm_pid :: pid(), %% real pid}
                content_length :: integer(), %% not symbolic
                total_blocks :: integer(), %% not symbolic
                last_chunk :: binary(), %% not symbolic
                counter=0 :: integer()}). %% not symbolic

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 30,
         ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(15, ?QC_OUT(prop_get_fsm()))))}
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
    eqc:quickcheck(eqc:numtests(Iterations, prop_get_fsm())).


%% ====================================================================
%% eqc property
%% ====================================================================

prop_get_fsm() ->
    application:set_env(riak_cs, lfs_block_size, 1048576),
    ?FORALL(State, #state{content_length=?LET(X, riak_cs_gen:bounded_content_length(), X * 10)},
        ?FORALL(Cmds, eqc_statem:more_commands(10, commands(?MODULE, {start, State})),
                begin
                    {H,{_F,_S},Res} = run_commands(?MODULE, Cmds),
                    ?WHENFAIL(io:format("history is ~p ~n", [[StateRecord#state{last_chunk=last_chunk_substitute} || {{_StateName, StateRecord}, _Results}<- H]]), equals(ok, Res))
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

stop_fsm() -> ok.

%%====================================================================
%% Helpers
%%====================================================================

check_chunk(Counter, Chunk) ->
    <<NewCounter:32/little, _/binary>> = Chunk,
    Counter == NewCounter.

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initial_state() ->
    {start, true}.

next_state_data(start, waiting_chunk, #state{content_length=ContentLength}=S, R, _C) ->
    BlockSize = riak_cs_lfs_utils:block_size(),
    BlockCount = riak_cs_lfs_utils:block_count(ContentLength, BlockSize),
    S#state{total_blocks=(BlockCount-1), fsm_pid=R};
next_state_data(waiting_chunk, waiting_chunk, #state{counter=Counter}=S, R, _C) ->
    S#state{counter=Counter+1,last_chunk=R};
next_state_data(_From, _To, S, _R, _C) ->
    S.

start(#state{content_length=ContentLength}) ->
    [{waiting_chunk, {call, ?MODULE, start_fsm, [ContentLength, riak_cs_lfs_utils:block_size()]}}].

waiting_chunk(#state{fsm_pid=Pid}) ->
    [{waiting_chunk, {call, ?MODULE, get_chunk, [Pid]}},
     {stop, {call, ?MODULE, stop_fsm, []}}].

stop(_S) ->
    [].

precondition(waiting_chunk,stop,#state{counter=Counter, total_blocks=TotalBlocks},_C) ->
    Counter == TotalBlocks;
precondition(waiting_chunk,waiting_chunk,#state{counter=Counter, total_blocks=TotalBlocks},_C) ->
    Counter < TotalBlocks;
precondition(_From,_To,_S,_C) ->
    true.

postcondition(waiting_chunk, waiting_chunk, #state{counter=Counter,last_chunk={done, Chunk},total_blocks=Counter}, _C, _R) ->
    check_chunk((Counter-1), Chunk);
postcondition(waiting_chunk, waiting_chunk, #state{counter=Counter,last_chunk={chunk, Chunk}}, _C, _R) ->
    check_chunk((Counter-1), Chunk);
postcondition(_From, _To, _S, _C, _R) ->
    true.

-endif.
