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

-module(riak_cs_get_fsm_pulse).

-ifdef(EQC).
-ifdef(PULSE).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").


-include_lib("pulse/include/pulse.hrl").
-compile({parse_transform, pulse_instrument}).

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
%% ===================================================================
%% Eunit Test
%% ===================================================================

eqc_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 60,
         ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_blocks_in_order()))))}
       ]
      }
     ]
    }.

%% ===================================================================
%% Helper Funcs
%% ===================================================================

setup() ->
    pulse:start(),
    application:load(sasl),
    application:load(riak_cs),
    application:set_env(sasl, sasl_error_logger, {file, "cs_get_fsm_sasl.log"}),
    error_logger:tty(false),
    application:start(lager).

cleanup(_) ->
    ok.

calc_block_size(ContentLength, NumBlocks) ->
    Quotient = ContentLength div NumBlocks,
    case ContentLength rem NumBlocks of
        0 ->
            Quotient;
        _ ->
            Quotient + 1
    end.

test_n_chunks_builder(BlockSize, NumBlocks, FetchConcurrency, BufferFactor) ->
    fun () ->
            ContentLength = BlockSize * NumBlocks,
            application:set_env(riak_cs, lfs_block_size, BlockSize),
            {ok, Pid} = riak_cs_get_fsm:test_link(<<"bucket">>, <<"key">>,
                                                  ContentLength, BlockSize,
                                                  FetchConcurrency,
                                                 BufferFactor),
            Manifest = riak_cs_get_fsm:get_manifest(Pid),
            ?assertEqual(ContentLength, Manifest?MANIFEST.content_length),
            riak_cs_get_fsm:continue(Pid, {0, ContentLength - 1}),
            try
                expect_n_bytes(Pid, NumBlocks, ContentLength)
            after
                riak_cs_get_fsm:stop(Pid)
            end
    end.

expect_n_bytes(FsmPid, N, Bytes) ->
    {done, Res} = lists:foldl(
                    fun(_, {done, _} = Acc) ->
                            Acc;
                       (_, {working, L}) ->
                            case riak_cs_get_fsm:get_next_chunk(FsmPid) of
                                {chunk, X} ->
                                    {working, [X|L]};
                                {done, <<>>} ->
                                    {done, L}
                            end
                    end, {working, []}, lists:seq(1, N + 1)),
    ?assertEqual({N, Bytes}, {N, byte_size(iolist_to_binary(Res))}),

    %% dummy reader uses little endian to encode the sequence number
    %% in each chunk ... pull that seq num out, then check that usort
    %% yields the same thing.
    FirstBytes = lists:reverse([begin <<X:32/little, _/binary>> = Bin, X end ||
                                   Bin <- Res]),
    USorted = lists:usort(FirstBytes),
    ?assertMatch(FirstBytes, USorted).

%% ===================================================================
%% EQC Properties
%% ===================================================================

run_n_chunks(BlockSize, NumBlocks, FetchConcurrency, BufferFactor) ->
    (test_n_chunks_builder(BlockSize, NumBlocks, FetchConcurrency, BufferFactor))().

prop_blocks_in_order() ->
    ?FORALL({BlockSize, NumBlocks, FetchConcurrency, BufferFactor},
            {block_size(), num_blocks(), fetch_concurrency(), buffer_factor()},
            ?PULSE(Result,
                   run_n_chunks(BlockSize, NumBlocks,
                                FetchConcurrency, BufferFactor),
                   ?WHENFAIL(io:format("~p /= ~p~n", [Result, ok]),
                             Result == ok))).

%% ===================================================================
%% Generators
%% ===================================================================

block_size() ->
    %% lower bound is `4' because we treat binaries
    %% as integers, and need at least 4 bytes.
    choose(4, 64).

num_blocks() ->
    choose(1, 64).

fetch_concurrency() ->
    choose(1, 16).

buffer_factor() ->
    frequency([{1, 1}, {3, choose(2, 64)}]).

%% ===================================================================
%% Test Helpers
%% ===================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_blocks_in_order())).

-endif. %% PULSE
-endif. %% EQC
