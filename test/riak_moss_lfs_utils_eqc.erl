%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_lfs_utils'.

-module(riak_moss_lfs_utils_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_block_count/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 500).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
        [%% Run the quickcheck tests
            {timeout, 60,
                ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, prop_block_count())))}
        ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_block_count() ->
    ?FORALL({CLength, BlockSize},{block_size(), content_length()},
        begin
            NumBlocks = riak_moss_lfs_utils:block_count(CLength, BlockSize),
            (NumBlocks * BlockSize) >= CLength
        end).

%%====================================================================
%% Generators
%%====================================================================

block_size() ->
    elements([bs(El) || El <- [4, 8, 16, 32, 64]]).

content_length() ->
    ?LET(X, large_non_zero_nums(), abs(X)).

large_non_zero_nums() ->
    ?SUCHTHAT(X, largeint(), X =/= 0).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_block_count())).

bs(Power) ->
    trunc(math:pow(2, Power)).

-endif.
