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
-export([prop_reverse/0]).

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
                ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, prop_reverse())))}
        ]
    }.

%% ====================================================================
%% eqc property
%% ====================================================================

prop_reverse() ->
    ?FORALL(Xs,list(int()),
        lists:reverse(lists:reverse(Xs)) == Xs).

%%====================================================================
%% Generators
%%====================================================================

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_reverse())).

-endif.
