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

-module(riak_cs_list_objects_fsm_eqc).

-ifdef(EQC).

-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_index_of_first_greater_element/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 500).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test() ->
    ?assert(quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_index_of_first_greater_element())))).

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_index_of_first_greater_element() ->
    ?FORALL({L, E}, list_and_element(),
      begin
            Index = riak_cs_list_objects_fsm:index_of_first_greater_element(L, E),
            index_of_first_greater_element_correct(L, Index, E)
      end).

index_of_first_greater_element_correct([], 1, _Elem) ->
    true;
index_of_first_greater_element_correct([H], 1, Elem) ->
    Elem =< H;
index_of_first_greater_element_correct([H], 2, Elem) ->
    Elem >= H;
index_of_first_greater_element_correct([H | _Tail], 1, Elem) ->
    Elem =< H;
index_of_first_greater_element_correct(List, Index, Elem)
        when Index > length(List) ->
    Elem >= lists:last(List);
index_of_first_greater_element_correct(List, Index, Elem) ->
    Elem =< lists:nth(Index, List).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_index_of_first_greater_element())).


%%====================================================================
%% Generators
%%====================================================================

%% Return a tuple of {A, B}. A is a sorted list of non-negative numbers,
%% where elements only appear once. B is either a member of that list,
%% or isn't, with some likelihood near 50%.
list_and_element() ->
    ?LET(L, sorted_unique(), {L, element_or_not(L)}).

%% Given a list, return a non-negative int if the list is empty,
%% otherwise, return an int that is either in the list or not, with some
%% likelihood.
element_or_not([]) ->
    positive_int();
element_or_not(List) ->
    oneof([positive_int(), elements(List)]).

positive_int() ->
    ?LET(I, int(), abs(I)).

sorted_unique() ->
    ?LET(L, list(positive_int()), lists:usort(L)).

-endif.
