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

%% @doc twop_set_eqc: Quickcheck testing for the `twop_set' module.

-module(twop_set_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_twop_set_api/0,
         prop_twop_set_resolution/0]).

%% States
-export([stopped/1,
         running/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 500).
-define(SET_MODULE, twop_set).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-ifdef(namespaced_types).
-type stdlib_set() :: sets:set().
-else.
-type stdlib_set() :: set().
-endif.

-record(eqc_state, {adds=sets:new() :: stdlib_set(),
                    deletes=sets:new() :: stdlib_set(),
                    operation_count=0 :: non_neg_integer(),
                    operation_limit=500 :: pos_integer(),
                    set :: twop_set:twop_set(),
                    size=0 :: non_neg_integer()}).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
        [
            {timeout, 60, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_twop_set_api()))))},
            {timeout, 60, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_twop_set_resolution()))))}
        ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_twop_set_api() ->
    ?FORALL(Cmds,
            commands(?MODULE),
            begin
                {H, {_F, S}, Res} = run_commands(?MODULE, Cmds),
                aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 ?debugFmt("Cmds: ~p~n",
                                           [zip(state_names(H),
                                                command_names(Cmds))]),
                                 ?debugFmt("Result: ~p~n", [Res]),
                                 ?debugFmt("History: ~p~n", [H]),
                                 ?debugFmt("Current expected size: ~p~n", [S#eqc_state.size]),
                                 ?debugFmt("Current actual size: ~p~n", [twop_set:size(S#eqc_state.set)]),
                                 ?debugFmt("Operation count: ~p~n", [S#eqc_state.operation_count]),
                                 ?debugFmt("Operation limit: ~p~n", [S#eqc_state.operation_limit]),
                                 ?debugFmt("Adds: ~p~n", [sets:to_list(S#eqc_state.adds)]),
                                 ?debugFmt("Deletes: ~p~n", [sets:to_list(S#eqc_state.deletes)]),
                                 ?debugFmt("Set: ~p~n", [twop_set:to_list(S#eqc_state.set)])
                             end,
                             equals(ok, Res)))
            end
           ).

prop_twop_set_resolution() ->
    ?FORALL(Siblings,
            siblings(),
            ?TRAPEXIT(
               begin
                   SiblingSets = sibling_sets(Siblings),
                   {ExpectedAdds, ExpectedDels} = expected_adds_and_dels(Siblings),
                   Res = twop_set:resolve(SiblingSets),
                   Adds = twop_set:adds(Res),
                   Dels = twop_set:dels(Res),
                   ?WHENFAIL(
                      begin
                          ?debugFmt("Adds: ~p~n", [sets:to_list(Adds)]),
                          ?debugFmt("Deletes: ~p~n", [sets:to_list(Dels)]),
                          ?debugFmt("Expected Adds: ~p~n", [sets:to_list(ExpectedAdds)]),
                          ?debugFmt("Expected Deletes: ~p~n", [sets:to_list(ExpectedDels)])
                      end,
                      conjunction(
                        [
                         {adds, equals(lists:sort(sets:to_list(Adds)),
                                        lists:sort(sets:to_list(ExpectedAdds)))},
                         {deletes, equals(lists:sort(sets:to_list(Dels)),
                                        lists:sort(sets:to_list(ExpectedDels)))}
                        ]))
               end
              )).

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

stopped(_S) ->
    [{running, {call, ?SET_MODULE, new, []}}].

running(#eqc_state{operation_count=OpCount,
                   operation_limit=OpLimit,
                   set=Set}) ->
    [{stopped, {call, ?SET_MODULE, size, [Set]}} || OpCount > OpLimit] ++
    [{history, {call, ?SET_MODULE, size, [Set]}},
     {history, {call, ?SET_MODULE, to_list, [Set]}},
     {history, {call, ?SET_MODULE, is_element, [int(), Set]}},
     {history, {call, ?SET_MODULE, add_element, [int(), Set]}},
     {history, {call, ?SET_MODULE, del_element, [int(), Set]}}
    ].

initial_state() ->
    stopped.

initial_state_data() ->
    #eqc_state{}.

next_state_data(running, stopped, S, _R, _C) ->
    S#eqc_state{adds=sets:new(),
                deletes=sets:new(),
                operation_count=0,
                set=undefined,
                size=0};
next_state_data(stopped, running, S, R, {call, _M, new, _}) ->
    S#eqc_state{set=R};
next_state_data(_From, _To, S, R, {call, _M, add_element, [Element, _Set]}) ->
    Adds = S#eqc_state.adds,
    Dels = S#eqc_state.deletes,
    Size = S#eqc_state.size,
    OpCount = S#eqc_state.operation_count,
    case sets:is_element(Element, Adds)
        orelse
        sets:is_element(Element, Dels) of
        true ->
            UpdAdds = Adds,
            UpdSize = Size;
        false ->
            UpdAdds = sets:add_element(Element, Adds),
            UpdSize = Size + 1
    end,
    S#eqc_state{adds=UpdAdds,
                operation_count=OpCount+1,
                set=R,
                size=UpdSize};
next_state_data(_From, _To, S, R, {call, _M, del_element, [Element, _Set]}) ->
    Adds = S#eqc_state.adds,
    Dels = S#eqc_state.deletes,
    Size = S#eqc_state.size,
    OpCount = S#eqc_state.operation_count,
    case sets:is_element(Element, Dels) of
        true ->
            UpdDels = Dels,
            UpdSize = Size;
        false ->
            UpdDels = sets:add_element(Element, Dels),
            case Size > 0
                andalso
                sets:is_element(Element, Adds) of
                true ->
                    UpdSize = Size - 1;
                false ->
                    UpdSize = Size
            end
    end,
    S#eqc_state{deletes=UpdDels,
                operation_count=OpCount+1,
                set=R,
                size=UpdSize};
next_state_data(_From, _To, S, _R, _C) ->
    OpCount = S#eqc_state.operation_count,
    S#eqc_state{operation_count=OpCount+1}.

precondition(_From, _To, _S, _C) ->
    true.

postcondition(_From, _To, #eqc_state{size=Size} ,{call, _M, size, _}, R) ->
    R =:= Size;
postcondition(_From, _To, S, {call, _M, to_list, _}, R) ->
    #eqc_state{adds=Adds,
               deletes=Dels} = S,
    R =:= sets:to_list(sets:subtract(Adds, Dels));
postcondition(_From, _To, S, {call, _M, is_element, [Element, _Set]}, R) ->
    #eqc_state{adds=Adds,
               deletes=Dels} = S,
    (sets:is_element(Element, Adds)
     andalso
     not sets:is_element(Element, Dels)) =:= R;
postcondition(_From, _To, S, {call, _M, add_element, [Element, Set]}, R) ->
    #eqc_state{adds=Adds,
               deletes=Dels} = S,
    ResultContainsElement = sets:is_element(Element, twop_set:adds(R)),
    ShouldContainElement = not sets:is_element(Element, Dels),
    case sets:is_element(Element, Adds)
        orelse
        sets:is_element(Element, Dels) of
        true ->
            ExpectedGrowth = twop_set:size(R) =:= twop_set:size(Set);
        false ->
            ExpectedGrowth = twop_set:size(R) =:= (twop_set:size(Set) + 1)
    end,
    ResultContainsElement =:= ShouldContainElement andalso ExpectedGrowth;
postcondition(_From, _To, _S, {call, _M, del_element, [Element, _Set]}, R) ->
    sets:is_element(Element, twop_set:dels(R))
        andalso
        not sets:is_element(Element, twop_set:adds(R));
postcondition(_From, _To, _S, _C, _R) ->
    true.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_twop_set_api())).

-spec sibling_sets([{[term()], [term()]}]) -> [twop_set:twop_set()].
sibling_sets(Siblings) ->
    [list_pair_to_twop_set(Sibling) || Sibling <- Siblings].

-spec list_pair_to_twop_set({[term()], [term()]}) -> twop_set:twop_set().
list_pair_to_twop_set({AddList, DelList}) ->
    {sets:from_list(AddList), sets:from_list(DelList)}.

%% @doc Take the raw lists from the `siblings' generator and generate
%% the expected sets of adds and deletes for the `twop_set'. It would
%% be more direct to use the result of a call to `sibling_sets', but
%% this way the resulting set is arrived at by a different manner than
%% that used by the module under test.
-spec expected_adds_and_dels([{[term()], [term()]}]) -> twop_set:twop_set().
expected_adds_and_dels(Siblings) ->
    {Adds, Dels} = lists:unzip(Siblings),
    DelSet = sets:from_list(lists:flatten(Dels)),
    AddSet = sets:subtract(sets:from_list(lists:flatten(Adds)), DelSet),
    {AddSet, DelSet}.

%%====================================================================
%% Generators
%%====================================================================

siblings() ->
    list({list(int()), list(int())}).

-endif.
