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

%% @doc This module implements the two-phase CRDT set.
%% This implementation has one notable difference from
%% the Comprehensive CRDT Paper [1]. The CRDT paper
%% has the precondition that deletes are only allowed
%% if the element is present in the local replica
%% of the set already. This implementation does not
%% have that restriction. This can lead to a situation
%% where an item is added, but that item has been previously
%% deleted and it not visible.
%% For Riak CS, we opt for this implementation because
%% we _only_ delete items that we have previously observed
%% in another replica of the same set. This allows
%% to be sure that the item is only missing in our
%% local replica because it hasn't been replicated
%% to it yet.
%% [1]: [http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf]

-module(twop_set).

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([
         new/0,
         size/1,
         to_list/1,
         is_element/2,
         add_element/2,
         del_element/2,
         resolve/1
        ]).

-ifdef(namespaced_types).
-type stdlib_set() :: sets:set().
-else.
-type stdlib_set() :: set().
-endif.

-type twop_set() :: {stdlib_set(), stdlib_set()}.
-export_type([twop_set/0]).

%%%===================================================================
%%% API
%%%===================================================================

new() ->
    {sets:new(), sets:new()}.

%% not implementing is_set

size(Set) ->
    sets:size(minus_deletes(Set)).

to_list(Set) ->
    sets:to_list(minus_deletes(Set)).

is_element(Element, Set) ->
    sets:is_element(Element, minus_deletes(Set)).

add_element(Element, Set={Adds,Dels}) ->
    case sets:is_element(Element, Dels) of
        true ->
            %% this element
            %% has already been added
            %% and deleted. It can't be
            %% added back.
            Set;
        false ->
            {sets:add_element(Element, Adds),
             Dels}
    end.

del_element(Element, {Adds, Dels}) ->
    {sets:del_element(Element, Adds),
     sets:add_element(Element, Dels)}.


%% CRDT Funs =========================================================
resolve(Siblings) ->
    FoldFun = fun({A_Adds, A_Dels}, {B_Adds, B_Dels}) ->
            DelsUnion = sets:union(A_Dels, B_Dels),
            AddsUnion = sets:union(A_Adds, B_Adds),
            Adds_Minus_Dels = sets:subtract(AddsUnion, DelsUnion),

            {Adds_Minus_Dels, DelsUnion} end,

    lists:foldl(FoldFun, new(), Siblings).

%%%===================================================================
%%% Internal functions
%%%===================================================================

minus_deletes({Adds, Dels}) ->
    sets:subtract(Adds, Dels).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

empty_resolve_test() ->
    ?assertEqual(new(), resolve([])).

item_shows_up_test() ->
    Set = add_element(foo, new()),
    ?assert(is_element(foo, Set)).

item_is_deleted_test() ->
    Set = add_element(foo, new()),
    Set2 = del_element(foo, Set),
    ?assertNot(is_element(foo, Set2)).

resolution_test() ->
    O = new(),
    WithFoo = add_element(foo, O),
    WithOutFoo = del_element(foo, O),
    WithBar = add_element(bar, O),
    Resolved = resolve([WithBar, WithFoo, WithOutFoo]),
    ?assert(is_element(bar, Resolved)),
    ?assertNot(is_element(foo, Resolved)).

%%%===================================================================
%%% Test API
%%%===================================================================

-spec adds(twop_set()) -> stdlib_set().
adds({Adds, _}) ->
    Adds.

-spec dels(twop_set()) -> stdlib_set().
dels({_, Dels}) ->
    Dels.

-endif.
