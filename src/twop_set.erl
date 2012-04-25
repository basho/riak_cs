%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module for choosing and manipulating lists (well, orddict) of manifests

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

-endif.
