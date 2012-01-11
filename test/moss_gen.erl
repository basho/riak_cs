%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Common QuickCheck generators for Riak Moss

-module(moss_gen).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").

%% Generators
-export([bucket/0,
         file_name/0,
         block_size/0,
         content_length/0]).

%%====================================================================
%% Generators
%%====================================================================

bucket() ->
    non_blank_string().

file_name() ->
    non_blank_string().

block_size() ->
    elements([bs(El) || El <- [4, 8, 16, 32, 64]]).

content_length() ->
    ?LET(X, large_non_zero_nums(), abs(X)).

%%====================================================================
%% Helpers
%%====================================================================

large_non_zero_nums() ->
    ?SUCHTHAT(X, largeint(), X =/= 0).

non_blank_string() ->
    ?LET(X,not_empty(list(lower_char())), list_to_binary(X)).

%% Generate a lower 7-bit ACSII character that should not cause any problems
%% with utf8 conversion.
lower_char() ->
    choose(16#20, 16#7f).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

bs(Power) ->
    trunc(math:pow(2, Power)).

-endif.
