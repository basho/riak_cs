%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(basho_bench_driver_moss).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {}).

-spec new(integer()) -> {ok, term()}.
new(_ID) ->
    {ok, #state{}}.

-spec run(atom(), fun(), fun(), term()) -> {ok, term()}.
run(_Operation, _KeyGen, _ValueGen, State) ->
    {ok, State}.
