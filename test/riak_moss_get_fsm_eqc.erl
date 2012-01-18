%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_get_fsm'.

-module(riak_moss_get_fsm_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-compile(export_all).
-export([test/0, test/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% eqc property
-export([prop_get_fsm/0]).

%% States
-export([state_name/1]).

%% Helpers
-export([]).

-define(TEST_ITERATIONS, 500).

-record(state, {}).

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
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H,{_F,S},Res} = run_commands(?MODULE, Cmds),
                equals(ok, Res)
            end).

%%====================================================================
%% Generators
%%====================================================================

%%====================================================================
%% Helpers
%%====================================================================

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initial_state() ->
    {stopped, true}.

initial_state_data() ->
    ok.

next_state_data(_From, _To, S, _R, _C) ->
    S.

state_name(_S) ->
    [].

precondition(_From,_To,_S,_C) ->
    true.

postcondition(_From, _To, _S, _C, _R) ->
    true.

-endif.

