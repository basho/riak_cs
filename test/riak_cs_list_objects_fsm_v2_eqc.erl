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

-module(riak_cs_list_objects_fsm_v2_eqc).

-ifdef(EQC).

-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_skip_past_prefix_and_delimiter/0,
         prop_prefix_must_be_in_between/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    [?_assert(quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_skip_past_prefix_and_delimiter())))),
     ?_assert(quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_prefix_must_be_in_between()))))].

%% ====================================================================
%% EQC Properties
%% ====================================================================

%% @doc Forall non-empty binaries `B',
%% `B < skip_past_prefix_and_delimiter(B)'.
%% Unless the last byte in `B' is 255, then they should be equal
prop_skip_past_prefix_and_delimiter() ->
    ?FORALL(B, non_empty_binary(),
            less_than_prop(B)).

less_than_prop(Binary) ->
    case binary:last(Binary) of
        255 ->
            Binary == riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(Binary);
        _Else ->
            Binary < riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(Binary)
    end.

%% ====================================================================

%% Forall binaries `A' and `B',
%% concatenating `B' to `A' (A + B) should sort inbetween
%% `A' and `skip_past_prefix_and_delimiter(A)'
prop_prefix_must_be_in_between() ->
    ?FORALL({A, B}, {non_empty_binary(), non_empty_binary()},
            ?IMPLIES(no_max_byte(A), bool_in_between(A, B))).

no_max_byte(<<255:8/integer>>) ->
    false;
no_max_byte(Binary) ->
    255 =/= binary:last(Binary).

bool_in_between(A, B) ->
    Between = <<A/binary, B/binary>>,
    (A < Between) andalso (Between < riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(A)).

%%====================================================================
%% Generators
%%====================================================================

non_empty_binary() ->
    ?SUCHTHAT(B, binary(), B =/= <<>>).

%%====================================================================
%% Test Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_skip_past_prefix_and_delimiter())),
    eqc:quickcheck(eqc:numtests(Iterations, prop_prefix_must_be_in_between())).

-endif.
