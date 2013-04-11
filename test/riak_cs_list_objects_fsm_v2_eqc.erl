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
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc properties
-export([prop_skip_past_prefix_and_delimiter/0]).

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
    ?assert(quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_skip_past_prefix_and_delimiter())))).

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_skip_past_prefix_and_delimiter() ->
    ?FORALL(B, binary(),
            bool_prop(B)).

bool_prop(<<>>=B) ->
    B < riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(B);
bool_prop(Binary) ->
    case binary:last(Binary) of
        255 ->
            Binary == riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(Binary);
        _Else ->
            Binary < riak_cs_list_objects_fsm_v2:skip_past_prefix_and_delimiter(Binary)
    end.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_skip_past_prefix_and_delimiter())).

-endif.
