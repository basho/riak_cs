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

%% @doc Quickcheck test module for `riak_cs_acl_utils'.

-module(riak_cs_acl_utils_eqc).
-compile(export_all).

-include("riak_cs.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_add_grant/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 1000).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 60, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_add_grant()))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_add_grant() ->
    ?FORALL({Grants, RandomGrant}, ?LET(Grants, non_empty(list(grant())),
                                        {Grants, elements(Grants)}),
            begin
                CombinedGrants = lists:foldl(fun riak_cs_acl_utils:add_grant/2, [], Grants),
                lists:sort(CombinedGrants) =:= lists:sort(riak_cs_acl_utils:add_grant(RandomGrant, CombinedGrants))
            end).

%%====================================================================
%% Generators
%%====================================================================

grantee() ->
    elements([{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}]).

permission() ->
    elements(['READ', 'WRITE', 'READ_ACP', 'WRITE_ACP', 'FULL_CONTROL']).

grant() ->
    {grantee(), [permission()]}.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_add_grant())).

-endif.
