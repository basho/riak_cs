%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

%% @doc PropErtest module for `riak_cs_acl_utils'.

-module(prop_riak_cs_acl_utils).

-include("riak_cs.hrl").

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_add_grant_idempotent/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        proper:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 1000).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 60, ?_assertEqual(true, proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_add_grant_idempotent()))))},
      {timeout, 60, ?_assertEqual(true, proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_grant_gives_permission()))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_add_grant_idempotent() ->
    %% For all grants, adding the same grant twice with `add_grant'
    %% should be idempotent.
    ?FORALL({Grants, RandomGrant}, ?LET(Grants, non_empty(list(grant())),
                                        {Grants, elements(Grants)}),
            begin
                CombinedGrants = lists:foldl(fun riak_cs_acl_utils:add_grant/2, [], Grants),
                lists:sort(CombinedGrants) =:= lists:sort(riak_cs_acl_utils:add_grant(RandomGrant, CombinedGrants))
            end).

prop_grant_gives_permission() ->
    ?FORALL({Grants, RandomGrant}, ?LET(Grants, non_empty(list(grant())),
                                        {Grants, elements(Grants)}),
            begin
                CombinedGrants = lists:foldl(fun riak_cs_acl_utils:add_grant/2, [], Grants),
                case RandomGrant of
                    ?ACL_GRANT{grantee = #{canonical_id := CanonicalID},
                               perms = [RequestedAccess]} ->
                        riak_cs_acl:has_permission(CombinedGrants, RequestedAccess, CanonicalID);
                    ?ACL_GRANT{perms = [RequestedAccess]} ->
                        riak_cs_acl:has_group_permission(CombinedGrants, RequestedAccess)
                end
            end).


%%====================================================================
%% Generators
%%====================================================================

grantee() ->
    oneof([owner(), group_grant()]).
owner() ->
    #{display_name => riak_cs_aws_utils:make_id(4),
      canonical_id => riak_cs_aws_utils:make_id(8),
      email => iolist_to_binary([riak_cs_aws_utils:make_id(2), $@, riak_cs_aws_utils:make_id(4), ".com"])}.
group_grant() ->
    oneof(['AllUsers', 'AuthUsers']).

permission() ->
    elements(['READ', 'WRITE', 'READ_ACP', 'WRITE_ACP', 'FULL_CONTROL']).

grant() ->
    ?ACL_GRANT{grantee = grantee(),
               perms = [permission()]}.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(?TEST_ITERATIONS).

test(Iterations) ->
    proper:quickcheck(proper:numtests(Iterations, prop_add_grant_idempotent())),
    proper:quickcheck(proper:numtests(Iterations, prop_grant_gives_permission())).
