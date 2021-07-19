%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(prop_riak_cs_manifest_resolution).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_cs.hrl").

%% proper property
-export([prop_resolution_commutative/0]).

%% helpers
-export([test/0, test/1]).

-define(TEST_ITERATIONS, 500).
-define(QC_OUT(P),
    on_output(fun(Str, Args) ->
                      io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

proper_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 300,
            ?_assertEqual(true, proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT((prop_resolution_commutative())))))}
       ]
      }
     ]
    }.

setup() ->
    ok.

cleanup(_) ->
    ok.


%% ====================================================================
%% PropEr property
%% ====================================================================

prop_resolution_commutative() ->
    ?FORALL(Manifests, resize(50, manifests()),
        begin
            MapFun = fun(Mani) ->
                riak_cs_manifest_utils:new_dict(Mani?MANIFEST.uuid, Mani)
            end,
            Filtered = only_one_active(Manifests),
            WrappedManifests = lists:map(MapFun, Filtered),
            Resolved = riak_cs_manifest_resolution:resolve(WrappedManifests),
            ReversedResolved = riak_cs_manifest_resolution:resolve(lists:reverse(WrappedManifests)),
            Resolved == ReversedResolved
        end).

%%====================================================================
%% Generators
%%====================================================================

raw_manifest() ->
    ?MANIFEST{uuid=riak_cs_gen:bounded_uuid(),
                     bkey={<<"bucket">>, <<"key">>},
                     state=riak_cs_gen:manifest_state()}.

manifest() ->
    ?LET(Manifest, raw_manifest(), process_manifest(Manifest)).

process_manifest(Manifest=?MANIFEST{state=State}) ->
    case State of
        writing ->
            Manifest?MANIFEST{last_block_written_time=erlang:timestamp(),
                                     write_blocks_remaining=blocks_set()};
        active ->
            %% this clause isn't
            %% needed but it makes
            %% things more clear imho
            Manifest?MANIFEST{last_block_deleted_time=erlang:timestamp()};
        pending_delete ->
            Manifest?MANIFEST{last_block_deleted_time=erlang:timestamp(),
                                     delete_blocks_remaining=blocks_set()};
        scheduled_delete ->
            Manifest?MANIFEST{last_block_deleted_time=erlang:timestamp(),
                                     delete_blocks_remaining=blocks_set()};
        deleted ->
            Manifest?MANIFEST{last_block_deleted_time=erlang:timestamp()}
    end.

manifests() ->
    list(manifest()).

blocks_set() ->
    ?LET(L, list(int()), ordsets:from_list(L)).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    proper:quickcheck(numtests(Iterations, prop_resolution_commutative())).

only_one_active(Manifests) ->
    {_, FilteredManifests} = lists:foldl(fun only_one_active_helper/2, {not_found, []}, Manifests),
    FilteredManifests.

only_one_active_helper(?MANIFEST{state=active}, {found, List}) ->
    {found, List};
only_one_active_helper(Manifest, {found, List}) ->
    {found, [Manifest | List]};
only_one_active_helper(Manifest=?MANIFEST{state=active}, {not_found, List}) ->
    {found, [Manifest | List]};
only_one_active_helper(Manifest, {not_found, List}) ->
    {not_found, [Manifest | List]}.
