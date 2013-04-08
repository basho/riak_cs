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

-module(riak_cs_manifest_utils_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_cs_core/include/riak_cs.hrl").

-compile(export_all).

%% eqc property
-export([prop_choose_active_commutative/0]).

%% helpers
-export([test/0, test/1]).

-define(TEST_ITERATIONS, 500).
-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Run the quickcheck tests
        {timeout, 300,
            ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT((prop_choose_active_commutative())))))}
       ]
      }
     ]
    }.

setup() ->
    ok.

cleanup(_) ->
    ok.


%% ====================================================================
%% eqc property
%% ====================================================================

prop_choose_active_commutative() ->
    ?FORALL(Manifests, eqc_gen:resize(50, manifests()),
        begin
            AlteredManifests = lists:map(fun(M) -> M?MANIFEST{uuid=druuid:v4()} end, Manifests),
            AsDict = orddict:from_list([{M?MANIFEST.uuid, M} || M <- AlteredManifests]),
            Active = riak_cs_manifest_utils:active_manifest(AsDict),
            case Active of
                {error, no_active_manifest} ->
                    %% if no manifest is returned then there should
                    %% not be an active manifest in the list _at_ _all_
                    [] == lists:filter(fun(?MANIFEST{state=State}) ->
                                State == active end,
                            Manifests);
                {ok, AMani} ->
                    %% if a manifest is returned,
                    %% its state should be active
                    AMani?MANIFEST.state == active
            end
        end).

%%====================================================================
%% Generators
%%====================================================================

raw_manifest() ->
    ?MANIFEST{uuid = <<"this-uuid-will-be-replaced-later">>,
                     bkey={<<"bucket">>, <<"key">>},
                     state=riak_cs_gen:manifest_state()}.

manifest() ->
    ?LET(Manifest, raw_manifest(), process_manifest(Manifest)).

process_manifest(Manifest=?MANIFEST{state=State}) ->
    case State of
        writing ->
            Manifest?MANIFEST{last_block_written_time=erlang:now(),
                                     write_blocks_remaining=blocks_set()};
        active ->
            %% this clause isn't
            %% needed but it makes
            %% things more clear imho
            Manifest?MANIFEST{last_block_deleted_time=erlang:now()};
        pending_delete ->
            Manifest?MANIFEST{last_block_deleted_time=erlang:now(),
                                     delete_blocks_remaining=blocks_set()};
        scheduled_delete ->
            Manifest?MANIFEST{delete_marked_time=erlang:now()}
    end.

manifests() ->
    eqc_gen:list(manifest()).

blocks_set() ->
    ?LET(L, eqc_gen:list(int()), ordsets:from_list(L)).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_choose_active_commutative())).

-endif. %EQC
