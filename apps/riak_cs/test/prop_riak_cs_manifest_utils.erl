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

-module(prop_riak_cs_manifest_utils).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_cs.hrl").

%% proper property
-export([prop_active_manifests/0]).

%% helpers
-export([test/0, test/1]).

-define(TEST_ITERATIONS, 100).
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
         ?_assertEqual(true, proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT((prop_active_manifests())))))},
        {timeout, 300,
         ?_assertEqual(true, proper:quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT((prop_prune_manifests())))))}
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

prop_active_manifests() ->
    ?FORALL(Manifests, resize(50, manifests()),
        begin
            AlteredManifests = lists:map(fun(M) -> M?MANIFEST{uuid = uuid:get_v4()} end, Manifests),
            AsDict = orddict:from_list([{M?MANIFEST.uuid, M} || M <- AlteredManifests]),
            ToGcUUIDs = lists:sort(riak_cs_manifest_utils:deleted_while_writing(AsDict)),
            Active = riak_cs_manifest_utils:active_manifest(AsDict),
            case Active of
                {error, no_active_manifest} ->
                    %% If no manifest is returned then there should
                    %% not be an active manifest in the list, unless
                    %% the manifest has a write_start_time < delete_marked_time of
                    %% some other deleted manifest's delete_marked_time.
                    ActiveManis = lists:filter(fun(?MANIFEST{state=State}) ->
                                              State == active
                                          end, AlteredManifests),
                    ActiveUUIDs = lists:sort([M?MANIFEST.uuid || M <- ActiveManis]),
                    ActiveUUIDs =:= ToGcUUIDs;
                {ok, AMani} ->
                    %% if a manifest is returned it's state should be active
                    %% and it should have a write_start_time > delete_marked_time
                    %% than the latest delete_marked_time of a deleted manifest.
                    ?assertEqual(AMani?MANIFEST.state, active),
                    false =:= lists:member(AMani?MANIFEST.uuid, ToGcUUIDs)
            end
        end).

prop_prune_manifests() ->
    ?FORALL({Manifests, MaxCount},
            {resize(50, manifests()), frequency([{9, nat()}, {1, 'unlimited'}])},
        begin
            AlteredManifests = lists:map(fun(M) -> M?MANIFEST{uuid = uuid:get_v4()} end, Manifests),
            AsDict = orddict:from_list([{M?MANIFEST.uuid, M} || M <- AlteredManifests]),
            NowTime = {-1, -1, -1},
            case MaxCount of
                'unlimited' ->
                    %% We should not prune any manifests if the prune
                    %% count is set to `unlimited'.
                    AsDict =:= riak_cs_manifest_utils:prune(AsDict, NowTime, MaxCount);
                _ ->
                    prune_helper(AsDict, NowTime, MaxCount)
            end
        end).

prune_helper(AsDict, NowTime, MaxCount) ->
    Pruned = riak_cs_manifest_utils:prune(AsDict, NowTime, MaxCount),
    RemainingScheduledDelete = riak_cs_manifest_utils:filter_manifests_by_state(Pruned, [scheduled_delete]),
    RemainingScheduledDeleteUUIDs = [UUID || {UUID, _Mani} <- RemainingScheduledDelete],
    RemainingScheduledDeleteTimes = [M?MANIFEST.scheduled_delete_time || {_UUID, M} <- RemainingScheduledDelete],

    AllScheduledDelete = riak_cs_manifest_utils:filter_manifests_by_state(AsDict, [scheduled_delete]),
    DroppedScheduledDelete = orddict:filter(fun (UUID, _) -> not lists:member(UUID, RemainingScheduledDeleteUUIDs) end, AllScheduledDelete),
    DroppedScheduledDeleteTimes = [M?MANIFEST.scheduled_delete_time || {_UUID, M} <- DroppedScheduledDelete],

    PredFun = fun(Time) -> lists:all(fun(KeptTime) -> KeptTime =< Time end, RemainingScheduledDeleteTimes) end,

    %% Assert that both we have have kept less than or equal
    %% to `MaxCount' `scheduled_delete' manifests, and that
    %% all of the manifests we did prune have timestamps
    %% greater than the ones we kept.
    conjunction([{dropped_older_than_kept, lists:all(PredFun, DroppedScheduledDeleteTimes)},
                 {count_pruned, length(RemainingScheduledDelete) =< MaxCount}]).

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
            Manifest?MANIFEST{last_block_written_time=erlang:timestamp(),
                              write_blocks_remaining=blocks_set()};
        active ->
            %% this clause isn't
            %% needed but it makes
            %% things more clear imho
            Manifest?MANIFEST{last_block_deleted_time=erlang:timestamp(),
                              write_start_time=riak_cs_gen:timestamp()};
        pending_delete ->
            Manifest?MANIFEST{last_block_deleted_time=erlang:timestamp(),
                              delete_blocks_remaining=blocks_set(),
                              delete_marked_time=riak_cs_gen:timestamp(),
                              props=riak_cs_gen:props()};
        scheduled_delete ->
            Manifest?MANIFEST{delete_marked_time=riak_cs_gen:timestamp(),
                              scheduled_delete_time=riak_cs_gen:timestamp(),
                              props=riak_cs_gen:props()}
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
    [proper:quickcheck(numtests(Iterations, prop_active_manifests())),
     proper:quickcheck(numtests(Iterations, prop_prune_manifests()))].
