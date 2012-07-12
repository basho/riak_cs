%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------


-module(riak_moss_manifest_resolution_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_moss.hrl").

-compile(export_all).

%% eqc property
-export([prop_resolution_commutative/0]).

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
            ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT((prop_resolution_commutative())))))}
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

prop_resolution_commutative() ->
    ?FORALL(Manifests, eqc_gen:resize(50, manifests()),
        begin
            MapFun = fun(Mani) ->
                riak_cs_manifest_utils:new_dict(Mani?MANIFEST.uuid, Mani)
            end,
            Filtered = only_one_active(Manifests),
            WrappedManifests = lists:map(MapFun, Filtered),
            Resolved = riak_moss_manifest_resolution:resolve(WrappedManifests),
            ReversedResolved = riak_moss_manifest_resolution:resolve(lists:reverse(WrappedManifests)),
            Resolved == ReversedResolved
        end).

%%====================================================================
%% Generators
%%====================================================================

raw_manifest() ->
    ?MANIFEST{uuid=moss_gen:bounded_uuid(),
                     bkey={<<"bucket">>, <<"key">>},
                     state=moss_gen:manifest_state()}.

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
            Manifest?MANIFEST{last_block_deleted_time=erlang:now(),
                                     delete_blocks_remaining=blocks_set()};
        deleted ->
            Manifest?MANIFEST{last_block_deleted_time=erlang:now()}
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
    eqc:quickcheck(eqc:numtests(Iterations, prop_resolution_commutative())).

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
