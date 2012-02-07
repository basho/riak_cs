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
-export([resolution_commutative/0]).

%% helpers
-export([test/0, test/1]).

-define(TEST_ITERATIONS, 500).
-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args) end, P)).

%% ====================================================================
%% eqc property
%% ====================================================================

resolution_commutative() ->
    ?FORALL(Manifests, manifests(),
        begin
            MapFun = fun(Mani) ->
                riak_moss_manifest:new(Mani#lfs_manifest_v2.uuid, Mani)
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
    #lfs_manifest_v2{uuid=moss_gen:bounded_uuid(),
                     bkey={<<"bucket">>, <<"key">>},
                     state=moss_gen:manifest_state()}.

manifest() ->
    ?LET(Manifest, raw_manifest(), process_manifest(Manifest)).

process_manifest(Manifest=#lfs_manifest_v2{state=State}) ->
    case State of
        writing ->
            Manifest#lfs_manifest_v2{last_block_written_time=erlang:now(),
                                     write_blocks_remaining=blocks_set()};
        active ->
            %% this clause isn't
            %% needed but it makes
            %% things more clear imho
            Manifest#lfs_manifest_v2{last_block_deleted_time=erlang:now()};
        pending_delete ->
            Manifest#lfs_manifest_v2{last_block_deleted_time=erlang:now(),
                                     delete_blocks_remaining=blocks_set()};
        deleted ->
            Manifest#lfs_manifest_v2{last_block_deleted_time=erlang:now()}
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
    eqc:quickcheck(eqc:numtests(Iterations, resolution_commutative())).

only_one_active(Manifests) ->
    {_, FilteredManifests} = lists:foldl(fun only_one_active_helper/2, {not_found, []}, Manifests),
    FilteredManifests.

only_one_active_helper(#lfs_manifest_v2{state=active}, {found, List}) ->
    {found, List};
only_one_active_helper(Manifest, {found, List}) ->
    {found, [Manifest | List]};
only_one_active_helper(Manifest=#lfs_manifest_v2{state=active}, {not_found, List}) ->
    {found, [Manifest | List]};
only_one_active_helper(Manifest, {not_found, List}) ->
    {not_found, [Manifest | List]}.

