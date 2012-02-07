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
            WrappedManifests = lists:map(MapFun, Manifests),
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
                     state=moss_gen:manifest_state(),
                     write_blocks_remaining=ordsets:new(),
                     delete_blocks_remaining=ordsets:new(),
                     last_block_written_time=erlang:now(),
                     last_block_deleted_time=erlang:now()}.

manifest() ->
    raw_manifest().
    %?LET(Manifest, raw_manifest(), Manifests).

manifests() ->
    eqc_gen:list(manifest()).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, resolution_commutative())).
