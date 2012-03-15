%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_lfs_utils'.

-module(riak_moss_lfs_utils_eqc).

-include("riak_moss.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_block_count/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 500).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
        [
            {timeout, 20, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_block_count()))))},
            {timeout, 60, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_manifest_manipulation()))))}
        ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_block_count() ->
    ?FORALL({CLength, BlockSize},{moss_gen:block_size(), moss_gen:content_length()},
        begin
            NumBlocks = riak_moss_lfs_utils:block_count(CLength, BlockSize),
            Product = NumBlocks * BlockSize,
            conjunction([{greater, Product >= CLength},
                         {lesser, (Product - BlockSize) < CLength}])
        end).

%% @doc EQC property for manipulating manifests
%% with `riak_moss_lfs_utils`. Tests:
%%
%% 1. `is_manifest` correctly returns true for binary
%% encoded manifests
%% 2. `still_waiting` correctly returns false when
%% all of the blocks calculated by `initial_blocks`
%% have been removed from the manifest
prop_manifest_manipulation() ->
    ?FORALL({Bucket, FileName, UUID, CLength, Md5, MD},
                    {moss_gen:bucket(),
                     moss_gen:file_name(),
                     moss_gen:uuid(),
                     moss_gen:content_length(),
                     moss_gen:md5(),
                     moss_gen:metadata()},

        begin
            application:set_env(riak_moss, lfs_block_size, 1048576),
            Manifest = riak_moss_lfs_utils:new_manifest(Bucket,
                                                        FileName,
                                                        UUID,
                                                        CLength,
                                                        <<"ctype">>,
                                                        Md5,
                                                        MD,
                                                        riak_moss_lfs_utils:block_size()),

            Blocks = riak_moss_lfs_utils:initial_blocks(CLength, riak_moss_lfs_utils:block_size()),
            %% TODO: maybe we should shuffle blocks?
            FoldFun = fun (Chunk, Mani) -> riak_moss_lfs_utils:remove_write_block(Mani, Chunk) end,
            EmptyMani = lists:foldl(FoldFun, Manifest, Blocks),
            conjunction([{is_manifest, riak_moss_lfs_utils:is_manifest(term_to_binary(Manifest))},
                         {is_active, EmptyMani#lfs_manifest_v2.state == active}])
        end).


%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_block_count())),
    eqc:quickcheck(eqc:numtests(Iterations, prop_manifest_manipulation())).

-endif.
