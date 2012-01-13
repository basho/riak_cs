%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_moss_lfs_utils'.

-module(riak_moss_lfs_utils_eqc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_block_count/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(TEST_ITERATIONS, 500).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
        [%% Run the quickcheck tests
            {timeout, 60,
                ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, prop_block_count())))},
            {timeout, 60,
                ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, prop_manifest_manipulation())))}
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

prop_manifest_manipulation() ->
    ?FORALL({Bucket, FileName, UUID, CLength, Md5, MD},
                    {moss_gen:bucket(),
                     moss_gen:file_name(),
                     moss_gen:uuid(),
                     moss_gen:content_length(),
                     moss_gen:md5(),
                     moss_gen:metadata()},

        begin
            Manifest = riak_moss_lfs_utils:new_manifest(Bucket,
                                                        FileName,
                                                        UUID,
                                                        CLength,
                                                        Md5,
                                                        MD),

            %% BlockCount = riak_moss_lfs_utils:block_count(Manifest),
            Blocks = sets:to_list(riak_moss_lfs_utils:initial_blocks(CLength)),
            %% TODO: maybe we should shuffle blocks?
            FoldFun = fun (Chunk, Mani) -> riak_moss_lfs_utils:remove_block(Mani, Chunk) end,
            EmptyMani = lists:foldl(FoldFun, Manifest, Blocks),
            conjunction([{is_manifest, riak_moss_lfs_utils:is_manifest(term_to_binary(Manifest))},
                         {not_waiting, not riak_moss_lfs_utils:still_waiting(EmptyMani)}])
        end).


%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(100).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_block_count())),
    eqc:quickcheck(eqc:numtests(Iterations, prop_manifest_manipulation())).

-endif.
