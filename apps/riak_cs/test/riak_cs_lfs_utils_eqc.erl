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

%% @doc Quickcheck test module for `riak_cs_lfs_utils'.

-module(riak_cs_lfs_utils_eqc).

-include("riak_cs.hrl").

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
    ?FORALL({CLength, BlockSize},{riak_cs_gen:block_size(), riak_cs_gen:content_length()},
            begin
                NumBlocks = riak_cs_lfs_utils:block_count(CLength, BlockSize),
                Product = NumBlocks * BlockSize,
                conjunction([{greater, Product >= CLength},
                             {lesser, (Product - BlockSize) < CLength}])
            end).

%% @doc EQC property for manipulating manifests
%% with `riak_cs_lfs_utils`. Tests:
%%
%% 2. `still_waiting` correctly returns false when
%% all of the blocks calculated by `initial_blocks`
%% have been removed from the manifest
prop_manifest_manipulation() ->
    ?FORALL({Bucket, FileName, UUID, CLength, Md5, MD},
            {riak_cs_gen:bucket(),
             riak_cs_gen:file_name(),
             riak_cs_gen:uuid(),
             riak_cs_gen:content_length(),
             riak_cs_gen:md5(),
             riak_cs_gen:metadata()},

            begin
                application:set_env(riak_cs, lfs_block_size, 1048576),
                Manifest = riak_cs_lfs_utils:new_manifest(
                             Bucket,
                             FileName,
                             UUID,
                             CLength,
                             <<"ctype">>,
                             Md5,
                             MD,
                             riak_cs_lfs_utils:block_size(),
                             riak_cs_acl_utils:default_acl("tester",
                                                           "tester_id",
                                                           "tester_key_id"),
                             [],
                             undefined,
                             undefined),
                Blocks = riak_cs_lfs_utils:initial_blocks(CLength, riak_cs_lfs_utils:block_size()),
                %% TODO: maybe we should shuffle blocks?
                FoldFun = fun (Chunk, Mani) -> riak_cs_lfs_utils:remove_write_block(Mani, Chunk) end,
                EmptyMani = lists:foldl(FoldFun, Manifest, Blocks),
                EmptyMani?MANIFEST.state == active
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
