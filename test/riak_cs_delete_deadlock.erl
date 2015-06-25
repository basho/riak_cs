%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_cs_delete_deadlock).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("riak_cs.hrl").

-compile(export_all).

%% eqc property
-export([prop_delete_deadlock/0]).

-define(TEST_ITERATIONS, 500).
-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) ->
                io:format(user, Str, Args) end, P)).
-define(CONTENT_LENGTH, 10 * 1048576).
-define(BLOCK_SIZE, 1024).

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
            ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT((prop_delete_deadlock())))))}
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

prop_delete_deadlock() ->
    ?FORALL({UUID, Parts},
             {g_uuid(),
              part_manifests()},
             begin
                BlockSize = riak_cs_lfs_utils:block_size(),
                Manifest = riak_cs_lfs_utils:new_manifest(
                             <<"bucket">>,
                             "test_file",
                             UUID,
                             ?CONTENT_LENGTH,
                             <<"ctype">>,
                             "md5",
                             dict:new(),
                             BlockSize,
                             riak_cs_acl_utils:default_acl("tester",
                                                           "tester_id",
                                                           "tester_key_id"),
                             [],
                             undefined,
                             undefined),
                MpM = ?MULTIPART_MANIFEST{parts = Parts},
                NewManifest = Manifest?MANIFEST{props = 
                    riak_cs_mp_utils:replace_mp_manifest(MpM, Manifest?MANIFEST.props)},

                OutputList = riak_cs_lfs_utils:block_sequences_for_manifest(NewManifest),
                TestList = assemble_test_list(?CONTENT_LENGTH, BlockSize, Parts),

                OutputList == TestList
            end).

%%====================================================================
%% Helpers
%%====================================================================
assemble_test_list(ContentLength, BlockSize, Parts) ->
    Int = ContentLength div BlockSize,
    lists:usort([ {Part?PART_MANIFEST.part_id, BlockSeg} || BlockSeg <- lists:seq(0, Int - 1),
                                  Part <- Parts ]).

%%====================================================================
%% Generators
%%====================================================================
part_manifests() ->
    not_empty(eqc_gen:list(part())).

raw_part() ->    
    ?PART_MANIFEST{bucket= <<"part_bucket">>, 
                   key = <<"part_key">>,
                   start_time = os:timestamp(),
                   part_id = g_uuid(),
                   content_length = ?CONTENT_LENGTH,
                   block_size=?BLOCK_SIZE}.
part() ->          
    ?LET(Part, raw_part(), process_part(Part)).

process_part(Part) ->
    Part?PART_MANIFEST{part_number = choose(1,1000)}.

g_uuid() ->
    noshrink(eqc_gen:bind(eqc_gen:bool(), fun(_) -> druuid:v4_str() end)).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

-endif.
