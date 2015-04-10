%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Test for storage MR functions

-module(riak_cs_storage_mr_test).

-compile(export_all).

-include("riak_cs.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

object_size_map_test_() ->
    M0 = ?MANIFEST{state=active, content_length=25},
    M1 = ?MANIFEST{state=active, content_length=35},
    M2 = ?MANIFEST{state=writing, props=undefined, content_length=42},
    M3 = ?MANIFEST{state=writing, props=pocketburger, content_length=234},
    M4 = ?MANIFEST{state=writing, props=[{multipart,undefined}],
                   content_length=23434},
    M5 = ?MANIFEST{state=writing, props=[{multipart,pocketburger}],
                   content_length=23434},

    [?_assertEqual([{1,25}], riak_cs_storage_mr:object_size([{uuid,M0}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M2},{uuid1,M1}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M3},{uuid1,M1}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M4},{uuid1,M1}])),
     ?_assertEqual([{1,35}], riak_cs_storage_mr:object_size([{uuid2,M5},{uuid1,M1}]))].

count_multipart_parts_test_() ->
    ZeroZero = {0, 0},
    ValidMPManifest = ?MULTIPART_MANIFEST{parts=[?PART_MANIFEST{content_length=10}]},
    [?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=pocketburgers, state=writing}}])),
     ?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=pocketburgers, state=iamyourfather}}])),
     ?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=[], state=writing}}])),
     ?_assertEqual(ZeroZero,
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=[{multipart, pocketburger}], state=writing}}])),
     ?_assertEqual({1, 10},
                   riak_cs_storage_mr:count_multipart_parts(
                     [{<<"pocketburgers">>,
                       ?MANIFEST{props=[{multipart, ValidMPManifest}], state=writing}}]))
    ].

-endif.
