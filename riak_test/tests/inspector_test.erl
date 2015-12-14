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

-module(inspector_test).

-export([confirm/0]).

-define(BLOCK_SIZE, 10).
-define(OBJECT_SIZE, 100).
-define(DEFAULT_HEADER_LINES, 2).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    rtcs:set_advanced_conf(cs, [{riak_cs, [{lfs_block_size, ?BLOCK_SIZE}]}]),
    {UserConfig, _Nodes} = rtcs:setup(1),
    [ok = erlcloud_s3:create_bucket(Bucket, UserConfig)||Bucket<-buckets()],

    load_objects(bucket(1), UserConfig),

    verify_bucket_cmd(),
    verify_object_cmd(),
    verify_block_cmd(),
    verify_storage_cmd(),
    verify_access_cmd(),

    cleanup_bucket(bucket(1), UserConfig),

    verify_gc_cmd(),
    pass.

verify_bucket_cmd() ->
    %% verify listing buckets with sort option
    BucketList = result_to_lists(rtcs_exec:inspector("bucket list --sort")),
    lists:foldl(fun([Bucket|_], Num) ->
                        ?assertEqual(bucket(Num), Bucket),
                        Num+1
                end, 1, BucketList),

    %% verify listing buckets
    UnsortedBucketList = result_to_lists(rtcs_exec:inspector("bucket list")),
    ?assertNotEqual(BucketList, UnsortedBucketList),
    ?assertEqual(BucketList, lists:sort(fun([BucketA|_], [BucketB|_]) ->
                                                BucketA < BucketB
                                        end,
                                        UnsortedBucketList)),

    %% verify show bucket
    Bucket1 = rtcs_exec:inspector(string:join(["bucket", "show", bucket(1)], " ")),
    Owner1 = lists:nth(3, hd(BucketList)),
    ?assertMatch({match, _}, re:run(Bucket1, Owner1)),
    ok.

verify_object_cmd() ->
    %% verify listing objects with sort option
    KeyList = result_to_lists(
                rtcs_exec:inspector("object list " ++ bucket(1) ++ " --sort")),
    lists:foldl(fun([Key|_], Num) ->
                        ?assertEqual(key(Num), Key),
                        Num+1
                end, 1, KeyList),

    %% verify listing objects
    UnsortedKeyList = result_to_lists(rtcs_exec:inspector("object list " ++ bucket(1))),
    ?assertNotEqual(KeyList, UnsortedKeyList),
    ?assertEqual(KeyList, lists:sort(fun([KeyA|_], [KeyB|_]) ->
                                             KeyA < KeyB
                                     end,
                                     UnsortedKeyList)),

    %% verify show object
    Obj1 = rtcs_exec:inspector(string:join(["object", "show", bucket(1), key(1)], " ")),
    UUID1 = lists:nth(4, hd(KeyList)),
    ?assertMatch({match, _}, re:run(Obj1, UUID1)),
    ok.

verify_block_cmd() ->
    %% verify listing blocks
    KeyList = result_to_lists(
                rtcs_exec:inspector("object list " ++ bucket(1) ++ " --sort")),
    UUID1 = lists:nth(4, hd(KeyList)),
    BlockList = result_to_lists(
                  rtcs_exec:inspector(
                    string:join(["block", "list", bucket(1), key(1), UUID1], " ")), 3),
    ?assertEqual(?OBJECT_SIZE div ?BLOCK_SIZE, length(BlockList)),
    ok.

verify_storage_cmd() ->
    _ = rtcs_exec:calculate_storage(1),
    [Key|_] = hd(result_to_lists(rtcs_exec:inspector("storage list"))),
    Stats = result_to_lists(rtcs_exec:inspector("storage show " ++ Key), 6),
    ?assertEqual(["inspector-test-001", "100", "10000", "-1"], lists:last(Stats)),
    ok.

verify_access_cmd() ->
    _ = rtcs_exec:flush_access(1),
    [Key|_] = hd(result_to_lists(rtcs_exec:inspector("access list"))),
    Stats = result_to_lists(rtcs_exec:inspector("access show " ++ Key)),
    ?assertEqual([["BucketCreate", "Count",   "100"],
                  ["KeyWrite",     "BytesIn", "10000"],
                  ["KeyWrite",     "Count",   "100"]],
                 lists:nthtail(length(Stats)-3, Stats)),
    ok.

verify_gc_cmd() ->
    [Key|_] = hd(result_to_lists(rtcs_exec:inspector("gc list"))),
    Ret = rtcs_exec:inspector("gc show " ++ Key),
    ?assertMatch({match, _}, re:run(Ret, "pending_delete")),
    ok.

load_objects(Bucket, UserConfig) ->
    [?assertEqual([{version_id, "null"}],
                  erlcloud_s3:put_object(Bucket,
                                         Key,
                                         crypto:rand_bytes(?OBJECT_SIZE),
                                         UserConfig))
     ||Key<-keys()].


cleanup_bucket(Bucket, UserConfig) ->
    [?assertEqual([{delete_marker, false},
                   {version_id, "null"}],
                  erlcloud_s3:delete_object(Bucket, Key, UserConfig))
     ||Key<-keys()].

bucket(N) ->
    "inspector-test-" ++ zero_padding(N).

key(N) ->
    "key-" ++ zero_padding(N).

zero_padding(N) ->
    lists:flatten(io_lib:format("~3..0B", [N])).

buckets() ->
    [bucket(N)||N<-lists:seq(1, 100)].

keys() ->
    [key(N)||N<-lists:seq(1, 100)].

result_to_lists(Ret) ->
    result_to_lists(Ret, ?DEFAULT_HEADER_LINES).

result_to_lists(Ret, HeaderLines) ->
    Tokens1 = string:tokens(Ret, "\n"),
    Tokens2 = lists:nthtail(HeaderLines, Tokens1), % remove headers
    lists:map(fun(Line) -> string:tokens(Line, " :=") end, Tokens2).
