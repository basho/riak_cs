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

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(1),
    [ok = erlcloud_s3:create_bucket(Bucket, UserConfig)||Bucket<-buckets()],
    [[{version_id, "null"}] = erlcloud_s3:put_object(bucket(1),
                                                     Key,
                                                     crypto:rand_bytes(100),
                                                     UserConfig)||Key<-keys()],

    %% verify listing buckets
    SortedBucketsByDict = lists:sort(buckets()),
    BucketList = result_to_lists(rtcs_exec:inspector("bucket list --sort")),
    lists:foldl(fun([Bucket|_], Num) ->
                        ?assertEqual(lists:nth(Num, SortedBucketsByDict), Bucket),
                        Num+1
                end, 1, BucketList),

    %% verify listing bucketsption
    UnsortedBucketList = result_to_lists(rtcs_exec:inspector("bucket list")),
    ?assertNotEqual(BucketList, UnsortedBucketList),
    ?assertEqual(BucketList, lists:sort(fun([BucketA|_], [BucketB|_]) ->
                                                BucketA < BucketB
                                        end,
                                        UnsortedBucketList)),

    %% verify show object
    Bucket1 = rtcs_exec:inspector(string:join(["bucket", "show", bucket(1)], " ")),
    Owner1 = lists:nth(3, hd(BucketList)),
    ?assertMatch({match, _}, re:run(Bucket1, Owner1)),

    %% verify listing objects with sort option
    SortedKeysByDict = lists:sort(keys()),
    KeyList = result_to_lists(
                rtcs_exec:inspector("object list " ++ bucket(1) ++ " --sort")),
    lists:foldl(fun([Key|_], Num) ->
                        ?assertEqual(lists:nth(Num, SortedKeysByDict), Key),
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

    pass.

bucket(N) ->
    "inspector-test" ++ integer_to_list(N).

key(N) ->
    "key" ++ integer_to_list(N).

buckets() ->
    [bucket(N)||N<-lists:seq(1, 100)].

keys() ->
    [key(N)||N<-lists:seq(1, 100)].

result_to_lists(Ret) ->
    Tokens1 = string:tokens(Ret, "\n"),
    Tokens2 = lists:sublist(Tokens1, 3, length(Tokens1)-2), % remove headers
    lists:map(fun(Line) -> string:tokens(Line, " ") end, Tokens2).
