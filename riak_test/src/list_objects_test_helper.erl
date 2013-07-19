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

-module(list_objects_test_helper).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

test(UserConfig) ->
    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    %% Put 10 objects in the bucket
    Count1 = 10,
    load_objects(?TEST_BUCKET, Count1, UserConfig),

    %% Successively list the buckets, verify the output, and delete an
    %% object from the bucket until the bucket is empty again.
    delete_and_verify_objects(?TEST_BUCKET, Count1, UserConfig),

    %% Put 200 objects in the bucket
    Count2 = 200,
    load_objects(?TEST_BUCKET, Count2, UserConfig),

    %% Successively list the buckets, verify the output, and delete an
    %% object from the bucket until the bucket is empty again.
    delete_and_verify_objects(?TEST_BUCKET, Count2, UserConfig),

    %% Put 30 objects in the bucket
    Count3 = 30,
    load_objects(?TEST_BUCKET, Count3, UserConfig),

    %% Use `max-keys' to restrict object list to 20 results
    Options1 = [{max_keys, 20}],
    ObjList1 = erlcloud_s3:list_objects(?TEST_BUCKET, Options1, UserConfig),
    verify_object_list(ObjList1, 20, 30),

    %% Use `marker' to request remainder of results
    Options2 = [{marker, "27"}],
    verify_object_list(erlcloud_s3:list_objects(?TEST_BUCKET, Options2, UserConfig), 10, 30, 21),

    %% Put 2 sets of 4 objects with keys that have a common subdirectory
    Prefix1 = "0/prefix1/",
    Prefix2 = "0/prefix2/",
    load_objects(?TEST_BUCKET, 4, Prefix1, UserConfig),
    load_objects(?TEST_BUCKET, 4, Prefix2, UserConfig),

    %% Use `prefix' and `delimiter' to get the key groups under
    %% the `prefix'. Should get 2 common prefix results back.
    Options3 = [{prefix, "0/"}, {delimiter, "/"}],
    ObjList2 = erlcloud_s3:list_objects(?TEST_BUCKET, Options3, UserConfig),
    CommonPrefixes1 = proplists:get_value(common_prefixes, ObjList2),
    ?assert(lists:member([{prefix, Prefix1}], CommonPrefixes1)),
    ?assert(lists:member([{prefix, Prefix2}], CommonPrefixes1)),
    ?assertEqual([], proplists:get_value(contents, ObjList2)),

    %% Use `prefix' option to restrict results to only keys that
    %% begin with that prefix. Without the `delimiter' option the keys
    %% are returned in the contents instead of as common prefixes.
    Options4 = [{prefix, "0/"}],
    ObjList3 = erlcloud_s3:list_objects(?TEST_BUCKET, Options4, UserConfig),
    CommonPrefixes2 = proplists:get_value(common_prefixes, ObjList3),
    ?assertEqual([], CommonPrefixes2),
    ExpObjList1 = [Prefix1 ++ integer_to_list(X) || X <- lists:seq(1,4)] ++
        [Prefix2 ++ integer_to_list(Y) || Y <- lists:seq(1,4)],
    ?assertEqual(ExpObjList1, [proplists:get_value(key, O) ||
                                  O <- proplists:get_value(contents, ObjList3)]),

    %% Request remainder of results
    Options5 = [{marker, "7"}],
    verify_object_list(erlcloud_s3:list_objects(?TEST_BUCKET, Options5, UserConfig), 2, 30, 29),

    %% Use `delimiter' and verify results include a single common
    %% prefixe and 30 keys in the contents
    Options6 = [{delimiter, "/"}],
    ObjList4 = erlcloud_s3:list_objects(?TEST_BUCKET, Options6, UserConfig),
    CommonPrefixes3 = proplists:get_value(common_prefixes, ObjList4),
    ?assert(lists:member([{prefix, "0/"}], CommonPrefixes3)),
    verify_object_list(ObjList4, 30),

    delete_objects(?TEST_BUCKET, Count3, [], UserConfig),
    delete_objects(?TEST_BUCKET, 4, Prefix1, UserConfig),
    delete_objects(?TEST_BUCKET, 4, Prefix2, UserConfig),

    Options7 = [{max_keys, "invalid"}],
    ?assertError({aws_error, {http_error, 400, _, _}},
                 erlcloud_s3:list_objects(?TEST_BUCKET, Options7, UserConfig)),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}}, erlcloud_s3:list_objects(?TEST_BUCKET, UserConfig)),
    pass.

load_objects(Bucket, Count, Config) ->
    load_objects(Bucket, Count, [], Config).

load_objects(Bucket, Count, KeyPrefix, Config) ->
    [erlcloud_s3:put_object(Bucket,
                            KeyPrefix ++ integer_to_list(X),
                            crypto:rand_bytes(100),
                            Config) || X <- lists:seq(1,Count)].

verify_object_list(ObjList, ExpectedCount) ->
    verify_object_list(ObjList, ExpectedCount, ExpectedCount, 1).

verify_object_list(ObjList, ExpectedCount, TotalCount) ->
    verify_object_list(ObjList, ExpectedCount, TotalCount, 1).

verify_object_list(ObjList, ExpectedCount, TotalCount, Offset) ->
    verify_object_list(ObjList, ExpectedCount, TotalCount, Offset, []).

verify_object_list(ObjList, ExpectedCount, TotalCount, 1, KeyPrefix)
  when ExpectedCount =:= TotalCount ->
    ?assertEqual(lists:sort([KeyPrefix ++ integer_to_list(X)
                             || X <- lists:seq(1, ExpectedCount)]),
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(contents, ObjList)]);
verify_object_list(ObjList, ExpectedCount, TotalCount, Offset, KeyPrefix) ->
    ?assertEqual(lists:sublist(
                   lists:sort([KeyPrefix ++ integer_to_list(X)
                               || X <- lists:seq(1, TotalCount)]),
                   Offset,
                   ExpectedCount),
                 [proplists:get_value(key, O) ||
                     O <- proplists:get_value(contents, ObjList)]).

delete_and_verify_objects(Bucket, 0, Config) ->
    verify_object_list(erlcloud_s3:list_objects(Bucket, Config), 0),
    ok;
delete_and_verify_objects(Bucket, Count, Config) ->
    verify_object_list(erlcloud_s3:list_objects(Bucket, Config), Count),
    erlcloud_s3:delete_object(Bucket, integer_to_list(Count), Config),
    delete_and_verify_objects(Bucket, Count-1, Config).

delete_objects(Bucket, Count, Prefix, Config) ->
    [erlcloud_s3:delete_object(Bucket,
                               Prefix ++ integer_to_list(X),
                               Config)
     || X <- lists:seq(1, Count)].
