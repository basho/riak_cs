%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_bucket_test).

-compile(export_all).
-compile(nowarn_export_all).

-ifdef(TEST).
-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

handle_delete_response_test() ->
    ErrorDoc =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>"
        " <Code>MultipartUploadRemaining</Code> <Message>Multipart uploads still remaining.</Message>"
        "<Resource>/buckets/riak-test-bucket</Resource><RequestId></RequestId></Error>",
    %% which is defined at stanchion_response.erl
    ?assertEqual({error, remaining_multipart_upload},
                 riak_cs_bucket:handle_stanchion_response(409, ErrorDoc, delete, <<>>)),
    ?assertThrow({remaining_multipart_upload_on_deleted_bucket, <<>>},
                 riak_cs_bucket:handle_stanchion_response(409, ErrorDoc, create, <<>>)),
    ErrorResponse = riak_cs_s3_response:error_response(ErrorDoc),
    ?assertEqual(ErrorResponse,
                 riak_cs_bucket:handle_stanchion_response(503, ErrorDoc, delete, <<>>)),
    ?assertEqual(ErrorResponse,
                 riak_cs_bucket:handle_stanchion_response(204, ErrorDoc, delete, <<>>)).


bucket_resolution_test() ->
    %% @TODO Replace or augment this with eqc testing.
    UserRecord = riak_cs_user:user_record("uncle fester",
                                          "fester@tester.com",
                                          "festersquest",
                                          "wasthebest",
                                          "cid"),
    BucketList1 = [riak_cs_bucket:bucket_record(<<"bucket1">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket2">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket3">>, create)],
    BucketList2 = [riak_cs_bucket:bucket_record(<<"bucket1">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, create)],
    BucketList3 = [riak_cs_bucket:bucket_record(<<"bucket1">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, delete),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, create)],
    BucketList4 = [riak_cs_bucket:bucket_record(<<"bucket1">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, create),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, delete)],
    BucketList5 = [riak_cs_bucket:bucket_record(<<"bucket1">>, delete),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, delete),
                   riak_cs_bucket:bucket_record(<<"bucket1">>, delete)],
    Obj1 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList1]),
    Obj2 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList2]),
    Obj3 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList3]),
    Obj4 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList4]),
    Obj5 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList5]),
    Values1 = riakc_obj:get_values(Obj1),
    Values2 = riakc_obj:get_values(Obj2),
    Values3 = riakc_obj:get_values(Obj3),
    Values4 = riakc_obj:get_values(Obj4),
    Values5 = riakc_obj:get_values(Obj5),
    ResBuckets1 = riak_cs_bucket:resolve_buckets(Values1, [], true),
    ResBuckets2 = riak_cs_bucket:resolve_buckets(Values2, [], true),
    ResBuckets3 = riak_cs_bucket:resolve_buckets(Values3, [], true),
    ResBuckets4 = riak_cs_bucket:resolve_buckets(Values4, [], true),
    ResBuckets5 = riak_cs_bucket:resolve_buckets(Values5, [], true),
    ?assertEqual(BucketList1, ResBuckets1),
    ?assertEqual([hd(BucketList2)], ResBuckets2),
    ?assertEqual([hd(lists:reverse(BucketList3))], ResBuckets3),
    ?assertEqual([hd(lists:reverse(BucketList4))], ResBuckets4),
    ?assertEqual([hd(BucketList5)], ResBuckets5).


-endif.
