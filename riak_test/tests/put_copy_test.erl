%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(put_copy_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(assert403(X),
        ?assertError({aws_error, {http_error, 403, _, _}}, (X))).

-define(BUCKET, "put-copy-bucket-test").
-define(KEY, "pocket").

-define(DATA0, <<"pocket">>).

-define(BUCKET2, "put-target-bucket").
-define(KEY2, "sidepocket").
-define(KEY3, "superpocket").
-define(BUCKET3, "the-other-put-target-bucket").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _}} = rtcs:setup(1),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    Data = ?DATA0,
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY, Data, UserConfig)),
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY2, Data, UserConfig)),

    RiakNode = hd(RiakNodes),
    {AccessKeyId, SecretAccessKey} = rtcs:create_user(RiakNode, 1),
    UserConfig2 = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(RiakNode)),
    {AccessKeyId1, SecretAccessKey1} = rtcs:create_user(RiakNode, 1),
    UserConfig3 = rtcs:config(AccessKeyId1, SecretAccessKey1, rtcs:cs_port(RiakNode)),

    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET2, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET3, UserConfig2)),

    ok = verify_simple_copy(UserConfig),
    ok = verify_others_copy(UserConfig, UserConfig2),
    ok = verify_multipart_copy(UserConfig),
    ok = verify_security(UserConfig, UserConfig2, UserConfig3),

    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(?BUCKET, ?KEY, UserConfig)),
    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(?BUCKET, ?KEY2, UserConfig)),
    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(?BUCKET2, ?KEY2, UserConfig)),
    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(?BUCKET3, ?KEY, UserConfig2)),
    rtcs:pass().


verify_simple_copy(UserConfig) ->

    ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
                 erlcloud_s3:copy_object(?BUCKET2, ?KEY2, ?BUCKET, ?KEY, UserConfig)),

    Props = erlcloud_s3:get_object(?BUCKET2, ?KEY2, UserConfig),
    lager:debug("copied object: ~p", [Props]),
    ?assertEqual(?DATA0, proplists:get_value(content, Props)),

    ok.


verify_others_copy(UserConfig, OtherUserConfig) ->
    %% try copy to fail, because no permission
    ?assert403(erlcloud_s3:copy_object(?BUCKET3, ?KEY, ?BUCKET, ?KEY, OtherUserConfig)),

    %% set key public
    Acl = [{acl, public_read}],
    ?assertEqual([{version_id,"null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY, ?DATA0,
                                        Acl, [], UserConfig)),

    %% make sure observable from Other
    Props = erlcloud_s3:get_object(?BUCKET, ?KEY, OtherUserConfig),
    ?assertEqual(?DATA0, proplists:get_value(content, Props)),

    %% try copy
    ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
                 erlcloud_s3:copy_object(?BUCKET3, ?KEY, ?BUCKET, ?KEY, OtherUserConfig)),

    Props2 = erlcloud_s3:get_object(?BUCKET3, ?KEY, OtherUserConfig),
    lager:debug("copied object: ~p", [Props2]),
    ?assertEqual(?DATA0, proplists:get_value(content, Props2)),
    ok.

verify_multipart_copy(UserConfig) ->
    %% ~6MB iolist
    MillionPockets = binary:copy(<<"pocket">>, 1000000),
    MillionBurgers = binary:copy(<<"burger">>, 1000000),

    ?assertEqual([{version_id,"null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY, MillionPockets, UserConfig)),
    ?assertEqual([{version_id,"null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY2, MillionBurgers, UserConfig)),

    InitUploadRes=erlcloud_s3_multipart:initiate_upload(?BUCKET2, ?KEY3, "text/plain", [], UserConfig),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),
    lager:info("~p ~p", [InitUploadRes, UploadId]),
    
    {RespHeaders1, _} = rtcs_multipart:upload_part_copy(?BUCKET2, ?KEY3, UploadId, 1,
                                                        ?BUCKET, ?KEY, UserConfig),
    lager:debug("RespHeaders1: ~p", [RespHeaders1]),
    Etag1 = rtcs_multipart:assert_part(?BUCKET2, ?KEY3, UploadId, 1, UserConfig, RespHeaders1),

    {RespHeaders2, _} = rtcs_multipart:upload_part_copy(?BUCKET2, ?KEY3, UploadId, 2,
                                                        ?BUCKET, ?KEY2, UserConfig),
    lager:debug("RespHeaders2: ~p", [RespHeaders2]),
    Etag2 = rtcs_multipart:assert_part(?BUCKET2, ?KEY3, UploadId, 2, UserConfig, RespHeaders2),

    List = erlcloud_s3_multipart:list_uploads(?BUCKET2, [], UserConfig),
    lager:debug("List: ~p", [List]),

    EtagList = [ {1, Etag1}, {2, Etag2} ],
    ?assertEqual(ok, erlcloud_s3_multipart:complete_upload(?BUCKET2,
                                                           ?KEY3,
                                                           UploadId,
                                                           EtagList,
                                                           UserConfig)),

    UploadsList2 = erlcloud_s3_multipart:list_uploads(?BUCKET2, [], UserConfig),
    Uploads2 = proplists:get_value(uploads, UploadsList2, []),
    ?assertNot(mp_upload_test:upload_id_present(UploadId, Uploads2)),

    MillionPocketBurgers = iolist_to_binary([MillionPockets, MillionBurgers]),
    Props = erlcloud_s3:get_object(?BUCKET2, ?KEY3, UserConfig),
    %% lager:debug("~p> Props => ~p", [?LINE, Props]),
    ?assertEqual(MillionPocketBurgers, proplists:get_value(content, Props)),
    ok.

verify_security(Alice, Bob, Charlie) ->
    AlicesBucket = "alice",
    AlicesPublicBucket = "alice-public",
    AlicesObject = "alices-secret-note",
    AlicesPublicObject = "alices-public-note",

    BobsBucket = "bob",
    BobsObject = "bobs-secret-note",

    CharliesBucket = "charlie",

    %% setup Alice's data
    ?assertEqual(ok, erlcloud_s3:create_bucket(AlicesBucket, Alice)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(AlicesPublicBucket, public_read_write, Alice)),

    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(AlicesBucket, AlicesObject,
                                        <<"I'm here!!">>, Alice)),
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(AlicesBucket, AlicesPublicObject,
                                        <<"deadbeef">>, [{acl, public_read}], Alice)),
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(AlicesPublicBucket, AlicesObject,
                                        <<"deadly public beef">>, Alice)),

    %% setup Bob's box
    ?assertEqual(ok, erlcloud_s3:create_bucket(BobsBucket, Bob)),
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(BobsBucket, BobsObject,
                                        <<"bobfat">>, Bob)),

    %% >> setup Charlie's box
    ?assertEqual(ok, erlcloud_s3:create_bucket(CharliesBucket, Charlie)),

    %% >> Bob can do it right
    %% Bring Alice's objects to Bob's bucket
    ?assert403(erlcloud_s3:copy_object(BobsBucket, AlicesObject,
                                       AlicesBucket, AlicesObject, Bob)),

    ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
                 erlcloud_s3:copy_object(BobsBucket, AlicesPublicObject,
                                         AlicesBucket, AlicesPublicObject, Bob)),

    %% TODO: put to public bucket is degrated for now
    %% ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
    %%              erlcloud_s3:copy_object(BobsBucket, AlicesObject,
    %%                AlicesPublicBucket, AlicesObject, Bob)),

    %% Bring Bob's object to Alice's bucket
    ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
                 erlcloud_s3:copy_object(AlicesPublicBucket, BobsObject,
                                         BobsBucket, BobsObject, Bob)),
    %% Cleanup Bob's
    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(BobsBucket, AlicesPublicObject, Bob)),
    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(BobsBucket, AlicesObject, Bob)),
    %% ?assertEqual([{delete_marker, false}, {version_id, "null"}],
    %%              erlcloud_s3:delete_object(AlicesPublicObject, BobsObject, Bob)),

    %% >> Charlie can't do it
    %% try copying Alice's private object to Charlie's
    ?assert403(erlcloud_s3:copy_object(CharliesBucket, AlicesObject,
                                       AlicesBucket, AlicesObject, Charlie)),

    ?assert403(erlcloud_s3:copy_object(AlicesPublicBucket, AlicesObject,
                                       AlicesBucket, AlicesObject, Charlie)),

    %% try copy Alice's public object to Bob's
    ?assert403(erlcloud_s3:copy_object(BobsBucket, AlicesPublicObject,
                                       AlicesBucket, AlicesPublicObject, Charlie)),
    ?assert403(erlcloud_s3:copy_object(BobsBucket, AlicesObject,
                                       AlicesPublicBucket, AlicesObject, Charlie)),

    %% charlie tries to copy anonymously, which should fail in 403
    CSPort = Charlie#aws_config.s3_port,
    URL = lists:flatten(io_lib:format("http://~s.~s:~p/~s",
                                      [AlicesPublicBucket, Charlie#aws_config.s3_host,
                                       CSPort, AlicesObject])),
    Headers = [{"x-amz-copy-source", string:join([AlicesBucket, AlicesObject], "/")},
               {"Content-Length", 0}],
    {ok, Status, Hdr, _Msg} = ibrowse:send_req(URL, Headers, put, [],
                                               Charlie#aws_config.http_options),
    lager:debug("request ~p ~p => ~p ~p", [URL, Headers, Status, Hdr]),
    ?assertEqual("403", Status),

    ok.
