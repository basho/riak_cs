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
-define(assertHeader(Key, Expected, Props),
        ?assertEqual(Expected,
                     proplists:get_value(Key, proplists:get_value(headers, Props)))).

-define(BUCKET, "put-copy-bucket-test").
-define(KEY, "pocket").

-define(DATA0, <<"pocket">>).

-define(BUCKET2, "put-target-bucket").
-define(KEY2, "sidepocket").
-define(KEY3, "superpocket").
-define(BUCKET3, "the-other-put-target-bucket").
-define(BUCKET4, "no-cl-bucket").
-define(SRC_KEY, "source").
-define(TGT_KEY, "target").
-define(MP_TGT_KEY, "mp-target").
-define(REPLACE_KEY, "replace-target").

confirm() ->
    {UserConfig, {RiakNodes, _CSNodes, _}} = rtcs:setup(1),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    Data = ?DATA0,
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY, Data, UserConfig)),
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(?BUCKET, ?KEY2, Data, UserConfig)),

    RiakNode = hd(RiakNodes),
    UserConfig2 = rtcs_admin:create_user(RiakNode, 1),
    UserConfig3 = rtcs_admin:create_user(RiakNode, 1),

    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET2, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET3, UserConfig2)),

    ok = verify_simple_copy(UserConfig),
    ok = verify_others_copy(UserConfig, UserConfig2),
    ok = verify_multipart_copy(UserConfig),
    ok = verify_security(UserConfig, UserConfig2, UserConfig3),
    ok = verify_source_not_found(UserConfig),
    ok = verify_replace_usermeta(UserConfig),
    ok = verify_without_cl_header(UserConfig),

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

verify_source_not_found(UserConfig) ->
    NonExistingKey = "non-existent-source",
    {'EXIT', {{aws_error, {http_error, 404, _, ErrorXml}}, _Stack}} =
        (catch erlcloud_s3:copy_object(?BUCKET2, ?KEY2,
                                       ?BUCKET, NonExistingKey, UserConfig)),
    lager:debug("ErrorXml: ~s", [ErrorXml]),
    ?assert(string:str(ErrorXml,
                       "<Resource>/" ++ ?BUCKET ++
                           "/" ++ NonExistingKey ++ "</Resource>") > 0).

verify_replace_usermeta(UserConfig) ->
    lager:info("Verify replacing usermeta using Put Copy"),

    %% Put Initial Object
    Headers0 = [{"Content-Type", "text/plain"}],
    Options0 = [{meta, [{"key1", "val1"}, {"key2", "val2"}]}],
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(?BUCKET, ?REPLACE_KEY, ?DATA0, Options0, Headers0, UserConfig)),
    Props0 = erlcloud_s3:get_object(?BUCKET, ?REPLACE_KEY, UserConfig),
    lager:debug("Initial Obj: ~p", [Props0]),
    ?assertEqual("text/plain", proplists:get_value(content_type, Props0)),
    ?assertHeader("x-amz-meta-key1", "val1", Props0),
    ?assertHeader("x-amz-meta-key2", "val2", Props0),

    %% Replace usermeta using Put Copy
    Headers1 = [{"Content-Type", "application/octet-stream"}],
    Options1 = [{metadata_directive, "REPLACE"},
                {meta, [{"key3", "val3"}, {"key4", "val4"}]}],
    ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
                 erlcloud_s3:copy_object(?BUCKET, ?REPLACE_KEY, ?BUCKET, ?REPLACE_KEY,
                                         Options1, Headers1, UserConfig)),
    Props1 = erlcloud_s3:get_object(?BUCKET, ?REPLACE_KEY, UserConfig),
    lager:debug("Updated Obj: ~p", [Props1]),
    ?assertEqual(?DATA0, proplists:get_value(content, Props1)),
    ?assertEqual("application/octet-stream", proplists:get_value(content_type, Props1)),
    ?assertHeader("x-amz-meta-key1", undefined, Props1),
    ?assertHeader("x-amz-meta-key2", undefined, Props1),
    ?assertHeader("x-amz-meta-key3", "val3", Props1),
    ?assertHeader("x-amz-meta-key4", "val4", Props1),
    ok.


%% Verify reuqests without Content-Length header, they should succeed.
%% To avoid automatic Content-Length header addition by HTTP client library,
%% this test uses `curl' command line utitlity, intended.
verify_without_cl_header(UserConfig) ->
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET4, UserConfig)),
    Data = ?DATA0,
    ?assertEqual([{version_id, "null"}],
                 erlcloud_s3:put_object(?BUCKET4, ?SRC_KEY, Data, UserConfig)),
    verify_without_cl_header(UserConfig, normal, Data),
    verify_without_cl_header(UserConfig, mp, Data),
    ok.

verify_without_cl_header(UserConfig, normal, Data) ->
    lager:info("Verify basic (non-MP) PUT copy without Content-Length header"),
    Target = fmt("/~s/~s", [?BUCKET4, ?TGT_KEY]),
    Source = fmt("/~s/~s", [?BUCKET4, ?SRC_KEY]),
    _Res = exec_curl(UserConfig, "PUT", Target, [{"x-amz-copy-source", Source}]),

    Props = erlcloud_s3:get_object(?BUCKET4, ?TGT_KEY, UserConfig),
    ?assertEqual(Data, proplists:get_value(content, Props)),
    ok;
verify_without_cl_header(UserConfig, mp, Data) ->
    lager:info("Verify Multipart upload copy without Content-Length header"),
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(
                      ?BUCKET4, ?MP_TGT_KEY, "application/octet-stream",
                      [], UserConfig),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),
    lager:info("~p ~p", [InitUploadRes, UploadId]),
    Source = fmt("/~s/~s", [?BUCKET4, ?SRC_KEY]),
    MpTarget = fmt("/~s/~s?partNumber=1&uploadId=~s", [?BUCKET4, ?MP_TGT_KEY, UploadId]),
    _Res = exec_curl(UserConfig, "PUT", MpTarget,
                     [{"x-amz-copy-source", Source},
                      {"x-amz-copy-source-range", "bytes=1-2"}]),

    ListPartsXml = erlcloud_s3_multipart:list_parts(?BUCKET4, ?MP_TGT_KEY, UploadId, [], UserConfig),
    lager:debug("ListParts: ~p", [ListPartsXml]),
    ListPartsRes = erlcloud_s3_multipart:parts_to_term(ListPartsXml),
    Parts = proplists:get_value(parts, ListPartsRes),
    EtagList = [{PartNum, Etag} || {PartNum, [{etag, Etag}, {size, _Size}]} <- Parts],
    lager:debug("EtagList: ~p", [EtagList]),
    ?assertEqual(ok, erlcloud_s3_multipart:complete_upload(
                       ?BUCKET4, ?MP_TGT_KEY, UploadId, EtagList, UserConfig)),
    Props = erlcloud_s3:get_object(?BUCKET4, ?MP_TGT_KEY, UserConfig),
    ExpectedBody = binary:part(Data, 1, 2),
    ?assertEqual(ExpectedBody, proplists:get_value(content, Props)),
    ok.

exec_curl(#aws_config{s3_port=Port} = UserConfig, Method, Resource, AmzHeaders) ->
    ContentType = "application/octet-stream",
    Date = httpd_util:rfc1123_date(),
    Auth = rtcs_admin:make_authorization(Method, Resource, ContentType, UserConfig, Date,
                                   AmzHeaders),
    HeaderArgs = [fmt("-H '~s: ~s' ", [K, V]) ||
                     {K, V} <- [{"Date", Date}, {"Authorization", Auth},
                                {"Content-Type", ContentType} | AmzHeaders]],
    Cmd="curl -X " ++ Method ++ " -v -s " ++ HeaderArgs ++
        "'http://127.0.0.1:" ++ integer_to_list(Port) ++ Resource ++ "'",
    lager:debug("Curl command line: ~s", [Cmd]),
    Res = os:cmd(Cmd),
    lager:debug("Curl result: ~s", [Res]),
    Res.

fmt(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).
