%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(rtcs_object).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

upload(UserConfig, normal, B, K) ->
    Content = crypto:rand_bytes(mb(4)),
    erlcloud_s3:put_object(B, K, Content, UserConfig),
    {B, K, Content};
upload(UserConfig, multipart, B, K) ->
    Content = rtcs_multipart:multipart_upload(B, K, [mb(10), 400], UserConfig),
    {B, K, Content}.

upload(UserConfig, normal_copy, B, DstK, SrcK) ->
    ?assertEqual([{copy_source_version_id, "false"}, {version_id, "null"}],
                 erlcloud_s3:copy_object(B, DstK, B, SrcK, UserConfig));
upload(UserConfig, multipart_copy, B, DstK, SrcK) ->
    InitUploadRes = erlcloud_s3_multipart:initiate_upload(B, DstK, "text/plain", [], UserConfig),
    UploadId = erlcloud_s3_multipart:upload_id(InitUploadRes),

    {RespHeaders1, _} = rtcs_multipart:upload_part_copy(
                          B, DstK, UploadId, 1, B, SrcK, {0, mb(5)-1}, UserConfig),
    Etag1 = rtcs_multipart:assert_part(B, DstK, UploadId, 1, UserConfig, RespHeaders1),
    {RespHeaders2, _} = rtcs_multipart:upload_part_copy(
                          B, DstK, UploadId, 2, B, SrcK, {mb(5), mb(10)+400-1}, UserConfig),
    Etag2 = rtcs_multipart:assert_part(B, DstK, UploadId, 2, UserConfig, RespHeaders2),

    EtagList = [ {1, Etag1}, {2, Etag2} ],
    ?assertEqual(ok, erlcloud_s3_multipart:complete_upload(
                       B, DstK, UploadId, EtagList, UserConfig)).

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

assert_whole_content(UserConfig, Bucket, Key, ExpectedContent) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, UserConfig),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).
