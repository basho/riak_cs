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

-module(rtcs_multipart).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%% Upload object by multipart and return generetad (=expected) content
multipart_upload(Bucket, Key, Sizes, Config) ->
    InitRes = erlcloud_s3_multipart:initiate_upload(
                Bucket, Key, "text/plain", [], Config),
    UploadId = erlcloud_xml:get_text(
                 "/InitiateMultipartUploadResult/UploadId", InitRes),
    Content = upload_parts(Bucket, Key, UploadId, Config, 1, Sizes, [], []),
    Content.

upload_parts(Bucket, Key, UploadId, Config, _PartCount, [], Contents, Parts) ->
    ?assertEqual(ok, erlcloud_s3_multipart:complete_upload(
                       Bucket, Key, UploadId, lists:reverse(Parts), Config)),
    iolist_to_binary(lists:reverse(Contents));
upload_parts(Bucket, Key, UploadId, Config, PartCount, [Size | Sizes], Contents, Parts) ->
    Content = crypto:rand_bytes(Size),
    {RespHeaders, _UploadRes} = erlcloud_s3_multipart:upload_part(
                                  Bucket, Key, UploadId, PartCount, Content, Config),
    PartEtag = proplists:get_value("ETag", RespHeaders),
    lager:debug("UploadId: ~p~n", [UploadId]),
    lager:debug("PartEtag: ~p~n", [PartEtag]),
    upload_parts(Bucket, Key, UploadId, Config, PartCount + 1,
                 Sizes, [Content | Contents], [{PartCount, PartEtag} | Parts]).

upload_part_copy(BucketName, Key, UploadId, PartNum, SrcBucket, SrcKey, Config) ->
    upload_part_copy(BucketName, Key, UploadId, PartNum, SrcBucket, SrcKey, undefined, Config).

upload_part_copy(BucketName, Key, UploadId, PartNum, SrcBucket, SrcKey, SrcRange, Config) ->
    Url = "/" ++ Key,
    Source = filename:join([SrcBucket, SrcKey]),
    Subresources = [{"partNumber", integer_to_list(PartNum)},
                    {"uploadId", UploadId}],
    Headers = [%%{"content-length", byte_size(PartData)},
               {"x-amz-copy-source", Source} |
               source_range(SrcRange)],
    erlcloud_s3:s3_request(Config, put, BucketName, Url,
                           Subresources, [], {<<>>, []}, Headers).

source_range(undefined) -> [];
source_range({First, Last}) ->
    [{"x-amz-copy-source-range",
      lists:flatten(io_lib:format("bytes=~b-~b", [First, Last]))}].

upload_and_assert_part(Bucket, Key, UploadId, PartNum, PartData, Config) ->
    {RespHeaders, _UploadRes} = erlcloud_s3_multipart:upload_part(Bucket, Key, UploadId, PartNum, PartData, Config),
    assert_part(Bucket, Key, UploadId, PartNum, Config, RespHeaders).


assert_part(Bucket, Key, UploadId, PartNum, Config, RespHeaders) ->
    PartEtag = proplists:get_value("ETag", RespHeaders),
    PartsTerm = erlcloud_s3_multipart:parts_to_term(
                  erlcloud_s3_multipart:list_parts(Bucket, Key, UploadId, [], Config)),
    %% lager:debug("~p", [PartsTerm]),
    Parts = proplists:get_value(parts, PartsTerm),
    ?assertEqual(Bucket, proplists:get_value(bucket, PartsTerm)),
    ?assertEqual(Key, proplists:get_value(key, PartsTerm)),
    ?assertEqual(UploadId, proplists:get_value(upload_id, PartsTerm)),
    verify_part(PartEtag, proplists:get_value(PartNum, Parts)),
    PartEtag.

verify_part(_, undefined) ->
    ?assert(false);
verify_part(ExpectedEtag, PartInfo) ->
    ?assertEqual(ExpectedEtag, proplists:get_value(etag, PartInfo)).
