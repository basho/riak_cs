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

-module(object_get_test).

%% @doc `riak_test' module for testing object get behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET,        "riak-test-bucket").
-define(KEY_SINGLE_BLOCK,   "riak_test_key1").
-define(KEY_MULTIPLE_BLOCK, "riak_test_key2").

%% keys for multipart uploaded objects
-define(KEY_MP_TINY,        "riak_test_mp_tiny").  % single part, single block
-define(KEY_MP_SMALL,       "riak_test_mp_small"). % single part, multiple blocks
-define(KEY_MP_LARGE,       "riak_test_mp_large"). % multiple parts


confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

    non_mp_get_cases(UserConfig),
    mp_get_cases(UserConfig),
    pass.

non_mp_get_cases(UserConfig) ->
    %% setup objects
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),
    MultipleBlock = crypto:rand_bytes(4000000), % not aligned to block boundary
    erlcloud_s3:put_object(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock, UserConfig),

    %% basic GET test cases
    basic_get_test_case(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock, UserConfig),
    basic_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock, UserConfig),

    %% Range GET for single-block object test cases
    [range_get_test_case(?TEST_BUCKET, ?KEY_SINGLE_BLOCK, SingleBlock,
                         Range, UserConfig)
     || Range <- [{10, 20},
                  {0, none}, {none, 10},
                  {0, 0}, {0, 99}, {0, 1000000}]],

    %% Range GET for multiple-block object test cases
    [range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                         {0, End}, UserConfig)
     || End <- [mb(1)-2, mb(1)-1, mb(1), mb(1)+1]],
    [range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                         {Start, mb(2)}, UserConfig)
     || Start <- [mb(1)-2, mb(1)-1, mb(1), mb(1)+1]],
    range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                        {100, mb(3)}, UserConfig),
    range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                        {0, none}, UserConfig),

    %% Multiple ranges, CS returns whole resources.
    multiple_range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                                 [{10, 20}, {30, 50}], UserConfig),
    multiple_range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                                 [{10, 50}, {20, 50}], UserConfig),

    %% Invalid ranges
    LastPosExceeded = byte_size(MultipleBlock),
    invalid_range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                                [{LastPosExceeded, LastPosExceeded + 10}], UserConfig),
    invalid_range_get_test_case(?TEST_BUCKET, ?KEY_MULTIPLE_BLOCK, MultipleBlock,
                                [{20, 10}], UserConfig),
    ok.

mp_get_cases(UserConfig) ->
    TinyContent = multipart_upload(?TEST_BUCKET, ?KEY_MP_TINY,
                                   [400], UserConfig),
    SmallContent = multipart_upload(?TEST_BUCKET, ?KEY_MP_SMALL,
                                    [mb(3)], UserConfig),
    LargeContent = multipart_upload(?TEST_BUCKET, ?KEY_MP_LARGE,
                                    [mb(10), mb(5), mb(9) + 123, mb(6), mb(400)], % 30MB + 523 B
                                    UserConfig),

    %% Range GET for single part / single block
    [range_get_test_case(?TEST_BUCKET, ?KEY_MP_TINY, TinyContent,
                         Range, UserConfig)
     || Range <- [{10, 20},
                  {0, none}, {none, 10},
                  {0, 0}, {0, 99}, {0, 1000000}]],

    %% Range GET for single part / multiple blocks
    [range_get_test_case(?TEST_BUCKET, ?KEY_MP_SMALL, SmallContent,
                         {Start, End}, UserConfig)
     || Start <- [100, mb(1)+100],
        End   <- [mb(1)+100, mb(3)-1, mb(4)]],

    %% Range GET for multiple parts / multiple blocks
    [range_get_test_case(?TEST_BUCKET, ?KEY_MP_LARGE, LargeContent,
                         {Start, End}, UserConfig)
     || Start <- [mb(1), mb(16)],
        End   <- [mb(16), mb(30), mb(30) + 500, mb(1000)]],
    ok.

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

basic_get_test_case(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    assert_whole_content(ExpectedContent, Obj).

assert_whole_content(ExpectedContent, ResultObj) ->
    Content = proplists:get_value(content, ResultObj),
    ContentLength = proplists:get_value(content_length, ResultObj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).

range_get_test_case(Bucket, Key, WholeContent, {Start, End}, Config) ->
    Range = format_ranges([{Start, End}]),
    Obj = erlcloud_s3:get_object(Bucket, Key, [{range, Range}], Config),
    Content = proplists:get_value(content, Obj),
    ContentLength = proplists:get_value(content_length, Obj),
    WholeSize = byte_size(WholeContent),
    {Skip, Length} = range_skip_length({Start, End}, WholeSize),
    ?assertEqual(Length, list_to_integer(ContentLength)),
    ?assertEqual(Length, byte_size(Content)),
    assert_content_range(Skip, Length, WholeSize, Obj),
    ExpectedContent = binary:part(WholeContent, Skip, Length),
    ?assertEqual(ExpectedContent, Content).

multiple_range_get_test_case(Bucket, Key, WholeContent, Ranges, Config) ->
    RangeValue = format_ranges(Ranges),
    Obj = erlcloud_s3:get_object(Bucket, Key, [{range, RangeValue}], Config),
    assert_whole_content(WholeContent, Obj).

invalid_range_get_test_case(Bucket, Key, _WholeContent, Ranges, Config) ->
    RangeValue = format_ranges(Ranges),
    {'EXIT', {{aws_error, {http_error, 416, _, Body}}, _Backtrace}} =
        (catch erlcloud_s3:get_object(Bucket, Key, [{range, RangeValue}], Config)),
    ?assertMatch({match, _},
                 re:run(Body, "InvalidRange", [multiline])).

format_ranges(Ranges) ->
    Formatted = [format_range(Range) || Range <- Ranges],
    io_lib:format("bytes=~s", [string:join(Formatted, ",")]).

format_range(Range) ->
    RangeStr = case Range of
                   {none, End} ->
                       io_lib:format("-~B", [End]);
                   {Start, none} ->
                       io_lib:format("~B-", [Start]);
                   {Start, End} ->
                       io_lib:format("~B-~B", [Start, End])
               end,
    lists:flatten(RangeStr).

assert_content_range(Skip, Length, Size, Obj) ->
    Expected = lists:flatten(
                 io_lib:format("bytes ~B-~B/~B", [Skip, Skip + Length - 1, Size])),
    Headers = proplists:get_value(headers, Obj),
    ContentRange = proplists:get_value("Content-Range", Headers),
    ?assertEqual(Expected, ContentRange).

%% TODO: riak_test includes its own mochiweb by escriptizing.
%% End position which is lager than size is fixed on the branch 1.5 of basho/mochweb:
%%   https://github.com/basho/mochiweb/commit/38992be7822ddc1b8e6f318ba8e73fc8c0b7fd22
%%   Accept range end position which exceededs the resource size
%% After mochiweb is tagged, change riakhttpc and webmachine's deps to the tag.
%% So this function should be removed and replaced by mochiweb_http:range_skip_length/2.
range_skip_length(Spec, Size) ->
    case Spec of
        {Start, End} when is_integer(Start), is_integer(End),
                          0 =< Start, Start < Size, Size =< End ->
            {Start, Size - Start};
        _ ->
            mochiweb_http:range_skip_length(Spec, Size)
    end.

multipart_upload(Bucket, Key, Sizes, Config) ->
    InitRes = erlcloud_s3_multipart:initiate_upload(
                Bucket, Key, "text/plain", [], Config),
    UploadId = erlcloud_xml:get_text(
                 "/InitiateMultipartUploadResult/UploadId", InitRes),
    Content = upload_parts(Bucket, Key, UploadId, Config, 1, Sizes, [], []),
    basic_get_test_case(Bucket, Key, Content, Config),
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
