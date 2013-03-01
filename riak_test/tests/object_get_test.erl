-module(object_get_test).

%% @doc `riak_test' module for testing object get behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET,        "riak_test_bucket").
-define(KEY_SINGLE_BLOCK,   "riak_test_key1").
-define(KEY_MULTIPLE_BLOCK, "riak_test_key2").

confirm() ->
    {RiakNodes, _CSNodes, _Stanchion} = rtcs:setup(4),

    FirstNode = hd(RiakNodes),

    {AccessKeyId, SecretAccessKey} = rtcs:create_user(FirstNode, 1),

    %% User config
    UserConfig = rtcs:config(AccessKeyId, SecretAccessKey, rtcs:cs_port(FirstNode)),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
                 erlcloud_s3:list_buckets(UserConfig)),

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
    pass.

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

basic_get_test_case(Bucket, Key, ExpectedContent, Config) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Config),
    Content = proplists:get_value(content, Obj),
    ContentLength = proplists:get_value(content_length, Obj),
    ?assertEqual(byte_size(ExpectedContent), list_to_integer(ContentLength)),
    ?assertEqual(byte_size(ExpectedContent), byte_size(Content)),
    ?assertEqual(ExpectedContent, Content).

range_get_test_case(Bucket, Key, WholeContent, {Start, End}, Config) ->
    Range = case {Start, End} of
                {none, End} ->
                    io_lib:format("bytes=-~B", [End]);
                {Start, none} ->
                    io_lib:format("bytes=~B-", [Start]);
                {Start, End} ->
                    io_lib:format("bytes=~B-~B", [Start, End])
            end,
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

assert_content_range(Skip, Length, Size, Obj) ->
    Expected = lists:flatten(
                 io_lib:format("bytes ~B-~B/~B", [Skip, Skip + Length - 1, Size])),
    Headers = proplists:get_value(headers, Obj),
    ContentRange = proplists:get_value("content-range", Headers),
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
