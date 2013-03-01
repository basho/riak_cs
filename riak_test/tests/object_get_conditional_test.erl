-module(object_get_conditional_test).

%% @doc `riak_test' module for testing conditional object get behavior.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(TEST_BUCKET, "riak_test_bucket").
-define(TEST_KEY,    "riak_test_key1").

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

    {Content, Etag, {Before, After}} = setup_object(?TEST_BUCKET, ?TEST_KEY, UserConfig),
    lager:debug("{Etag, {Before, After}}: ~p~n", [{Etag, {Before, After}}]),

    last_modified_condition_test_cases(?TEST_BUCKET, ?TEST_KEY,
                                       Content, {Before, After}, UserConfig),

    pass.

setup_object(Bucket, Key, UserConfig) ->
    Content = crypto:rand_bytes(400),
    erlcloud_s3:put_object(Bucket, Key, Content, UserConfig),
    Obj = erlcloud_s3:get_object(Bucket, Key, UserConfig),
    ?assertEqual(Content, proplists:get_value(content, Obj)),
    Etag = proplists:get_value(etag, Obj),
    {Before, After} = before_and_after_of_last_modified(Obj),
    {Content, Etag, {Before, After}}.

before_and_after_of_last_modified(Obj) ->
    Headers = proplists:get_value(headers, Obj),
    RFC1123LastModified = proplists:get_value("last-modified", Headers),
    LastModified = httpd_util:convert_request_date(RFC1123LastModified),
    LastModifiedSec = calendar:datetime_to_gregorian_seconds(LastModified),
    Before = rfc1123_date(LastModifiedSec - 1),
    After = rfc1123_date(LastModifiedSec + 1),
    %% Sleep 1 sec because webmachine ignores if-modified-since header
    %% if it is future date.
    lager:warning("{RFC1123LastModified, Before, After}: ~p~n",
                  [{RFC1123LastModified, Before, After}]),
    timer:sleep(1000),
    {Before, After}.

rfc1123_date(GregorianSecs) ->
    ErlDate = calendar:gregorian_seconds_to_datetime(GregorianSecs),
    riak_cs_wm_utils:iso_8601_to_rfc_1123(riak_cs_wm_utils:iso_8601_datetime(ErlDate)).

last_modified_condition_test_cases(Bucket, Key, ExpectedContent,
                                   {Before, After}, UserConfig) ->
    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_modified_since, Before}], UserConfig),
    not_modified_case(Bucket, Key,
                      [{if_modified_since, After}], UserConfig),

    normal_get_case(Bucket, Key, ExpectedContent,
                    [{if_unmodified_since, After}], UserConfig),
    precondition_failed_case(Bucket, Key,
                             [{if_unmodified_since, Before}], UserConfig).

normal_get_case(Bucket, Key, ExpectedContent, Options, UserConfig) ->
    Obj = erlcloud_s3:get_object(Bucket, Key, Options, UserConfig),
    ?assertEqual(ExpectedContent, proplists:get_value(content, Obj)).

not_modified_case(Bucket, Key, Options, UserConfig) ->
    ?assertError({aws_error, {http_error, 304, "Not Modified", _Body}},
                erlcloud_s3:get_object(Bucket, Key, Options, UserConfig)).

precondition_failed_case(Bucket, Key, Options, UserConfig) ->
    ?assertError({aws_error, {http_error, 412, "Precondition Failed", _Body}},
                 erlcloud_s3:get_object(Bucket, Key, Options, UserConfig)).
