-module(cs436_regression_test).

%% @doc Regression test for `riak_cs' <a href="https://github.com/basho/riak_cs/issues/436">
%% issue 436</a>. The issue description is: A 500 is returned instead of a 404 when
%% trying to put to a nonexistent bucket.

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak-test-bucket").

confirm() ->
    {UserConfig, {_RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    ?assertError({aws_error, {http_error, 404, _, _}},
                 erlcloud_s3:put_object(?TEST_BUCKET,
                                        "somekey",
                                        crypto:rand_bytes(100),
                                        UserConfig)),

    %% Create and delete test bucket
    lager:info("creating bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?TEST_BUCKET, UserConfig)),

    ?assertMatch([{buckets, [[{name, ?TEST_BUCKET}, _]]}],
        erlcloud_s3:list_buckets(UserConfig)),

    lager:info("deleting bucket ~p", [?TEST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:delete_bucket(?TEST_BUCKET, UserConfig)),

    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    %% Attempt to put object again and ensure result is still 404
    ?assertError({aws_error, {http_error, 404, _, _}},
                 erlcloud_s3:put_object(?TEST_BUCKET,
                                        "somekey",
                                        crypto:rand_bytes(100),
                                        UserConfig)),
    pass.
