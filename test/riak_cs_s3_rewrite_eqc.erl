%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_cs_s3_rewrite'.

-module(riak_cs_s3_rewrite_eqc).

-include("riak_cs.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% eqc property
-export([prop_extract_bucket_from_host/0]).

%% Helpers
-export([test/0,
         test/1]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(TEST_ITERATIONS, 1000).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {spawn,
     [
      {timeout, 30, ?_assertEqual(true, quickcheck(numtests(?TEST_ITERATIONS, ?QC_OUT(prop_extract_bucket_from_host()))))}
     ]
    }.

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_extract_bucket_from_host() ->
    ?FORALL({Bucket, BaseHost},{riak_cs_gen:bucket_or_blank(), base_host()},
            begin
                BucketStr = binary_to_list(Bucket),
                Host = compose_host(BucketStr, BaseHost),
                BaseHostIdx = string:str(Host, BaseHost),
                ExpectedBucket = expected_bucket(BucketStr, BaseHost),
                ResultBucket =
                    riak_cs_s3_rewrite:extract_bucket_from_host(Host,
                                                                BaseHostIdx),
                equals(ExpectedBucket, ResultBucket)
            end).

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_extract_bucket_from_host())).

base_host() ->
    oneof(["s3.amazonaws.com", "riakcs.net", "snarf", "hah-hah", ""]).

compose_host([], BaseHost) ->
    BaseHost;
compose_host(Bucket, []) ->
    Bucket;
compose_host(Bucket, BaseHost) ->
    Bucket ++ "." ++  BaseHost.

expected_bucket([], _BaseHost) ->
    undefined;
expected_bucket(_Bucket, []) ->
    undefined;
expected_bucket(Bucket, _) ->
    Bucket.

-endif.
