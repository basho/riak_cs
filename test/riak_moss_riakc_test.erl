%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_riakc_test).

-export([riakc_test_/0]).

-include("riak_moss.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:set_env(riak_moss, moss_ip, "127.0.0.1"),
    application:set_env(riak_moss, moss_port, 8080),
    %% Start erlang node
    application:start(sasl),
    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now()))),
    net_kernel:start([TestNode, shortnames]),
    application:start(lager),
    application:start(riakc),
    application:start(inets),
    application:start(mochiweb),
    application:start(webmachine),
    application:start(crypto),
    application:start(riakc),
    application:start(riak_moss).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

riakc_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun name_matches/0,
      fun bucket_appears/0
     ]}.

name_matches() ->
    Name = "fooser",
    {ok, User} = riak_moss_riakc:create_user(Name),
    ?assertEqual(Name, User#moss_user.name).

bucket_appears() ->
    Name = "fooser",
    BucketName = "fooser-bucket",
    {ok, User} = riak_moss_riakc:create_user(Name),
    KeyID = User#moss_user.key_id,
    riak_moss_riakc:create_bucket(KeyID, BucketName),
    {ok, UserAfter} = riak_moss_riakc:get_user(KeyID),
    AfterBucketNames = [B#moss_bucket.name ||
                               B <- riak_moss_riakc:get_buckets(UserAfter)],
    ?assertEqual([BucketName], AfterBucketNames).
