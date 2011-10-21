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
      fun bucket_appears/0,
      fun key_appears/0,
      fun object_returns/0
     ]}.

%% @doc Make sure that the name
%%      of the user created is the
%%      name that makes it into
%%      the moss_user record
name_matches() ->
    Name = "fooser",
    {ok, User} = riak_moss_riakc:create_user(Name),
    ?assertEqual(Name, User#moss_user.name).

%% @doc Make sure that when we create
%%      a new user and one bucket for that
%%      user, that that bucket is the only
%%      one owned by that user
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


%% @doc Make sure that when we create a key
%%      that is appears in list keys for that
%%      bucket
key_appears() ->
    %% TODO:
    %% How much of a hack is it
    %% that we have to pass the
    %% key in as a list instead
    %% of a binary?
    Name = "fooser",
    BucketName = "key_appears",
    KeyName = "testkey",
    {ok, User} = riak_moss_riakc:create_user(Name),
    KeyID = User#moss_user.key_id,
    ok = riak_moss_riakc:create_bucket(KeyID, BucketName),
    riak_moss_riakc:put_object(KeyID, BucketName, KeyName,
                                      "value", dict:new()),

    {ok, ListKeys} = riak_moss_riakc:list_keys(BucketName),
    ?assert(lists:member(list_to_binary(KeyName), ListKeys)).

%% @doc Make sure that when we save an object,
%%      we can later retrieve it and get the same
%%      value
object_returns() ->
    %% TODO:
    %% it's starting to get kind of annoying
    %% that we need a KeyID to store objects
    KeyID = "0",
    BucketName = "object_returns",
    KeyName = "testkey",
    Value = <<"value">>,
    Metadata = dict:new(),

    riak_moss_riakc:put_object(KeyID, BucketName, KeyName,
                                      Value, Metadata),

    {ok, RetrievedObject} = riak_moss_riakc:get_object(BucketName, KeyName),
    ?assertEqual(Value, riakc_obj:get_value(RetrievedObject)).
