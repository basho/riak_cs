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
      fun keys_are_sorted/0,
      fun object_returns/0,
      fun object_deletes/0,
      fun nonexistent_deletes/0
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

%% TODO:
%% This would make a great
%% EQC test
keys_are_sorted() ->
    Name = "fooser",
    BucketName = "keys_are_sorted",
    Keys = [<<"dog">>, <<"zebra">>, <<"aardvark">>, <<01>>, <<"panda">>],

    {ok, User} = riak_moss_riakc:create_user(Name),
    KeyID = User#moss_user.key_id,
    ok = riak_moss_riakc:create_bucket(KeyID, BucketName),

    [riak_moss_riakc:put_object(KeyID, BucketName, binary_to_list(KeyName),
                                      "value", dict:new()) ||
                                       KeyName <- Keys],

    {ok, ListKeys} = riak_moss_riakc:list_keys(BucketName),
    ?assertEqual(lists:sort(Keys), ListKeys).

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

%% @doc Make sure that after creating an
%%      object and deleting it that it
%%      no longer exists
object_deletes() ->
    KeyID = "0",
    BucketName = "object_deletes",
    KeyName = "testkey",
    Value = <<"value">>,
    Metadata = dict:new(),

    riak_moss_riakc:put_object(KeyID, BucketName, KeyName,
                                      Value, Metadata),

    ok = riak_moss_riakc:delete_object(BucketName, KeyName),
    {error, Reason} = riak_moss_riakc:get_object(BucketName, KeyName),
    ?assertEqual(Reason, notfound).


%% @doc Make sure that an object
%%      that doesn't exist still
%%      deletes fine
nonexistent_deletes() ->
    Return = riak_moss_riakc:delete_object("doesn't", "exist"),
    ?assertEqual(Return, ok).
