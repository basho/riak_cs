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

-module(riak_cs_utils_test).

-compile(export_all).

-ifdef(TEST).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
    %% Silence logging
    application:load(sasl),
    application:load(riak_cs),
    application:set_env(sasl, sasl_error_logger, {file, "riak_cs_utils_sasl.log"}),
    error_logger:tty(false),

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
    application:start(riak_cs).

%% TODO:
%% Implement this
teardown(_) ->
    ok.

riak_cs_utils_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun teardown/1,
       fun(_X) ->
               [
               ]
       end
      }]}.

%% @doc Make sure that the name
%%      of the user created is the
%%      name that makes it into
%%      the moss_user record
name_matches() ->
    Name = "fooser",
    Email = "fooser@fooser.com",
    {ok, User} = riak_cs_utils:create_user(Name, Email),
    {"name matches test",
     fun() ->
             [
              ?_assertEqual(Name, User?RCS_USER.name)
             ]
     end
    }.

%% @doc Make sure that when we create
%%      a new user and one bucket for that
%%      user, that that bucket is the only
%%      one owned by that user
bucket_appears() ->
    Name = "fooser",
    Email = "fooser@fooser.com",
    BucketName = "fooser-bucket",
    {ok, User} = riak_cs_utils:create_user(Name, Email),
    KeyID = User?RCS_USER.key_id,
    riak_cs_utils:create_bucket(KeyID, BucketName),
    {ok, UserAfter} = riak_cs_utils:get_user(KeyID),
    AfterBucketNames = [B#moss_bucket.name ||
                           B <- riak_cs_utils:get_buckets(UserAfter)],
    {"bucket appears test",
     fun() ->
             [
              ?_assertEqual([BucketName], AfterBucketNames)
             ]
     end
    }.

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
    Email = "fooser@fooser.com",
    BucketName = "key_appears",
    KeyName = "testkey",
    {ok, User} = riak_cs_utils:create_user(Name, Email),
    KeyID = User?RCS_USER.key_id,
    ok = riak_cs_utils:create_bucket(KeyID, BucketName),
    riak_cs_utils:put_object(BucketName, KeyName,
                               "value", dict:new()),

    {ok, ListKeys} = riak_cs_utils:list_keys(BucketName),
    {"key appears test",
     fun() ->
             [
              ?_assert(lists:member(list_to_binary(KeyName), ListKeys))
             ]
     end
    }.

%% @doc Make sure that storing a doc
%%      with a content type in the metadata
%%      keeps the content-type on GET
content_type_sticks() ->
    Name = "fooser",
    Email = "fooser@fooser.com",
    BucketName = "content_type_sticks",
    KeyName = "testkey",
    CType = "x-foo/bar",
    Metadata = dict:from_list([{<<"content-type">>, CType}]),
    {ok, User} = riak_cs_utils:create_user(Name, Email),
    KeyID = User?RCS_USER.key_id,
    ok = riak_cs_utils:create_bucket(KeyID, BucketName),
    riak_cs_utils:put_object(BucketName, KeyName,
                               <<"value">>, Metadata),
    {ok, Object} = riak_cs_utils:get_object(BucketName, KeyName),
    {"content type sticks test",
     fun() ->
             [
              ?_assertEqual(CType, riakc_obj:get_content_type(Object))
             ]
     end
    }.

%% TODO:
%% This would make a great
%% EQC test
keys_are_sorted() ->
    Name = "fooser",
    Email = "fooser@fooser.com",
    BucketName = "keys_are_sorted",
    Keys = [<<"dog">>, <<"zebra">>, <<"aardvark">>, <<01>>, <<"panda">>],

    {ok, User} = riak_cs_utils:create_user(Name, Email),
    KeyID = User?RCS_USER.key_id,
    ok = riak_cs_utils:create_bucket(KeyID, BucketName),

    [riak_cs_utils:put_object(BucketName, binary_to_list(KeyName),
                                "value", dict:new()) ||
        KeyName <- Keys],

    {ok, ListKeys} = riak_cs_utils:list_keys(BucketName),
    {"keys are sorted test",
     fun() ->
             [
              ?_assertEqual(lists:sort(Keys), ListKeys)
             ]
     end
    }.

%% @doc Make sure that when we save an object,
%%      we can later retrieve it and get the same
%%      value
object_returns() ->
    %% TODO:
    %% it's starting to get kind of annoying
    %% that we need a KeyID to store objects
    _KeyID = "0",
    BucketName = "object_returns",
    KeyName = "testkey",
    Value = <<"value">>,
    Metadata = dict:new(),

    riak_cs_utils:put_object(BucketName, KeyName,
                               Value, Metadata),

    {ok, RetrievedObject} = riak_cs_utils:get_object(BucketName, KeyName),
    {"object returns test",
     fun() ->
             [
              ?_assertEqual(Value, riakc_obj:get_value(RetrievedObject))
             ]
     end
    }.

%% @doc Make sure that after creating an
%%      object and deleting it that it
%%      no longer exists
object_deletes() ->
    _KeyID = "0",
    BucketName = "object_deletes",
    KeyName = "testkey",
    Value = <<"value">>,
    Metadata = dict:new(),

    riak_cs_utils:put_object(BucketName, KeyName,
                               Value, Metadata),

    ok = riak_cs_utils:delete_object(BucketName, KeyName),
    {error, Reason} = riak_cs_utils:get_object(BucketName, KeyName),
    {"object deletes test",
     fun() ->
             [
              ?_assertEqual(Reason, notfound)
             ]
     end
    }.

%% @doc Make sure that an object
%%      that doesn't exist still
%%      deletes fine
nonexistent_deletes() ->
    Return = riak_cs_utils:delete_object("doesn't", "exist"),
    {"nonexistenet deletes test",
     fun() ->
             [
              ?_assertEqual(Return, ok)
             ]
     end
    }.

bucket_resolution_test() ->
    %% @TODO Replace or augment this with eqc testing.
    UserRecord = riak_cs_utils:user_record("uncle fester",
                                           "fester@tester.com",
                                           "festersquest",
                                           "wasthebest"),
    BucketList1 = [riak_cs_utils:bucket_record(<<"bucket1">>, create),
                   riak_cs_utils:bucket_record(<<"bucket2">>, create),
                   riak_cs_utils:bucket_record(<<"bucket3">>, create)],
    BucketList2 = [riak_cs_utils:bucket_record(<<"bucket1">>, create),
                   riak_cs_utils:bucket_record(<<"bucket1">>, create),
                   riak_cs_utils:bucket_record(<<"bucket1">>, create)],
    BucketList3 = [riak_cs_utils:bucket_record(<<"bucket1">>, create),
                   riak_cs_utils:bucket_record(<<"bucket1">>, delete),
                   riak_cs_utils:bucket_record(<<"bucket1">>, create)],
    BucketList4 = [riak_cs_utils:bucket_record(<<"bucket1">>, create),
                   riak_cs_utils:bucket_record(<<"bucket1">>, create),
                   riak_cs_utils:bucket_record(<<"bucket1">>, delete)],
    BucketList5 = [riak_cs_utils:bucket_record(<<"bucket1">>, delete),
                   riak_cs_utils:bucket_record(<<"bucket1">>, delete),
                   riak_cs_utils:bucket_record(<<"bucket1">>, delete)],
    Obj1 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList1]),
    Obj2 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList2]),
    Obj3 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList3]),
    Obj4 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList4]),
    Obj5 = riakc_obj:new_obj(<<"bucket">>,
                            <<"key">>,
                            <<"value">>,
                            [{[], UserRecord?RCS_USER{buckets=[Buckets]}} ||
                                Buckets <- BucketList5]),
    Values1 = riakc_obj:get_values(Obj1),
    Values2 = riakc_obj:get_values(Obj2),
    Values3 = riakc_obj:get_values(Obj3),
    Values4 = riakc_obj:get_values(Obj4),
    Values5 = riakc_obj:get_values(Obj5),
    ResBuckets1 = riak_cs_utils:resolve_buckets(Values1, [], true),
    ResBuckets2 = riak_cs_utils:resolve_buckets(Values2, [], true),
    ResBuckets3 = riak_cs_utils:resolve_buckets(Values3, [], true),
    ResBuckets4 = riak_cs_utils:resolve_buckets(Values4, [], true),
    ResBuckets5 = riak_cs_utils:resolve_buckets(Values5, [], true),
    ?assertEqual(BucketList1, ResBuckets1),
    ?assertEqual([hd(BucketList2)], ResBuckets2),
    ?assertEqual([hd(lists:reverse(BucketList3))], ResBuckets3),
    ?assertEqual([hd(lists:reverse(BucketList4))], ResBuckets4),
    ?assertEqual([hd(BucketList5)], ResBuckets5).

-endif.
