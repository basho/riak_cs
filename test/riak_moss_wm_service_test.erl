%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_wm_service_test).

-export([service_test_/0]).

-include("riak_moss.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("eunit/include/eunit.hrl").

service_test_() ->
    {setup,
     fun riak_moss_wm_test_utils:setup/0,
     fun riak_moss_wm_test_utils:teardown/1,
     [fun get_bucket_to_json/0]}.

get_bucket_to_json() ->
    BucketNames = ["foo", "bar", "baz"],
    UserName = "fooser",
    {ok, User} = riak_moss_riakc:create_user(UserName),
    KeyID = User#moss_user.key_id,
    [riak_moss_riakc:create_bucket(KeyID, Name) || Name <- BucketNames],
    {ok, UpdatedUser} = riak_moss_riakc:get_user(User#moss_user.key_id),
    CorrectJsonBucketNames = [list_to_binary(Name) ||
                                     Name <- lists:reverse(BucketNames)],
    EncodedCorrectNames = mochijson2:encode(CorrectJsonBucketNames),
    Context = #context{user=UpdatedUser},
    {ResultToTest, _, _} = riak_moss_wm_service:to_json(fake_rd, Context),
    ?assertEqual(EncodedCorrectNames, ResultToTest).
