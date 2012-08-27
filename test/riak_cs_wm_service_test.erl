%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_service_test).

-compile(export_all).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("eunit/include/eunit.hrl").

service_test_() ->
    {setup,
     fun riak_cs_wm_test_utils:setup/0,
     fun riak_cs_wm_test_utils:teardown/1,
     [
     ]}.

get_bucket_to_json() ->
    %% XXX TODO: MAKE THESE ACTUALLY TEST SOMETHING
    BucketNames = ["foo", "bar", "baz"],
    UserName = "fooser",
    Email = "fooser@fooser.com",
    {ok, User} = riak_cs_utils:create_user(UserName, Email),
    KeyID = User?RCS_USER.key_id,
    [riak_cs_utils:create_bucket(KeyID, Name) || Name <- BucketNames],
    {ok, UpdatedUser} = riak_cs_utils:get_user(User?RCS_USER.key_id),
    CorrectJsonBucketNames = [list_to_binary(Name) ||
                                     Name <- lists:reverse(BucketNames)],
    _EncodedCorrectNames = mochijson2:encode(CorrectJsonBucketNames),
    _Context = #context{user=UpdatedUser},
    ?assert(true).
    %%{ResultToTest, _, _} = riak_cs_wm_service:to_json(fake_rd, Context),
    %%?assertEqual(EncodedCorrectNames, ResultToTest).
