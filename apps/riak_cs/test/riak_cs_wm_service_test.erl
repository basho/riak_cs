%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_wm_service_test).

-compile(export_all).
-compile(nowarn_export_all).

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
    {ok, User} = riak_cs_user:create_user(UserName, Email),
    KeyID = User?RCS_USER.key_id,
    [riak_cs_utils:create_bucket(KeyID, Name) || Name <- BucketNames],
    {ok, _UpdatedUser} = riak_cs_user:get_user(User?RCS_USER.key_id),
    CorrectJsonBucketNames = [list_to_binary(Name) ||
                                     Name <- lists:reverse(BucketNames)],
    _EncodedCorrectNames = mochijson2:encode(CorrectJsonBucketNames),
    %%_Context = #context{user=UpdatedUser},
    ?assert(true).
    %%{ResultToTest, _, _} = riak_cs_wm_service:to_json(fake_rd, Context),
    %%?assertEqual(EncodedCorrectNames, ResultToTest).
