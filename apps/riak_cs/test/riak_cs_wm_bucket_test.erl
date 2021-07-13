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

-module(riak_cs_wm_bucket_test).

-compile(export_all).
-compile(nowarn_export_all).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("eunit/include/eunit.hrl").

bucket_test_() ->
    {setup,
     fun riak_cs_wm_test_utils:setup/0,
     fun riak_cs_wm_test_utils:teardown/1,
     [fun create_bucket_and_list_keys/0]}.

%% @doc Test to see that a newly created
%%      bucket has no keys.

%% XXX TODO: MAKE THESE ACTUALLY TEST SOMETHING
%% The state needed for this test
%% scares me
create_bucket_and_list_keys() ->
    _PathInfo = dict:from_list([{bucket, "create_bucket_test"}]),
    %% _RD = #wm_reqdata{path_info = PathInfo},
    %% _Ctx = #context{},
    ?assert(true).
%%  {Result, _, _} = riak_cs_wm_bucket:to_json(RD, Ctx),
%%  ?assertEqual(mochijson2:encode([]), Result).
