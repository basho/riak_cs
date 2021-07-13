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

-module(riak_cs_wm_key_test).

-compile(export_all).
-compile(nowarn_export_all).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("eunit/include/eunit.hrl").

key_test_() ->
    {setup,
     fun riak_cs_wm_test_utils:setup/0,
     fun riak_cs_wm_test_utils:teardown/1,
     [fun get_object/0]}.

get_object() ->
    %% XXX TODO: MAKE THESE ACTUALLY TEST SOMETHING
    %% We use this instead of setting
    %% path info the wm_reqdata because
    %% riak_cs_wm_utils:ensure_doc uses
    %% it.
    %% _Ctx= #key_context{bucket="keytest", key="foo"},
    %% _RD = #wm_reqdata{},
    ?assert(true).
%%    {Object, _, _} = riak_cs_wm_key:produce_body(RD, Ctx),
%%    ?assertEqual(<<>>, Object).
