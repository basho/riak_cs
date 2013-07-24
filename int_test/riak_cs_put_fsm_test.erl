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

-module(riak_cs_put_fsm_test).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

setup() ->
    application:start(lager),
    application:start(folsom),
    {ok, _} = riak_cs_stats:start_link().

%% TODO:
%% Implement this
teardown(_) ->
    application:stop(folsom),
    application:stop(lager),
    ok.

put_fsm_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [fun small_put/0,
      fun zero_byte_put/0]}.

small_put() ->
    {ok, RiakPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, Pid} =
        riak_cs_put_fsm:start_link({<<"bucket">>, <<"key">>, 10, <<"ctype">>,
                                      orddict:new(), 2, riak_cs_acl_utils:default_acl("display", "canonical_id", "key_id"),
                                     60000, self(), RiakPid}),
    Data = <<"0123456789">>,
    Md5 = make_md5(Data),
    Parts = [<<"0">>, <<"123">>, <<"45">>, <<"678">>, <<"9">>],
    [riak_cs_put_fsm:augment_data(Pid, D) || D <- Parts],
    {ok, Manifest} = riak_cs_put_fsm:finalize(Pid, undefined),
    ?assert(Manifest?MANIFEST.state == active andalso
            Manifest?MANIFEST.content_md5==Md5).

zero_byte_put() ->
    {ok, RiakPid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, Pid} =
        riak_cs_put_fsm:start_link({<<"bucket">>, <<"key">>, 0, <<"ctype">>,
                                      orddict:new(), 2, riak_cs_acl_utils:default_acl("display", "canonical_id", "key_id"),
                                     60000, self(), RiakPid}),
    Data = <<>>,
    Md5 = make_md5(Data),
    {ok, Manifest} = riak_cs_put_fsm:finalize(Pid, undefined),
    ?assert(Manifest?MANIFEST.state == active andalso
            Manifest?MANIFEST.content_md5==Md5).

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_md5(Bin) ->
    M = crypto:md5_init(),
    M1 = crypto:md5_update(M, Bin),
    crypto:md5_final(M1).
