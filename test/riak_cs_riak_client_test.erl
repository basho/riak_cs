%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2004 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak client test

-module(riak_cs_riak_client_test).

-compile(export_all).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(assert_pool_status(ReqActives, PbcActives),
        (fun() ->
                 ReqIdles = 3 - ReqActives,
                 PbcIdles = 5 - PbcActives,
                 ?assertEqual({ready, ReqIdles, 0, ReqActives},
                              poolboy:status(request_pool)),
                 ?assertEqual({ready, PbcIdles, 0, PbcActives},
                              poolboy:status(pbc_pool_master))
         end)()).

setup() ->
    poolboy:start_link([{name, {local, request_pool}},
                        {worker_module, riak_cs_riak_client},
                        {size, 3},
                        {max_overflow, 0},
                        {stop_fun, fun(P) -> riak_cs_riak_client:stop(P) end}],
                       []),
    poolboy:start_link([{name, {local, pbc_pool_master}},
                        {worker_module, riak_cs_riakc_pool_worker},
                        {size, 5},
                        {max_overflow, 0},
                        {stop_fun, fun(P) -> riak_cs_riakc_pool_worker:stop(P) end}],
                       []),
    ok.

teardown(_) ->
    poolboy:stop(request_pool),
    poolboy:stop(pbc_pool_master),
    ok.

pool_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      fun() ->
              ?assert_pool_status(0, 0),
              {ok, RcPid} = riak_cs_riak_client:checkout(),
              ?assert_pool_status(1, 0),
              {ok, _Pbc} = riak_cs_riak_client:master_pbc(RcPid),
              ?assert_pool_status(1, 1),
              ok = riak_cs_riak_client:checkin(RcPid),
              ?assert_pool_status(0, 0)
      end,
      fun() ->
              wait_process_finish(
                fun() ->
                        {ok, RcPid} = riak_cs_riak_client:checkout(),
                        {ok, _Pbc} = riak_cs_riak_client:manifest_pbc(RcPid)
                        %% Failed to checkin and exit normal
                end),
              ?assert_pool_status(0, 0)
      end,
      fun() ->
              Result = wait_process_result(
                         fun(From) ->
                                 {ok, RcPid} = riak_cs_riak_client:checkout(),
                                 {ok, _Pbc} = riak_cs_riak_client:manifest_pbc(RcPid),
                                 exit(RcPid, dummY),
                                 timer:sleep(100),
                                 From ! {ok, pool_status()}
                         end),
              ?debugVal(Result),
              ?assertEqual(empty_pool(), Result)
      end,
      fun() ->
              Result = wait_process_result(
                         fun(From) ->
                                 {ok, RcPid} = riak_cs_riak_client:checkout(),
                                 {ok, Pbc} = riak_cs_riak_client:manifest_pbc(RcPid),
                                 exit(Pbc, dummY),
                                 timer:sleep(100),
                                 From ! {ok, pool_status()}
                         end),
              ?debugVal(Result),
              ?assertEqual(empty_pool(), Result)
      end,
      fun() ->
              wait_process_finish(
                fun() ->
                        {ok, RcPid} = riak_cs_riak_client:start_link([]),
                        {ok, _Pbc} = riak_cs_riak_client:manifest_pbc(RcPid),
                        spawn_link(fun() -> exit(dummY) end),
                        timer:sleep(1000) % Wait the above process dies
                end),
              ?assert_pool_status(0, 0)
      end,
      %% Process (A) which spawned RcPid dies normally first, and
      %% another process linked to (A) dies abnormally.  At the time,
      %% A has already gone, RcPid does NOT receive EXIT signal.
      %%
      %% fun() ->
      %%         wait_process_finish(
      %%           fun() ->
      %%                   {ok, RcPid} = riak_cs_riak_client:start_link([]),
      %%                   {ok, _Pbc} = riak_cs_riak_client:manifest_pbc(RcPid),
      %%                   spawn_link(fun() -> timer:sleep(100), % Wait spawner dies
      %%                                       exit(dummY) end)
      %%           end),
      %%         timer:sleep(1000),
      %%         ?assert_pool_status(0, 0)
      %% end,
      fun() ->
              wait_process_finish(
                fun() ->
                        {ok, RcPid} = riak_cs_riak_client:start_link([]),
                        {ok, _Pbc} = riak_cs_riak_client:manifest_pbc(RcPid),
                        exit(RcPid, dummY)
                end),
              ?assert_pool_status(0, 0)
      end,
      fun() ->
              wait_process_finish(
                fun() ->
                        {ok, RcPid} = riak_cs_riak_client:start_link([]),
                        {ok, Pbc} = riak_cs_riak_client:manifest_pbc(RcPid),
                        _ = exit(Pbc, dummY),
                        ?assert_pool_status(0, 0)
                end)
      end
     ]}.

pool_status() ->
    [{request_pool, poolboy:status(request_pool)},
     {pbc_pool_master, poolboy:status(pbc_pool_master)}].

empty_pool() ->
    [{request_pool, {ready, 3, 0, 0}},
     {pbc_pool_master, {ready, 5, 0, 0}}].

wait_process_result(Fun) when is_function(Fun) ->
    Self = self(),
    wait_process_finish(fun() -> Fun(Self) end),
    receive
        {ok, Res} -> Res
    after 10000 ->
            exit(timeout)
    end.

wait_process_finish(Fun) when is_function(Fun) ->
    wait_process_finish(spawn(Fun));
wait_process_finish(Pid) ->
    wait_process_finish(Pid, 10).

wait_process_finish(Pid, 0) ->
    ?assertEqual(false, erlang:is_process_alive(Pid));
wait_process_finish(Pid, N) ->
    timer:sleep(100),
    case erlang:is_process_alive(Pid) of
        true ->
            wait_process_finish(Pid, N -1);
        false ->
            ok
    end.
