%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_zone_mgr2).

%% @doc riak_kv_zone_mgr2: async background work manager for
%%      riak_kv_zone_backend.erl

-export([start/2]).

-record(clean, {
          zone :: integer(),
          parent :: pid(),
          parent_waiting :: boolean(),
          q :: riak_cs_disk_log_q:dlq(),
          timer :: timer:tref(),
          cleanups :: queue(),
          am_folding :: boolean()
         }).

start(Zone, DropQueueName) ->
    Parent = self(),
    spawn_link(fun() ->
                       cleanup_loop(Zone, Parent, DropQueueName)
               end).

cleanup_loop(Zone, Parent, DropQueueName) ->
    {ok, Q} = riak_cs_disk_log_q:new(DropQueueName),
    {ok, TRef} = timer:send_interval(30*1000, perhaps_start_next_fold),
    %% Send one sooner, just to start quicker if there's work to do.
    timer:send_after(5*1000, perhaps_start_next_fold),
    idle_0(#clean{zone=Zone,
                  parent=Parent,
                  parent_waiting=false,
                  q=Q,
                  timer=TRef,
                  cleanups=queue:new(),
                  am_folding=false}).

idle_0(#clean{zone=Zone, parent_waiting=false, q=Q, cleanups=Cleanups}=CS) ->
    receive
        perhaps_start_next_fold ->
            perhaps_start_fold(CS, fun(NewCS) -> idle_0(NewCS) end);
        {fold_keys_finished, Partition, ZPrefix, Dropped} ->
            Msg = helper_fold_keys_finished(Partition, ZPrefix, Dropped),
            idle_N(CS#clean{am_folding=false,
                            q=riak_cs_disk_log_q:in(Msg, Q)});
        {partition_dropped, Partition, ZPrefix} = Msg ->
            lager:info("~s: zone ~p ~p: idle_0: "
                       "queue drop partition ~p prefix ~p",
                       [?MODULE, Zone, self(), Partition, ZPrefix]),
            idle_0(CS#clean{cleanups=queue:in(Msg, Cleanups)});
        give_me_work ->
            wants_work(CS#clean{parent_waiting=true});
        {flow_control, FoldPid} ->
            FoldPid ! {flow_control, ack},
            idle_0(CS);
        {bg_work_delete, _BKey} = Msg ->
            true = riak_cs_disk_log_q:is_empty(Q),
            NewQ = riak_cs_disk_log_q:in(Msg, Q),
            idle_N(CS#clean{q=NewQ})
    end.

perhaps_start_fold(#clean{am_folding=true} = CS, ResumeFun) ->
    ResumeFun(CS);
perhaps_start_fold(#clean{am_folding=false,
                          parent=Parent, cleanups=Cleanups}=CS, ResumeFun) ->
    case queue:out(Cleanups) of
        {empty, _} ->
            ResumeFun(CS);
        {{value, {partition_dropped, Partition, ZPrefix}}, NewCleanups} ->
            Fun = make_drop_cleanup_fun(ZPrefix),
            Parent ! {bg_work_fold_keys, Partition, ZPrefix, Fun, 0},
            ResumeFun(CS#clean{cleanups=NewCleanups,
                               parent_waiting=false, am_folding=true})
    end.

wants_work(#clean{zone=Zone,
                  parent=Parent,
                  cleanups=Cleanups}=CS) ->
    receive
        perhaps_start_next_fold ->
            perhaps_start_fold(CS, fun(NewCS) -> wants_work(NewCS) end);
        {fold_keys_finished, Partition, ZPrefix, Dropped} ->
            Msg = helper_fold_keys_finished(Partition, ZPrefix, Dropped),
            Parent ! Msg,
            idle_0(CS#clean{am_folding=false,
                            parent_waiting=false});
        {partition_dropped, Partition, ZPrefix} = Msg ->
            lager:info("~s: zone ~p ~p: queue drop partition ~p prefix ~p",
                       [?MODULE, Zone, self(), Partition, ZPrefix]),
            wants_work(CS#clean{cleanups=queue:in(Msg, Cleanups)});
        give_me_work ->
            wants_work(CS);
        {flow_control, FoldPid} ->
            FoldPid ! {flow_control, ack},
            wants_work(CS);
        {bg_work_delete, _BKey} = Msg ->
            Parent ! Msg,
            idle_0(CS#clean{parent_waiting=false})
    end.

idle_N(#clean{zone=Zone, parent=Parent, q=Q, cleanups=Cleanups}=CS) ->
    receive
        perhaps_start_next_fold ->
            perhaps_start_fold(CS, fun(NewCS) -> idle_N(NewCS) end);
        {fold_keys_finished, Partition, ZPrefix, Dropped} ->
            Msg = helper_fold_keys_finished(Partition, ZPrefix, Dropped),
            idle_N(CS#clean{am_folding=false,
                            q=riak_cs_disk_log_q:in(Msg, Q)});
        {partition_dropped, Partition, ZPrefix} = Msg ->
            lager:info("~s: zone ~p ~p: idle_N: "
                       "queue drop partition ~p prefix ~p",
                       [?MODULE, Zone, self(), Partition, ZPrefix]),
            idle_N(CS#clean{cleanups=queue:in(Msg, Cleanups)});
        give_me_work ->
            case riak_cs_disk_log_q:out(Q) of
                {empty, _} ->
                    exit({wtf, idle_N, q, is_empty});
                {{value, Msg}, NewQ} ->
                    Parent ! Msg,
                    NewCS = CS#clean{parent_waiting=false, q=NewQ},
                    case riak_cs_disk_log_q:is_empty(NewQ) of
                        true ->
                            idle_0(NewCS);
                        false ->
                            idle_N(NewCS)
                    end
            end;
        {flow_control, FoldPid} ->
            FoldPid ! {flow_control, ack},
            idle_N(CS);
        {bg_work_delete, _BKey} = Msg ->
            NewQ = riak_cs_disk_log_q:in(Msg, Q),
            idle_N(CS#clean{q=NewQ})
    end.

helper_fold_keys_finished(Partition, ZPrefix, Dropped) ->
    self() ! perhaps_start_next_fold,
    {pending_drop_is_complete, Partition, ZPrefix, Dropped}.

make_drop_cleanup_fun(ZPrefix) ->
    %% {sigh}  Using the {bucket, ...} fold limiting option for KV
    %% will not work for our purposes.  We need to fold over all keys,
    %% and ignore the ones that we are not interested in.  {sigh}
    %% TODO Fix prefix length assumption
    MgrWorker = self(),
    fun(<<Prefix:16, _KvBucket/binary>>=FullBucket, Key, Acc)
          when Prefix == ZPrefix ->
            %% if Acc rem 500 == 0 ->
            if Acc rem 2 == 0 ->
                    MgrWorker ! {flow_control, self()},
                    receive
                        {flow_control, ack} ->
                            ok
                    end;
               true ->
                    ok
            end,
            MgrWorker ! {bg_work_delete, {FullBucket, Key}},
            Acc + 1;
       (_Bucket, _Key, Acc) ->
            Acc
    end.

