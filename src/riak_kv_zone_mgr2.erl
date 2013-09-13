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

%% @doc riak_kv_zone_mgr2: async background work manager for
%%      riak_kv_zone_backend.erl

-module(riak_kv_zone_mgr2).
-behaviour(gen_server).

-export([start/2, stop/1, send_check_start_next_fold/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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
    gen_server:start_link(?MODULE, {Zone, Parent, DropQueueName}, []).

stop(Pid) ->
    gen_server:call(Pid, {stop}, infinity).

send_check_start_next_fold(Pid) ->
    Pid ! check_start_next_fold.

init({Zone, Parent, DropQueueName}) ->
    {ok, Q} = riak_cs_disk_log_q:new(DropQueueName),
    {ok, TRef} = timer:send_interval(30*1000, check_start_next_fold),
    %% Send one sooner, just to start quicker if there's work to do.
    timer:send_after(5*1000, check_start_next_fold),
    {ok, #clean{zone=Zone,
                parent=Parent,
                parent_waiting=false,
                q=Q,
                timer=TRef,
                cleanups=queue:new(),
                am_folding=false}}.

handle_call({stop}, _From, CS) ->
    {stop, normal, ok, CS};
handle_call(Call, _From, CS) ->
    io:format(user, "~s: Unknown call ~p\n", [?MODULE, Call]),
    {reply, no_such_call, CS}.

handle_cast(Cast, CS) ->
    io:format(user, "~s: Unknown cast ~p\n", [?MODULE, Cast]),
    {noreply, CS}.

handle_info(give_me_work, CS) ->
    {noreply, do_give_me_work(CS)};
handle_info({bg_work_delete, _BKey} = Msg, CS) ->
    {noreply, do_bg_work_msg(Msg, CS)};
handle_info({bg_fold_keys_finished, Partition, ZPrefix, Dropped}, CS)->
    self() ! check_start_next_fold,
    Msg2 = {pending_drop_is_complete, Partition, ZPrefix, Dropped},
    {noreply, do_bg_work_msg(Msg2, CS#clean{am_folding=false})};
handle_info({flow_control, FoldPid}, CS) ->
    FoldPid ! {flow_control, ack},
    {noreply, CS};
handle_info(check_start_next_fold, CS) ->
    {noreply, do_check_start_fold(CS)};
handle_info({partition_dropped, Partition, ZPrefix} = Msg,
            #clean{zone=Zone, cleanups=Cleanups} = CS) ->
    lager:info("~s: zone ~p ~p: idle_0: queue drop partition ~p prefix ~p",
               [?MODULE, Zone, self(), Partition, ZPrefix]),
    {noreply, CS#clean{cleanups=queue:in(Msg, Cleanups)}};
handle_info(Info, CS) ->
    lager:info("~s: unknown message: ~p", [?MODULE, Info]),
    {noreply, CS}.

terminate(_Reason, CS) ->
    timer:cancel(CS#clean.timer),
    ok.

code_change(_OldVsn, CS, _Extra) ->
    {ok, CS}.

%%% Internal

make_drop_cleanup_fun(ZPrefix) ->
    %% {sigh}  Using the {bucket, ...} fold limiting option for KV
    %% will not work for our purposes.  We need to fold over all keys,
    %% and ignore the ones that we are not interested in.  {sigh}
    MgrWorker = self(),
    fun(<<Prefix:24, _KvBucket/binary>>=FullBucket, Key, Acc)
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

do_give_me_work(#clean{parent=Parent, q=Q} = CS) ->
    case riak_cs_disk_log_q:is_empty(Q) of
        true ->
            CS#clean{parent_waiting=true};
        false ->
            {{value, Msg}, NewQ} = riak_cs_disk_log_q:out(Q),
            Parent ! Msg,
            CS#clean{q=NewQ, parent_waiting=false}
    end.

do_check_start_fold(#clean{am_folding=true} = CS) ->
    CS;
do_check_start_fold(#clean{am_folding=false,
                           parent=Parent, cleanups=Cleanups}=CS) ->
    case queue:out(Cleanups) of
        {empty, _} ->
            CS;
        {{value, {partition_dropped, Partition, ZPrefix}}, NewCleanups} ->
            Fun = make_drop_cleanup_fun(ZPrefix),
            Parent ! {bg_work_fold_keys, Partition, ZPrefix, Fun, 0},
            CS#clean{cleanups=NewCleanups,
                     parent_waiting=false,
                     am_folding=true}
    end.

do_bg_work_msg(Msg, #clean{parent_waiting=false, q=Q} = CS) ->
    CS#clean{q=riak_cs_disk_log_q:in(Msg, Q)};
do_bg_work_msg(Msg, #clean{parent_waiting=true, parent=Parent} = CS) ->
    Parent ! Msg,
    CS#clean{parent_waiting=false}.
