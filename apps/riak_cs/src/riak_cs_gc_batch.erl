%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc The process that handles garbage collection of deleted file
%% manifests and blocks.
%%
%% Simpler State Diagram
%%
%%     init -> waiting_for_workers --(batch_complete)--> stop
%%                     ^                    |
%%                     +--------------------+

-module(riak_cs_gc_batch).

-behaviour(gen_fsm).

%% API
-export([start_link/1,
         current_state/1,
         status_data/1,
         stop/1]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         prepare/3,
         waiting_for_workers/2,
         waiting_for_workers/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_cs_gc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

-define(SERVER, ?MODULE).
-define(STATE, #gc_batch_state).

-define(GC_WORKER, riak_cs_gc_worker).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the garbage collection server
start_link(Options) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [Options], []).

current_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_state, infinity).

%% @doc Stop the process
-spec stop(pid()) -> ok | {error, term()}.
stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.

init([#gc_batch_state{
         batch_start=BatchStart,
         start_key=StartKey,
         end_key=EndKey,
         leeway=Leeway,
         max_workers=MaxWorkers,
         batch_size=BatchSize} = State])
  when
      %% StartKey can be negative as <<"-">> is smaller than any numeric digits
      0 =< StartKey andalso
      %% EndKey cannot be like 300, or would collect all gc keys
      StartKey =< EndKey ->
    case riak_cs_gc:default_batch_end(BatchStart, Leeway) of
        DefaultEndKey when EndKey =< DefaultEndKey andalso
                           1000000000 =< EndKey ->
            %% StartKey < EndKey
            %% EndKey <= BatchStart - Leeway
            _ = lager:info("Starting garbage collection: "
                           "(start, end) = (~p, ~p), "
                           "leeway=~p, batch_start=~p, max_workers=~p, page-size=~p",
                           [StartKey, EndKey, Leeway, BatchStart, MaxWorkers, BatchSize]),
            {ok, prepare, State, 0};
        DefaultEndKey ->
            _ = lager:error("GC did not start: "
                            "End of GC target period was too recent (~p > ~p)",
                            [EndKey, DefaultEndKey]),
            {stop, {error, invalid_gc_end_key}}
    end;
init([#gc_batch_state{start_key=StartKey,
                      end_key=EndKey}]) ->
    _ = lager:error("GC did not start due to wrong GC target period: "
                    "(start, end) = (~p, ~p)",
                    [StartKey, EndKey]),
    {stop, {error, invalid_gc_start_key}}.


%% Asynchronous events

prepare(timeout, State) ->
    State1 = fetch_first_keys(State),
    NextState = maybe_start_workers(State1),
    case has_batch_finished(NextState) of
        true ->
            {stop, normal, NextState};
        _ ->
            {next_state, waiting_for_workers, NextState}
    end.

%% @doc This state initiates the deletion of a file from
%% a set of manifests stored for a particular key in the
%% garbage collection bucket.
waiting_for_workers(_Msg, State) ->
    {next_state, waiting_for_workers, State}.

%% Synchronous events

%% Some race condition?
prepare(_, _, State) ->
    {reply, {error, preparing}, prepare, State, 0}.

waiting_for_workers(_Msg, _From, State) ->
    {reply, ok, waiting_for_workers, State}.

%% @doc there are no all-state events for this fsm
handle_event({batch_complete, WorkerPid, WorkerState}, StateName, State0) ->
    State1 = handle_batch_complete(WorkerPid, WorkerState, State0),
    State2 = maybe_start_workers(State1),
    case {has_batch_finished(State2), StateName} of
        {true, _} ->
            {stop, normal, State2};
        {false, waiting_for_workers} ->
            {next_state, waiting_for_workers, State2}
    end;
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), ?STATE{}) ->
                               {reply, term(), atom(), ?STATE{}}.
handle_sync_event(current_state, _From, StateName, State) ->
    {reply, {StateName, State}, StateName, State};
handle_sync_event(stop, _From, _StateName, State) ->
    _ = cancel_batch(State),
    {stop, cancel, {ok, State}, State};
handle_sync_event(_Event, _From, StateName, State) ->
    ok_reply(StateName, State).

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc TODO: log warnings if this fsm is asked to terminate in the
%% middle of running a gc batch
terminate(normal, _StateName, State) ->
    lager:info("Finished garbage collection: "
               "~b seconds, ~p batch_count, ~p batch_skips, "
               "~p manif_count, ~p block_count",
               [elapsed(State?STATE.batch_start), State?STATE.batch_count,
                State?STATE.batch_skips, State?STATE.manif_count,
                State?STATE.block_count]),
    riak_cs_gc_manager:finished(State);
terminate(cancel, _StateName, State) ->
    lager:warning("Garbage collection has been canceled: "
                  "~b seconds, ~p batch_count, ~p batch_skips, "
                  "~p manif_count, ~p block_count\n",
                  [elapsed(State?STATE.batch_start), State?STATE.batch_count,
                   State?STATE.batch_skips, State?STATE.manif_count,
                   State?STATE.block_count]),
    ok;
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

has_batch_finished(?STATE{worker_pids=[],
                          batch=[],
                          key_list_state=KeyListState} = _State) ->
    case KeyListState of
        undefined -> true;
        _ -> not riak_cs_gc_key_list:has_next(KeyListState)
    end;
has_batch_finished(_) ->
    false.

fetch_first_keys(?STATE{batch_start=_BatchStart,
                        start_key=StartKey,
                        end_key=EndKey,
                        leeway=_Leeway,
                        batch_size=BatchSize} = State) ->

    %% [Fetch the first set of manifests for deletion]
    %% this does not check out a worker from the riak connection pool;
    %% instead it creates a fresh new worker, the idea being that we
    %% don't want to delay deletion just because the normal request
    %% pool is empty; pool workers just happen to be literally the
    %% socket process, so "starting" one here is the same as opening a
    %% connection, and avoids duplicating the configuration lookup
    %% code.
    {KeyListRes, KeyListState} =
        riak_cs_gc_key_list:new(StartKey, EndKey, BatchSize),
    #gc_key_list_result{bag_id=BagId, batch=Batch} = KeyListRes,
    _ = lager:debug("Initial batch keys: ~p", [Batch]),
    State?STATE{batch=Batch,
                key_list_state=KeyListState,
                bag_id=BagId}.

%% @doc Handle a `batch_complete' event from a GC worker process.
-spec handle_batch_complete(pid(), #gc_worker_state{}, ?STATE{}) -> ?STATE{}.
handle_batch_complete(WorkerPid, WorkerState, State) ->
    ?STATE{
           worker_pids=WorkerPids,
           batch_count=BatchCount,
           batch_skips=BatchSkips,
           manif_count=ManifestCount,
           block_count=BlockCount} = State,
    #gc_worker_state{batch_count=WorkerBatchCount,
                     batch_skips=WorkerBatchSkips,
                     manif_count=WorkerManifestCount,
                     block_count=WorkerBlockCount} = WorkerState,
    _ = lager:debug("~p completed (~p)", [WorkerPid, WorkerState]),
    UpdWorkerPids = lists:delete(WorkerPid, WorkerPids),
    %% @TODO Workout the terminiology for these stats. i.e. Is batch
    %% count just an increment or represenative of something else.
    State?STATE{
                worker_pids=UpdWorkerPids,
                batch_count=BatchCount + WorkerBatchCount,
                batch_skips=BatchSkips + WorkerBatchSkips,
                manif_count=ManifestCount + WorkerManifestCount,
                block_count=BlockCount + WorkerBlockCount}.

%% @doc Start a GC worker and return the apprpriate next state and
%% updated state record.
-spec start_worker(?STATE{}) -> ?STATE{}.
start_worker(?STATE{batch=[NextBatch|RestBatches],
                    bag_id=BagId,
                    worker_pids=WorkerPids} = State) ->
    case ?GC_WORKER:start_link(BagId, NextBatch) of
        {ok, Pid} ->
            _ = lager:debug("GC worker ~p for bag ~p has started", [Pid, BagId]),
            State?STATE{batch=RestBatches,
                        worker_pids=[Pid | WorkerPids]};
        {error, _Reason} ->
            State
    end.

%% @doc Cancel the current batch of files set for garbage collection.
-spec cancel_batch(?STATE{}) -> any().
cancel_batch(?STATE{batch_start=BatchStart,
                    worker_pids=WorkerPids}=_State) ->
    %% Interrupt the batch of deletes
    _ = lager:info("Canceled garbage collection batch after ~b seconds.",
                   [elapsed(BatchStart)]),
    [riak_cs_gc_worker:stop(P) || P <- WorkerPids].

-spec ok_reply(atom(), ?STATE{}) -> {reply, ok, atom(), ?STATE{}}.
ok_reply(NextState, NextStateData) ->
    {reply, ok, NextState, NextStateData}.

maybe_start_workers(?STATE{max_workers=MaxWorkers,
                           worker_pids=WorkerPids} = State)
  when MaxWorkers =:= length(WorkerPids) ->
    State;
maybe_start_workers(?STATE{max_workers=MaxWorkers,
                           worker_pids=WorkerPids,
                           key_list_state=undefined,
                           batch=[]} = State)
  when MaxWorkers > length(WorkerPids) ->
    State;
maybe_start_workers(?STATE{max_workers=MaxWorkers,
                           worker_pids=WorkerPids,
                           key_list_state=KeyListState,
                           batch=[]} = State)
  when MaxWorkers > length(WorkerPids) ->
    %% Fetch the next set of manifests for deletion
    {KeyListRes, UpdKeyListState} = riak_cs_gc_key_list:next(KeyListState),
    #gc_key_list_result{bag_id=BagId, batch=Batch} = KeyListRes,
    lager:debug("Next batch keys: ~p", [Batch]),
    State2 = State?STATE{batch=Batch,
                         key_list_state=UpdKeyListState,
                         bag_id=BagId},
    case UpdKeyListState of
        undefined -> State2;
        _ ->         maybe_start_workers(State2)
    end;
maybe_start_workers(?STATE{max_workers=MaxWorkers,
                           worker_pids=WorkerPids,
                           batch=Batch} = State)
  when MaxWorkers > length(WorkerPids) ->
    lager:debug("Batch: ~p, WorkerPids: ~p", [Batch, WorkerPids]),
    State2 = start_worker(State),
    maybe_start_workers(State2).

-spec status_data(?STATE{}) -> [{atom(), term()}].
status_data(State) ->
    [{current, State?STATE.batch_start},
     {elapsed, elapsed(State?STATE.batch_start)},
     {files_deleted, State?STATE.batch_count},
     {files_skipped, State?STATE.batch_skips},
     {files_left, if is_list(State?STATE.batch) -> length(State?STATE.batch);
                     true                       -> 0
                  end}].

%% @doc How many seconds have passed from `Time' to now.
-spec elapsed(undefined | non_neg_integer()) -> non_neg_integer().
elapsed(undefined) ->
    riak_cs_gc:timestamp();
elapsed(Time) ->
    Now = riak_cs_gc:timestamp(),
    case (Diff = Now - Time) > 0 of
        true ->
            Diff;
        false ->
            0
    end.
