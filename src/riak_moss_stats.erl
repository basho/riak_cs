%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_moss_stats).

-behaviour(gen_server).

%% API
-export([start_link/0,
         update/2,
         update_with_starttime/2,
         report/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-define(IDS, [block_get,
              block_put,
              block_delete,
              service_get_buckets,
              bucket_list_keys,
              bucket_create,
              bucket_delete,
              bucket_get_acl,
              bucket_put_acl,
              object_get,
              object_put,
              object_head,
              object_delete,
              object_get_acl,
              object_put_acl,
              object_delete]).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

update(BaseId, ElapsedUs) ->
    gen_server:call(?MODULE, {update, BaseId, ElapsedUs}).

update_with_starttime(BaseId, StartTime) ->
    gen_server:call(?MODULE, {update, BaseId, timer:now_diff(os:timestamp(), StartTime)}).

report() ->
    [report_item(I) || I <- ?IDS].

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Not sure why folsom doesn't use app module to spin up...
    folsom_sup:start_link(),

    %% Setup a list of all the values we want to track. For each of these, we will
    %% have a latency histogram and meter
    [init_item(I) || I <- ?IDS],
    {ok, #state{}}.

handle_call({get_ids, BaseId}, _From, State) ->
    {reply, erlang:get(BaseId), State};
handle_call({update, BaseId, ElapsedUs}, _From, State) ->
    Reply = case erlang:get(BaseId) of
                {LatencyId, MeterId} ->
                    folsom_metrics:notify({LatencyId, ElapsedUs}),
                    folsom_metrics:notify({MeterId, 1}),
                    ok;
                undefined ->
                    {error, {unknown_id, BaseId}}
            end,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal
%% ====================================================================

init_item(BaseId) ->
    LatencyId = list_to_atom(atom_to_list(BaseId) ++ "_latency"),
    folsom_metrics:new_histogram(LatencyId),
    MeterId = list_to_atom(atom_to_list(BaseId) ++ "_meter"),
    folsom_metrics:new_meter(MeterId),
    %% Cache the two atom-ized Ids for this counter to avoid doing the
    %% conversion per update
    erlang:put(BaseId, {LatencyId, MeterId}).

report_item(BaseId) ->
    case gen_server:call(?MODULE, {get_ids, BaseId}) of
        {LatencyId, MeterId} ->
            Latency = folsom_metrics:get_histogram_statistics(LatencyId),
            Meter = folsom_metrics:get_metric_value(MeterId),
            MeterCount = proplists:get_value(count, Meter),
            MeterRate = proplists:get_value(mean, Meter),
            LatencyMean = proplists:get_value(arithmetic_mean, Latency),
            LatencyMedian = proplists:get_value(median, Latency),
            Percentile = proplists:get_value(percentile, Latency),
            Latency95 = proplists:get_value(95, Percentile),
            Latency99 = proplists:get_value(99, Percentile),
            io:format("~20s:\t~p\t~p\t~p\t~p\t~p\t~p\n", [BaseId, MeterCount, MeterRate, LatencyMean, LatencyMedian, Latency95, Latency99]);
        undefined ->
            io:format("UNKNOWN: ~p\n", [BaseId])
    end.
