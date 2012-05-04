%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
-module(riak_cs_stats).

-behaviour(gen_server).

-compile(export_all).

%% API
-export([start_link/0,
         update/2,
         update_with_start/2,
         report/0,
         report_json/0,
         report_pretty_json/0,
         report_str/0,
         get_stats/0]).

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
              object_put_acl]).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec update(atom(), integer()) -> ok | {error, {unknown_id, atom()}}.
update(BaseId, ElapsedUs) ->
    gen_server:call(?MODULE, {update, BaseId, ElapsedUs}).

-spec update_with_start(atom(), erlang:timestamp()) ->
                                   ok | {error, {unknown_id, atom()}}.
update_with_start(BaseId, StartTime) ->
    gen_server:call(?MODULE, {update, BaseId,
                              timer:now_diff(os:timestamp(), StartTime)}).

-spec report() -> ok.
report() ->
    _ = [report_item(I) || I <- ?IDS],
    ok.

-spec report_str() -> [string()].
report_str() ->
    [lists:flatten(report_item_str(I)) || I <- ?IDS].

-spec report_json() -> string().
report_json() ->
    lists:flatten(mochijson2:encode({struct, get_stats()})).

-spec report_pretty_json() -> string().
report_pretty_json() ->
    lists:flatten(riak_moss_utils:json_pp_print(report_json())).

-spec get_stats() -> [{legend, [atom()]} |
                      {atom(), [number()]}].
get_stats() ->
    [{legend, [meter_count, meter_rate, latency_mean, latency_median,
               latency_95, latency_99]}]
    ++
    [raw_report_item(I) || I <- ?IDS].

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Setup a list of all the values we want to track. For each of these, we will
    %% have a latency histogram and meter
    _ = [init_item(I) || I <- ?IDS],
    {ok, #state{}}.

handle_call({get_ids, BaseId}, _From, State) ->
    {reply, erlang:get(BaseId), State};
handle_call({update, BaseId, ElapsedUs}, _From, State) ->
    Reply = case erlang:get(BaseId) of
                {LatencyId, MeterId} ->
                    ok = folsom_metrics:notify({LatencyId, ElapsedUs}),
                    ok = folsom_metrics:notify({MeterId, 1}),
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
    ok = folsom_metrics:new_histogram(LatencyId),
    MeterId = list_to_atom(atom_to_list(BaseId) ++ "_meter"),
    ok = folsom_metrics:new_meter(MeterId),
    %% Cache the two atom-ized Ids for this counter to avoid doing the
    %% conversion per update
    erlang:put(BaseId, {LatencyId, MeterId}).

report_item(BaseId) ->
    io:format("~s\n", [report_item_str(BaseId)]).

report_item_str(BaseId) ->
    {BaseId, [MeterCount, MeterRate, LatencyMean, LatencyMedian,
              Latency95, Latency99]} = raw_report_item(BaseId),
    io_lib:format("~20s:\t~p\t~p\t~p\t~p\t~p\t~p",
                  [BaseId, MeterCount, MeterRate, LatencyMean, LatencyMedian,
                   Latency95, Latency99]).

raw_report_item(BaseId) ->
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
            {BaseId, [MeterCount, MeterRate, LatencyMean, LatencyMedian,
             Latency95, Latency99]};
        undefined ->
            {BaseId, -1, -1, -1, -1, -1, -1}
    end.

-define(MAGIC, '**DYNTRACE').

dyntrace(ArgList) ->
    case application:get_env(riak_moss, dyntrace) of
        undefined ->
            false;
        dyntrace ->
            erlang:apply(dyntrace, p, ArgList);
        dtrace ->
            erlang:apply(dtrace, p, ArgList)
    end.

%% On an unloaded/idle MacBook Pro:
%% 
%% dyntrace0:
%%
%% f(Xs), begin [_|Xs] = [begin put('**DYNTRACE', undefined), timer:tc(riak_cs_stats, dyntrace0, [[42]]) end || _ <- lists:seq(1,500)], lists:sum([X || {X, _} <- Xs]) / (length(Xs)-1) end.  
%% -> 5013.883534136547
%%
%% dyntrace2:
%%
%% riak_cs_stats:dyntrace2_setup(undefined).
%% f(Xs), begin [_|Xs] = [begin put('**DYNTRACE', undefined), timer:tc(riak_cs_stats, dyntrace2, [[42]]) end || _ <- lists:seq(1,10000)], lists:sum([X || {X, _} <- Xs]) / (length(Xs)-1) end.
%% -> 1.0608121624324864
%%
%% dyntrace:
%%
%% f(Xs), begin [_|Xs] = [begin put('**DYNTRACE', undefined), timer:tc(riak_cs_stats, dyntrace, [[42]]) end || _ <- lists:seq(1,10000)], lists:sum([X || {X, _} <- Xs]) / (length(Xs)-1) end. 
%% -> 0.5754150830166033

dyntrace0(ArgList) ->
    try
        erlang:apply(dyntrace, p, ArgList)
    catch
        error:undef ->
            try
                erlang:apply(dtrace, p, ArgList)
            catch
                error:undef ->
                    false
            end
    end.

dyntrace2_setup(Val)
  when Val == undefined; Val == dyntrace; Val == dtrace ->
    code:add_pathz("/Users/fritchie/b/src/riak/deps/mochiweb/ebin"),
    mochiglobal:put(?MAGIC, Val).

dyntrace2(ArgList) ->
    case mochiglobal:get(?MAGIC) of
        undefined ->
            false;
        dyntrace ->
            erlang:apply(dyntrace, p, ArgList);
        dtrace ->
            erlang:apply(dtrace, p, ArgList)
    end.
