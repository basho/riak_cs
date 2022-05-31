%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc Key listing logic for GC batch.

-module(riak_cs_gc_key_list).

%% API
-export([new/3, next/1, has_next/1]).

-export([find_oldest_entries/1]).

-include("riak_cs_gc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Start the garbage collection server
-spec new(non_neg_integer(), non_neg_integer(), pos_integer()) -> {gc_key_list_result(), gc_key_list_state()|undefined}.
new(StartKey, EndKey, BatchSize) ->
    Bags = riak_cs_mb_helper:bags(),
    State =  #gc_key_list_state{remaining_bags = Bags,
                                start_key=int2bin(StartKey),
                                end_key=int2bin(EndKey),
                                batch_size=BatchSize},
    next_pool(State).

%% @doc Fetch next key list and returns it with updated state
-spec next(gc_key_list_state()) ->
                  {gc_key_list_result(), gc_key_list_state()|undefined}.
next(#gc_key_list_state{current_riak_client=RcPid,
                        continuation=undefined} = State) ->
    ok = riak_cs_riak_client:stop(RcPid),
    next_pool(State#gc_key_list_state{current_riak_client=undefined});
next(#gc_key_list_state{current_riak_client=RcPid,
                        current_bag_id=BagId,
                        start_key=StartKey, end_key=EndKey,
                        batch_size=BatchSize,
                        continuation=Continuation} = State) ->
    {Batch, UpdContinuation} =
        fetch_eligible_manifest_keys(RcPid, StartKey, EndKey, BatchSize, Continuation),
    lager:debug("next Batch: ~p~n", [Batch]),
    {#gc_key_list_result{bag_id=BagId, batch=Batch},
     State#gc_key_list_state{continuation=UpdContinuation}}.

-spec has_next(gc_key_list_state()) -> boolean().
has_next(#gc_key_list_state{remaining_bags=[], continuation=undefined}) ->
    false;
has_next(_) ->
    true.

%% @doc Fetch next key list and returns it with updated state
-spec next_pool(gc_key_list_state()) -> {gc_key_list_result(), gc_key_list_state()|undefined}.
next_pool(#gc_key_list_state{remaining_bags=[]}) ->
    {#gc_key_list_result{bag_id=undefined, batch=[]},
     undefined};
next_pool(#gc_key_list_state{
             start_key=StartKey, end_key=EndKey,
             batch_size=BatchSize,
             remaining_bags=[{BagId, _Address, _PortType}|Rest]}=State) ->
    case riak_cs_riak_client:start_link([]) of
        {ok, RcPid} ->
            ok = riak_cs_riak_client:set_manifest_bag(RcPid, BagId),
            {Batch, Continuation} =
                fetch_eligible_manifest_keys(RcPid, StartKey, EndKey, BatchSize, undefined),
            lager:debug("next_bag ~s Batch: ~p~n", [BagId, Batch]),
            {#gc_key_list_result{bag_id=BagId, batch=Batch},
             State#gc_key_list_state{remaining_bags=Rest,
                                     current_riak_client=RcPid,
                                     current_bag_id=BagId,
                                     continuation=Continuation}};
        {error, Reason} ->
            lager:error("Connection error for bag ~s in garbage collection: ~p",
                        [BagId, Reason]),
            next_pool(State#gc_key_list_state{remaining_bags=Rest})
    end.

%% @doc Fetch the list of keys for file manifests that are eligible
%% for delete.
-spec fetch_eligible_manifest_keys(riak_client(), binary(), binary(), pos_integer(), continuation()) ->
                                          {[index_result_keys()], continuation()}.
fetch_eligible_manifest_keys(RcPid, StartKey, EndKey, BatchSize, Continuation) ->
    UsePaginatedIndexes = riak_cs_config:gc_paginated_indexes(),
    QueryResults = gc_index_query(RcPid,
                                  StartKey,
                                  EndKey,
                                  BatchSize,
                                  Continuation,
                                  UsePaginatedIndexes),
    {eligible_manifest_keys(QueryResults, UsePaginatedIndexes, BatchSize),
     continuation(QueryResults)}.

-spec eligible_manifest_keys({{ok, index_results()} | {error, term()}, {binary(), binary()}},
                             UsePaginatedIndexes::boolean(), pos_integer()) ->
                                    [index_result_keys()].
eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}}, _},
                       true, _) ->
    case Keys of
        [] -> [];
        _  -> [Keys]
    end;
eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}}, _},
                       false, BatchSize) ->
    split_eligible_manifest_keys(BatchSize, Keys, []);
eligible_manifest_keys({{error, Reason}, {StartKey, EndKey}}, _, _) ->
    _ = lager:warning("Error occurred trying to query from time ~p to ~p"
                      "in gc key index. Reason: ~p",
                      [StartKey, EndKey, Reason]),
    [].

%% @doc Break a list of gc-eligible keys from the GC bucket into smaller sets
%% to be processed by different GC workers.
-spec split_eligible_manifest_keys(non_neg_integer(), index_result_keys(), [index_result_keys()]) ->
                                          [index_result_keys()].
split_eligible_manifest_keys(_BatchSize, [], Acc) ->
    lists:reverse(Acc);
split_eligible_manifest_keys(BatchSize, Keys, Acc) ->
    {Batch, Rest} = split_at_most_n(BatchSize, Keys, []),
    split_eligible_manifest_keys(BatchSize, Rest, [Batch | Acc]).

split_at_most_n(_, [], Acc) ->
    {lists:reverse(Acc), []};
split_at_most_n(0, L, Acc) ->
    {lists:reverse(Acc), L};
split_at_most_n(N, [H|T], Acc) ->
    split_at_most_n(N-1, T, [H|Acc]).

-spec continuation({{ok, index_results()} | {error, term()},
                    {binary(), binary()}}) ->
                          continuation() | undefined.
continuation({{ok, ?INDEX_RESULTS{continuation=Continuation}},
              _EndTime}) ->
    Continuation;
continuation({{error, _}, _EndTime}) ->
    undefined.

-spec gc_index_query(riak_client(), binary(), binary(), non_neg_integer(), continuation(), boolean()) ->
                            {{ok, index_results()} | {error, term()},
                             {binary(), binary()}}.
gc_index_query(RcPid, StartKey, EndKey, BatchSize, Continuation, UsePaginatedIndexes) ->
    Options = case UsePaginatedIndexes of
                  true ->
                      [{max_results, BatchSize},
                       {continuation, Continuation}];
                  false ->
                      []
              end,
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),

    Timeout = riak_cs_config:get_index_range_gckeys_timeout(),
    CallTimeout = riak_cs_config:get_index_range_gckeys_call_timeout(),
    Options1 = [{timeout, Timeout}, {call_timeout, CallTimeout}] ++ Options,
    QueryResult = riak_cs_pbc:get_index_range(
                    ManifestPbc,
                    ?GC_BUCKET, ?KEY_INDEX,
                    StartKey, EndKey,
                    Options1,
                    [riakc, get_gc_keys_by_index]),

    case QueryResult of
        {error, disconnected} ->
            riak_cs_pbc:check_connection_status(ManifestPbc, gc_index_query);
        _ ->
            ok
    end,

    {QueryResult, {StartKey, EndKey}}.

-spec find_oldest_entries(BagId::binary()|master) ->
                                 {ok, [{string(), [pos_integer()]}]} |
                                 {error, term()}.
find_oldest_entries(BagId) ->
    %% walk around
    {ok, RcPid} = riak_cs_riak_client:start_link([]),
    try
        ok = riak_cs_riak_client:set_manifest_bag(RcPid, BagId),
        Start = riak_cs_gc:epoch_start(),
        End = riak_cs_gc:default_batch_end(riak_cs_gc:timestamp(), 0),
        {QueryResult, _} = gc_index_query(RcPid,
                                          int2bin(Start), int2bin(End),
                                          riak_cs_config:gc_batch_size(),
                                          undefined, true),
        case QueryResult of
            {ok, ?INDEX_RESULTS{keys=Keys}} ->
                List = correlate([ gc_key_to_datetime(Key) || Key <- Keys]),
                {ok, non_neg_only(List)};
            {error, _Reason} = E ->
                E
        end
    after
        riak_cs_riak_client:stop(RcPid)
    end.

-spec gc_key_to_datetime(binary()) -> {string(), integer()}.
gc_key_to_datetime(Key) ->
    [Str|Suffix] = string:tokens(binary_to_list(Key), "_"),
    Datetime = binary_to_list(riak_cs_gc_console:human_time(list_to_integer(Str))),
    case Suffix of
        [] -> {Datetime, -1}; %% Very old version of Riak CS, has no suffix
        _ ->  {Datetime, list_to_integer(lists:flatten(Suffix))}
    end.

non_neg_only(List) ->
    [{K, lists:sort(lists:filter(fun(V)-> V >= 0 end, Values))} || {K, Values} <- List].

correlate(Pairs) ->
    F = fun({K,V}, [{K,Vs}|L]) -> [{K,[V|Vs]}|L];
           ({K,V}, Acc) -> [{K, [V]}|Acc]
        end,
    lists:reverse(lists:foldl(F, [], Pairs)).

-spec int2bin(non_neg_integer()) -> binary().
int2bin(I) ->
    list_to_binary(integer_to_list(I)).

-ifdef(TEST).

%% ===================================================================
%% Tests
%% ===================================================================

correlate_test() ->
    %% Sort because 2i returns sorted data
    Data = lists:sort([{a, 10}, {b, -1}, {c, 23}, {a, -1}, {c, 435}, {c, 434}]),
    ?assertEqual([{a, [10]},
                  {b, []},
                  {c, [23, 434, 435]}], non_neg_only(correlate(Data))).

split_eligible_manifest_keys_test() ->
    ?assertEqual([], split_eligible_manifest_keys(3, [], [])),
    ?assertEqual([[1]], split_eligible_manifest_keys(3, [1], [])),
    ?assertEqual([[1,2,3]], split_eligible_manifest_keys(3, lists:seq(1,3), [])),
    ?assertEqual([[1,2,3],[4]], split_eligible_manifest_keys(3, lists:seq(1,4), [])),
    ?assertEqual([[1,2,3],[4,5,6]], split_eligible_manifest_keys(3, lists:seq(1,6), [])),
    ?assertEqual([[1,2,3],[4,5,6],[7,8,9],[10]],
                 split_eligible_manifest_keys(3, lists:seq(1,10), [])).

-endif.
