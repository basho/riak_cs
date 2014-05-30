%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Key listing logic for GC daemon.

-module(riak_cs_gc_key_list).

%% API
-export([new/2, next/1]).

-include("riak_cs_gc_d.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Start the garbage collection server
-spec new(non_neg_integer(), non_neg_integer()) -> {gc_key_list_result(), gc_key_list_state()}.
new(BatchStart, Leeway) ->
    Bags = riak_cs_mb_helper:bags(),
    State =  #gc_key_list_state{remaining_bags = Bags,
                                batch_start=BatchStart,
                                leeway=Leeway},
    next_pool(State).

%% @doc Fetch next key list and returns it with updated state
-spec next(gc_key_list_state()) -> {gc_key_list_result(), gc_key_list_state()}.
next(#gc_key_list_state{current_riak_client=RcPid,
                        continuation=undefined} = State) ->
    ok = riak_cs_riak_client:stop(RcPid),
    next_pool(State#gc_key_list_state{current_riak_client=undefined});
next(#gc_key_list_state{current_riak_client=RcPid,
                        current_bag_id=BagId,
                        batch_start=BatchStart, leeway=Leeway,
                        continuation=Continuation} = State) ->
    {Batch, UpdContinuation} =
        fetch_eligible_manifest_keys(RcPid, BatchStart, Leeway, Continuation),
    lager:debug("next Batch: ~p~n", [Batch]),
    {#gc_key_list_result{bag_id=BagId, batch=Batch},
     State#gc_key_list_state{continuation=UpdContinuation}}.

%% @doc Fetch next key list and returns it with updated state
-spec next_pool(gc_key_list_state()) -> {gc_key_list_result(), gc_key_list_state()}.
next_pool(#gc_key_list_state{remaining_bags=[]}) ->
    {#gc_key_list_result{bag_id=undefined, batch=[]},
     undefined};
next_pool(#gc_key_list_state{
             batch_start=BatchStart, leeway=Leeway,
             remaining_bags=[{BagId, _Address, _PortType}|Rest]}=State) ->
    case riak_cs_riak_client:start_link([]) of
        {ok, RcPid} ->
            ok = riak_cs_riak_client:set_manifest_bag(RcPid, BagId),
            {Batch, Continuation} =
                fetch_eligible_manifest_keys(RcPid, BatchStart, Leeway, undefined),
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
-spec fetch_eligible_manifest_keys(riak_client(), non_neg_integer(), non_neg_integer(), continuation()) ->
                                          {[index_result_keys()], continuation()}.
fetch_eligible_manifest_keys(RcPid, IntervalStart, Leeway, Continuation) ->
    EndTime = list_to_binary(integer_to_list(IntervalStart - Leeway)),
    UsePaginatedIndexes = riak_cs_config:gc_paginated_indexes(),
    QueryResults = gc_index_query(RcPid,
                                  EndTime,
                                  riak_cs_config:gc_batch_size(),
                                  Continuation,
                                  UsePaginatedIndexes),
    {eligible_manifest_keys(QueryResults, UsePaginatedIndexes), continuation(QueryResults)}.

-spec eligible_manifest_keys({{ok, index_results()} | {error, term()}, binary()},
                             UsePaginatedIndexes::boolean()) ->
                                    [index_result_keys()].
eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}},
                        _EndTime},
                       true) ->
    case Keys of
        [] -> [];
        _  -> [Keys]
    end;
eligible_manifest_keys({{ok, ?INDEX_RESULTS{keys=Keys}},
                        _EndTime},
                       false) ->
    split_eligible_manifest_keys(riak_cs_config:gc_batch_size(), Keys, []);
eligible_manifest_keys({{error, Reason}, EndTime}, _) ->
    _ = lager:warning("Error occurred trying to query from time 0 to ~p"
                      "in gc key index. Reason: ~p",
                      [EndTime, Reason]),
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

-spec continuation({{ok, riakc_pb_socket:index_results()} | {error, term()}, binary()}) ->
                          continuation().
continuation({{ok, ?INDEX_RESULTS{continuation=Continuation}},
              _EndTime}) ->
    Continuation;
continuation({{error, _}, _EndTime}) ->
    undefined.

-spec gc_index_query(riak_client(), binary(), non_neg_integer(), continuation(), boolean()) ->
                            {{ok, riakc_pb_socket:index_results()} | {error, term()}, binary()}.
gc_index_query(RcPid, EndTime, BatchSize, Continuation, UsePaginatedIndexes) ->
    Options = case UsePaginatedIndexes of
                  true ->
                      [{max_results, BatchSize},
                       {continuation, Continuation}];
                  false ->
                      []
              end,
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    QueryResult = riakc_pb_socket:get_index_range(
                    ManifestPbc,
                    ?GC_BUCKET, ?KEY_INDEX,
                    riak_cs_gc:epoch_start(), EndTime,
                    Options),
    {QueryResult, EndTime}.

-ifdef(TEST).

%% ===================================================================
%% Tests
%% ===================================================================

split_eligible_manifest_keys_test() ->
    ?assertEqual([], split_eligible_manifest_keys(3, [], [])),
    ?assertEqual([[1]], split_eligible_manifest_keys(3, [1], [])),
    ?assertEqual([[1,2,3]], split_eligible_manifest_keys(3, lists:seq(1,3), [])),
    ?assertEqual([[1,2,3],[4]], split_eligible_manifest_keys(3, lists:seq(1,4), [])),
    ?assertEqual([[1,2,3],[4,5,6]], split_eligible_manifest_keys(3, lists:seq(1,6), [])),
    ?assertEqual([[1,2,3],[4,5,6],[7,8,9],[10]],
                 split_eligible_manifest_keys(3, lists:seq(1,10), [])).

-endif.
