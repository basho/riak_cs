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

%% @doc Module to capsulate key listing logic for GC daemon.

-module(riak_cs_gc_key_list).

%% API
-export([new/3, next/1]).

-export_type([index_result_keys/0]).

-include_lib("riakc/include/riakc.hrl").
-include("riak_cs_gc_d.hrl").

-record(key_list_state, {
          %% Riak connection pid
          riak :: undefined | pid(),
          %% start of the current gc interval
          batch_start :: undefined | non_neg_integer(),
          leeway :: non_neg_integer(),
          %% Used for paginated 2I querying of GC bucket
          continuation :: continuation()
         }).

%% 'keys()` is defined in `riakc.hrl'.
%% The name is general so declare local type for readability.
-type index_result_keys() :: keys().

%% @doc Start the garbage collection server
-spec new(pid(), non_neg_integer(), non_neg_integer()) ->
                 {ok, {[index_result_keys()], #key_list_state{}}}.
new(Riakc, BatchStart, Leeway) ->
    {Batch, Continuation} =
        fetch_eligible_manifest_keys(Riakc, BatchStart, Leeway, undefined),
    lager:debug("new Batch: ~p~n", [Batch]),
    {ok, {Batch, #key_list_state{riak=Riakc, batch_start=BatchStart, leeway=Leeway,
                                 continuation=Continuation}}}.

%% @doc Fetch next key list and returns it with updated state
-spec next(#key_list_state{}) -> {ok, {[index_result_keys()], #key_list_state{}}}.
next(#key_list_state{continuation=undefined} = _State) ->
    {ok, {[], undefined}};
next(#key_list_state{riak=Riakc, batch_start=BatchStart, leeway=Leeway,
                     continuation=Continuation} = State) ->
    {Batch, UpdContinuation} =
        fetch_eligible_manifest_keys(Riakc, BatchStart, Leeway, Continuation),
    lager:debug("next Batch: ~p~n", [Batch]),
    {ok, {Batch, State#key_list_state{riak=Riakc, continuation=UpdContinuation}}}.

%% @doc Fetch the list of keys for file manifests that are eligible
%% for delete.
-spec fetch_eligible_manifest_keys(pid(), non_neg_integer(), non_neg_integer(), continuation()) ->
                                          {[binary()], continuation()}.
fetch_eligible_manifest_keys(RiakPid, IntervalStart, Leeway, Continuation) ->
    EndTime = list_to_binary(integer_to_list(IntervalStart - Leeway)),
    UsePaginatedIndexes = riak_cs_config:gc_paginated_indexes(),
    QueryResults = gc_index_query(RiakPid,
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
-spec split_eligible_manifest_keys(non_neg_integer(), index_result_keys(), [[index_result_keys()]]) ->
                                        [[index_result_keys()]].
split_eligible_manifest_keys(_BatchSize, [], Acc) ->
    lists:reverse(Acc);
split_eligible_manifest_keys(BatchSize, Keys, Acc) ->
    {Batch, Rest} = split(BatchSize, Keys, []),
    split_eligible_manifest_keys(BatchSize, Rest, [Batch | Acc]).

split(_, [], Acc) ->
    {lists:reverse(Acc), []};
split(0, L, Acc) ->
    {lists:reverse(Acc), L};
split(N, [H|T], Acc) ->
    split(N-1, T, [H|Acc]).

-spec continuation({{ok, riakc_pb_socket:index_results()} | {error, term()}, binary()}) ->
                          continuation().
continuation({{ok, ?INDEX_RESULTS{continuation=Continuation}},
              _EndTime}) ->
    Continuation;
continuation({{error, _}, _EndTime}) ->
    undefined.

-spec gc_index_query(pid(), binary(), non_neg_integer(), continuation(), boolean()) ->
                            {{ok, riakc_pb_socket:index_results()} | {error, term()}, binary()}.
gc_index_query(RiakPid, EndTime, BatchSize, Continuation, UsePaginatedIndexes) ->
    Options = case UsePaginatedIndexes of
                  true ->
                      [{max_results, BatchSize},
                       {continuation, Continuation}];
                  false ->
                      []
              end,
    QueryResult = riakc_pb_socket:get_index_range(
                    RiakPid,
                    ?GC_BUCKET, ?KEY_INDEX,
                    riak_cs_gc:epoch_start(), EndTime,
                    Options),
    {QueryResult, EndTime}.
