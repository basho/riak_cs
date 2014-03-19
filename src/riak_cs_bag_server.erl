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

%% @doc The server process which periodically retreives information of multi bags

-module(riak_cs_bag_server).

-behavior(gen_server).

-export([start_link/0]).
-export([allocate/1, status/0, new_weights/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs_bag.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {
          initialized = false :: boolean(),
          blocks = [] :: [{riak_cs_bag:pool_key(), riak_cs_bag:weight_info()}],
          manifests = [] :: [{riak_cs_bag:pool_key(), riak_cs_bag:weight_info()}]
         }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec allocate(riak_cs_bag:allocate_type()) -> {ok, riak_cs_bag:bag_id()} |
                                               {error, term()}.
allocate(Type) ->
    gen_server:call(?SERVER, {allocate, Type}).

new_weights(Weights) ->
    gen_server:cast(?SERVER, {new_weights, Weights}).

status() ->
    gen_server:call(?SERVER, status).

init([]) ->
    {ok, #state{}}.

handle_call({allocate, Type}, _From, #state{initialized = true} = State)
  when Type =:= block orelse Type =:= manifest ->
    Decision = case Type of
                block ->
                    decide_bag(State#state.blocks);
                manifest ->
                    decide_bag(State#state.manifests)
            end,
    case Decision of
        {ok, BagId} ->
            {reply, {ok, BagId}, State};
        {error, no_bag} ->
            {reply, {error, no_bag}, State}
    end;
handle_call({allocate, _Type}, _From, #state{initialized = false} = State) ->
    {reply, {error, not_initialized}, State};
handle_call(status, _From, #state{initialized=Initialized, 
                                  blocks=Blocks, manifests=Manifests} = State) ->
    {reply, {ok, [{initialized, Initialized},
                  {blocks, Blocks}, {manifests, Manifests}]}, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast({new_weights, Weights}, State) ->
    NewState = update_weight_state(Weights, State),
    %% TODO: write log only when weights are updated.
    %% lager:info("new_weights: ~p~n", [NewState]),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Decide bag to allocate block/manifest randomly regarding weights
%% bag    weight    cummulative-weight   point (1..60)
%% bag1   20        20                    1..20
%% bag2   10        30                   21..30
%% bag3    0        30                   N/A
%% bag4   30        60                   31..60
-spec decide_bag([{riak_cs_bag:pool_key(), riak_cs_bag:weight_info()}]) ->
                        {ok, riak_cs_bag:bag_id()} |
                        {error, no_bag}.
decide_bag([]) ->
    {error, no_bag};
decide_bag(WeightInfoList) ->
    %% TODO: SumOfWeights can be stored in state
    SumOfWeights = lists:sum([Weight || #weight_info{weight = Weight} <- WeightInfoList]),
    Point = random:uniform(SumOfWeights),
    decide_bag(Point, WeightInfoList).

%% Always "1 =< Point" holds, bag_id with weight=0 never selected.
decide_bag(Point, [#weight_info{bag_id = BagId, weight = Weight} | _WeightInfoList])
  when Point =< Weight ->
    {ok, BagId};
decide_bag(Point, [#weight_info{weight = Weight} | WeightInfoList]) ->
    decide_bag(Point - Weight, WeightInfoList).

update_weight_state([], State) ->
    State#state{initialized = true};
update_weight_state([{Type, WeightsForType} | Rest], State) ->
    NewState = case Type of
                   block ->
                       State#state{blocks = WeightsForType};
                   manifest ->
                       State#state{manifests = WeightsForType}
               end,
    update_weight_state(Rest, NewState).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

decide_bag_test() ->
    %% Better to convert to quickcheck?
    WeightInfoList = dummy_weights(),
    ListOfPointAndBagId = [
                           %% <<"bag-Z*">> are never selected
                           {  1, <<"bag-A">>},
                           { 10, <<"bag-A">>},
                           { 30, <<"bag-A">>},
                           { 31, <<"bag-B">>},
                           {100, <<"bag-B">>},
                           {101, <<"bag-C">>},
                           {110, <<"bag-C">>},
                           {120, <<"bag-C">>}],
    [?assertEqual({ok, BagId}, decide_bag(Point, WeightInfoList)) ||
        {Point, BagId} <- ListOfPointAndBagId].

dummy_weights() ->
     [
      #weight_info{bag_id = <<"bag-Z1">>, weight= 0},
      #weight_info{bag_id = <<"bag-Z2">>, weight= 0},
      #weight_info{bag_id = <<"bag-A">>,  weight=30},
      #weight_info{bag_id = <<"bag-B">>,  weight=70},
      #weight_info{bag_id = <<"bag-Z3">>, weight= 0},
      #weight_info{bag_id = <<"bag-C">>,  weight=20},
      #weight_info{bag_id = <<"bag-Z4">>, weight= 0}
     ].

-endif.
