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

-module(riak_cs_mc_server).

-behavior(gen_server).

-export([start_link/0]).
-export([allocate/1, status/0, input/1, refresh/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
%% not used now...
-export([calc_weight/2]).

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
%% Riak's bucket and key to store usage information
-define(USAGE_BUCKET, <<"riak-cs-mc">>).
-define(USAGE_KEY,    <<"usage">>).

-define(WEIGHT_MULTIPLIER, 1000).

%% FIXME make it more specific
-record(usage, {
          bag_id :: riak_cs_mc:bag_id(),
          weight :: non_neg_integer(),
          free :: non_neg_integer(),
          total :: non_neg_integer()
          }).
-type usage() :: #usage{}.
-record(state, {
          interval = timer:minutes(5) :: non_neg_integer(),
          blocks = [] :: [{riak_cs_mc:pool_key(), usage()}],
          manifests = [] :: [{riak_cs_mc:pool_key(), usage()}],
          failed_count =0 :: non_neg_integer()
         }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec allocate(riak_cs_mc:pool_type()) -> {ok, riak_cs_mc:bag_id()} |
                                          {error, term()}.
allocate(Type) ->
    gen_server:call(?SERVER, {allocate, Type}).

status() ->
    gen_server:call(?SERVER, status).

refresh() ->
    gen_server:call(?SERVER, refresh).

input(Json) ->
    case json_to_usages(Json) of
        {ok, Usages} ->
            put_and_refresh(Usages);
        {error, Reason} ->
            lager:debug("riak_cs_mc_server:update failed: ~p~n", [Reason]),
            {error, Reason}
    end.

put_and_refresh(Usages) ->
    case put_usages(Usages) of
        ok ->
            refresh();
        {error, Reason} ->
            {error, Reason}
    end.

init([]) ->
    random:seed(os:timestamp()),
    %% FIXME
    %% 1. Schedule retreival (in loop)
    %% 2. Implement retreival and update functionality (use default connection pool)
    {ok, _, NewState} = refresh_usage(#state{}),
    schedule(),
    {ok, NewState}.

handle_call({allocate, Type}, _From, State)
  when Type =:= block orelse Type =:= manifest ->
    BagId = case Type of
                block ->
                    decide_bag(State#state.blocks);
                manifest ->
                    decide_bag(State#state.manifests)
            end,
    {reply, {ok, BagId}, State};
handle_call(status, _From, #state{blocks=Blocks, manifests=Manifests} = State) ->
    {reply, {ok, [{blocks, Blocks}, {manifests, Manifests}]}, State};
handle_call(refresh, _From, State) ->
    case refresh_usage(State) of
        {ok, Usages, NewState} ->
            {reply, {ok, Usages}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    %% TODO: handle messages from GET process and update State.
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% NOTE: NOT USED currently
%% Decide bag to allocate block/manifest randomly weighted by free ratio.
%% bag    weight    cummulative-weight   point (1..60)
%% bag1   20        20                    1..20
%% bag2   10        30                   21..30
%% bag3    0        30                   N/A
%% bag4   30        60                   31..60
-spec decide_bag([{riak_cs_mc:pool_key(), usage()}]) ->
                        {riak_cs_mc:pool_key(), usage()}.
decide_bag(Usages) ->
    %% TODO: if the sum must be a constant value, we can skip this summation.
    %% FIXME: What to do if every usage has weight=0?
    SumOfWeights = lists:sum([Weight || #usage{weight = Weight} <- Usages]),
    Point = random:uniform(SumOfWeights),
    decide_bag(Point, Usages).

%% Always Point => 1 holds, usage with weight=0 never selected.
decide_bag(Point, [#usage{bag_id = BagId, weight = Weight} | _Usages])
  when Point =< Weight ->
    BagId;
decide_bag(Point, [#usage{weight = Weight} | Usages]) ->
    decide_bag(Point - Weight, Usages).

%% Connect to default cluster and GET {riak-cs-mc, usage}, then recalculate weights.
%% TODO: GET operation can be blocked. Make it by spawned process to be able to allocate
refresh_usage(State) ->
    case riak_cs_utils:riak_connection() of
        {ok, Riakc} ->
            Result = riakc_pb_socket:get(Riakc, ?USAGE_BUCKET, ?USAGE_KEY),
            riak_cs_utils:close_riak_connection(Riakc),
            handle_usage_info(Result, State);
        {error, _Reason} = E ->
            handle_usage_info(E, State)
    end.

handle_usage_info({error, notfound}, State) ->
    lager:debug("Cluster usage not found"),
    {ok, [], State#state{failed_count = 0}};
handle_usage_info({error, Reason}, #state{failed_count = Count} = State) ->
    lager:error("Retrieval of cluster usage information failed. Reason: ~p", [Reason]),
    {error, Reason, State#state{failed_count = Count + 1}};
handle_usage_info({ok, Obj}, State) ->
    %% TODO: Should blocks and manifests fields be cleared here?
    %% TODO: How to handle siblings?
    [Value | _] = riakc_obj:get_values(Obj),
    Usages = binary_to_term(Value),
    {ok, Usages, update_usage_state(Usages, State#state{failed_count = 0})}.

update_usage_state([], State) ->
    State;
update_usage_state([{Type, UsagesForType} | Rest], State) ->
    NewState = case Type of
                   block ->
                       State#state{blocks = UsagesForType};
                   manifest ->
                       State#state{manifests = UsagesForType}
               end,
    update_usage_state(Rest, NewState).

schedule() ->
    %% TODO: GET to riak should be in async.
    'NOT_IMPLEMENTED_YET'.

calc_weight([], Acc) ->
    Acc;
calc_weight([{Type, FreeInfoPerType} | Rest], Acc) ->
    Updated = update_weight(FreeInfoPerType, []),
    calc_weight(Rest, [{Type, Updated} | Acc]).

update_weight([], Updated) ->
    Updated;
update_weight([BagInfo | Rest], Updated) ->
    Weight = calc_weight(BagInfo),
    update_weight(Rest, [[{weight, Weight} | BagInfo] | Updated]).

calc_weight(BagInfo) ->
    Threashold = riak_cs_config:get_env(riak_cs, free_ratio_threashold, 20) / 100,
    {free, F} = lists:keyfind(free, 1, BagInfo),
    {total, T} = lists:keyfind(total, 1, BagInfo),
    case F / T of
        TooSmallFreeSpace when TooSmallFreeSpace =< Threashold ->
            0;
        FreeRatio ->
            trunc((FreeRatio - Threashold) * ?WEIGHT_MULTIPLIER)
    end.

json_to_usages({struct, JSON}) ->
    json_to_usages(JSON, []).

json_to_usages([], Usages) ->
    {ok, Usages};
json_to_usages([{TypeBin, Bags} | Rest], Usages) ->
    case TypeBin of
        <<"manifest">> ->
            json_to_usages(manifest, Bags, Rest, Usages);
        <<"block">> ->
            json_to_usages(block, Bags, Rest, Usages);
        _ ->
            {error, {bad_request, TypeBin}}
    end.

json_to_usages(Type, Bags, RestTypes, Usages) ->
    case json_to_usages_by_type(Type, Bags) of
        {ok, TypeUsage} ->
            json_to_usages(RestTypes, [TypeUsage | Usages]);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_usages_by_type(Type, Bags) ->
    json_to_usages_by_type(Type, Bags, []).

json_to_usages_by_type(Type, [], Usages) ->
    {ok, {Type, Usages}};
json_to_usages_by_type(Type, [Bag | Rest], Usages) ->
    case json_to_usage(Bag) of
        {ok, Usage} ->
            json_to_usages_by_type(Type, Rest, [Usage | Usages]);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_usage({struct, Bag}) ->
    json_to_usage(Bag, #usage{}).

json_to_usage([], #usage{bag_id=Id, weight=Weight} = Usage)
  when Id =/= undefined andalso Weight =/= undefined ->
    {ok, Usage};
json_to_usage([], Usage) ->
    {error, {bad_request, Usage}};
json_to_usage([{<<"id">>, Id} | Rest], Usage)
  when is_binary(Id) ->
    json_to_usage(Rest, Usage#usage{bag_id = Id});
json_to_usage([{<<"weight">>, Weight} | Rest], Usage)
  when is_integer(Weight) andalso Weight >= 0 ->
    json_to_usage(Rest, Usage#usage{weight = Weight});
json_to_usage([{<<"free">>, Free} | Rest], Usage) ->
    json_to_usage(Rest, Usage#usage{free = Free});
json_to_usage([{<<"total">>, Total} | Rest], Usage) ->
    json_to_usage(Rest, Usage#usage{total = Total});
json_to_usage(Json, _Usage) ->
    {error, {bad_request, Json}}.

%% Connect to default cluster and put usages to {riak-cs-mc, usage}
put_usages(Usages) ->
    case riak_cs_utils:riak_connection() of
        {ok, Riakc} ->
            update_to_new_usages(Riakc, Usages);
        {error, _Reason} = E ->
            E
    end.

update_to_new_usages(Riakc, Weights) ->
    Current = case riakc_pb_socket:get(Riakc, ?USAGE_BUCKET, ?USAGE_KEY) of
                  {error, notfound} ->
                      {ok, riakc_obj:new(?USAGE_BUCKET, ?USAGE_KEY)};
                  {error, Other} ->
                      lager:log(warning, self(), "Other: ~p~n", [Other]),
                      {error, Other};
                  {ok, Obj} ->
                      {ok, Obj}
              end,
    update_usages(Riakc, Weights, Current).

update_usages(Riakc, _Usages, {error, Reason}) ->
    riak_cs_utils:close_riak_connection(Riakc),
    lager:error("Retrieval of cluster usage information failed. Reason: ~p", [Reason]),
    {error, Reason};
update_usages(Riakc, Usages, {ok, Obj}) ->
    NewObj = riakc_obj:update_value(
               riakc_obj:update_metadata(Obj, dict:new()),
               term_to_binary(Usages)),
    PutRes = riakc_pb_socket:put(Riakc, NewObj),
    riak_cs_utils:close_riak_connection(Riakc),
    case PutRes of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("Update of cluster usage information failed. Reason: ~@", [Reason]),
            {error, Reason}
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
decide_bag_test() ->
    %% Better to convert to quickcheck?
    Usages = dummy_usages(),
    ListOfPointAndBagId = [
                           %% <<"block-Z*">> are never selected
                           {  1, <<"block-A">>},
                           { 10, <<"block-A">>},
                           { 30, <<"block-A">>},
                           { 31, <<"block-B">>},
                           {100, <<"block-B">>},
                           {101, <<"block-C">>},
                           {110, <<"block-C">>},
                           {120, <<"block-C">>}],
    [?assertEqual(BagId, ?debugVal(decide_bag(Point, Usages)))
     || {Point, BagId} <- ListOfPointAndBagId].

dummy_usages() ->
     [
      #usage{bag = <<"block-Z1">>, weight= 0},
      #usage{bag = <<"block-Z2">>, weight= 0},
      #usage{bag = <<"block-A">>,  weight=30},
      #usage{bag = <<"block-B">>,  weight=70},
      #usage{bag = <<"block-Z3">>, weight= 0},
      #usage{bag = <<"block-C">>,  weight=20},
      #usage{bag = <<"block-Z4">>, weight= 0}
     ].

-endif.
