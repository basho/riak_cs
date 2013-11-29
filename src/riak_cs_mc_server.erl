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

%% @doc The server process which periodically retreives information of multi containers

-module(riak_cs_mc_server).

-behavior(gen_server).

-export([start_link/0]).
-export([allocate/1, status/0, update/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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
          container_id :: riak_cs_mc:container_id(),
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
    %% FIXME: PUT dummy data
    update(dummy_date_for_update()),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

dummy_date_for_update() ->
    [{block, [
               [{container_id, <<"block-A">>},
                {free, 100},
                {total, 200}
               ],
               [{container_id, <<"block-B">>},
                {free, 150},
                {total, 220}
               ]
              ]
     },
     {manifest, []}
    ].

-spec allocate(riak_cs_mc:pool_type()) -> {ok, riak_cs_mc:container_id()} |
                                          {error, term()}.
allocate(Type) ->
    gen_server:call(?SERVER, {allocate, Type}).

status() ->
    gen_server:call(?SERVER, status).

%% FreeInfo is expected nested proplists. Example:
%% [{block, [[{container_id, <<"block-A">>},
%%            {free, 100},
%%            {total, 200}
%%           ],
%%           [{container_id, <<"block-B">>},
%%            {free, 150},
%%            {total, 220}
%%           ]],
%%  },
%%  {manifest, [...]}
%% ]
update(FreeInfo) ->
    calc_weight_and_put(FreeInfo).

init([]) ->
    random:seed(os:timestamp()),
    %% FIXME
    %% 1. Schedule retreival (in loop)
    %% 2. Implement retreival and update functionality (use default connection pool)
    NewState = get_usage(#state{}),
    schedule(),
    {ok, NewState}.

handle_call({allocate, Type}, _From, State)
  when Type =:= block orelse Type =:= manifests->
    ContainerId = case Type of
                    block ->
                          decide_container(State#state.blocks);
                    manifests ->
                          decide_container(State#state.manifests)
                  end,
    {reply, {ok, ContainerId}, State};
handle_call(status, _From, #state{blocks=Blocks, manifests=Manifests} = State) ->
    {reply, {ok, [{blocks, Blocks}, {manifests, Manifests}]}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    %% TODO: handle messages from GET process and update State.
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Decide container to allocate block/manifest randomly weighted by free ratio.
%% container    weight    cummulative-weight   point (1..60)
%% Container1   20        20                    1..20
%% Container2   10        30                   21..30
%% Container3    0        30                   N/A
%% Container4   30        60                   31..60
-spec decide_container([{riak_cs_mc:pool_key(), usage()}]) ->
                              {riak_cs_mc:pool_key(), usage()}.
decide_container(Usages) ->
    %% TODO: if the sum must be a constant value, we can skip this summation.
    %% FIXME: What to do if every usage has weight=0?
    SumOfWeights = lists:sum([Weight || #usage{weight = Weight} <- Usages]),
    Point = random:uniform(SumOfWeights),
    decide_container(Point, Usages).

%% Always Point => 1 holds, usage with weight=0 never selected.
decide_container(Point, [#usage{container_id = ContainerId, weight = Weight} | _Usages])
  when Point =< Weight ->
    ContainerId;
decide_container(Point, [#usage{weight = Weight} | Usages]) ->
    decide_container(Point - Weight, Usages).

%% Connect to default cluster and GET {riak-cs-mc, usage}, then recalculate weights.
get_usage(State) ->
    case riak_cs_utils:riak_connection() of
        {ok, Riakc} ->
            Result = riakc_pb_socket:get(Riakc, ?USAGE_BUCKET, ?USAGE_KEY),
            riak_cs_utils:close_riak_connection(Riakc),
            handle_usage_info(Result, State);
        {error, _Reason} = E ->
            handle_usage_info(E, State)
    end.

handle_usage_info({error, Reason}, #state{failed_count = Count} = State) ->
    lager:error("Retrieval of cluster usage information failed. Reason: ~@", [Reason]),
    State#state{failed_count = Count + 1};
handle_usage_info({ok, Obj}, State) ->
    %% TODO: Should blocks and manifests fields be cleared here?
    %% TODO: How to handle siblings?
    [Value | _] = riakc_obj:get_values(Obj),
    update_usage_state(binary_to_term(Value), State#state{failed_count = 0}).

update_usage_state([], State) ->
    State;
update_usage_state([{Type, Usages} | Rest], State) ->
    UsageForType = [usage_list_to_record(U, #usage{}) || U <- Usages],
    NewState = case Type of
                   block ->
                       State#state{blocks = UsageForType};
                   manifest ->
                       State#state{manifests = UsageForType}
               end,
    update_usage_state(Rest, NewState).

usage_list_to_record([], Rec) ->
    Rec;
usage_list_to_record([{container_id, C} | Rest], Rec) ->
    usage_list_to_record(Rest, Rec#usage{container_id = C});
usage_list_to_record([{weight, W} | Rest], Rec) ->
    usage_list_to_record(Rest, Rec#usage{weight = W});
usage_list_to_record([{free, F} | Rest], Rec) ->
    usage_list_to_record(Rest, Rec#usage{free = F});
usage_list_to_record([{total, T} | Rest], Rec) ->
    usage_list_to_record(Rest, Rec#usage{total = T});
%% Ignore unknown props
usage_list_to_record([_ | Rest], Rec) ->
    usage_list_to_record(Rest, Rec).

schedule() ->
    %% TODO: GET to riak should be in async.
    'NOT_IMPLEMENTED_YET'.

calc_weight_and_put(FreeInfo) ->
    Weights = calc_weight(FreeInfo, []),
    case riak_cs_utils:riak_connection() of
        {ok, Riakc} ->
            put_new_weight(Riakc, Weights);
        {error, _Reason} = E ->
            E
    end.

calc_weight([], Acc) ->
    Acc;
calc_weight([{Type, FreeInfoPerType} | Rest], Acc) ->
    Updated = update_weight(FreeInfoPerType, []),
    calc_weight(Rest, [{Type, Updated} | Acc]).

update_weight([], Updated) ->
    Updated;
update_weight([ContainerInfo | Rest], Updated) ->
    Weight = calc_weight(ContainerInfo),
    update_weight(Rest, [[{weight, Weight} | ContainerInfo] | Updated]).

calc_weight(ContainerInfo) ->
    Threashold = riak_cs_config:get_env(riak_cs, free_ratio_threashold, 20) / 100,
    {free, F} = lists:keyfind(free, 1, ContainerInfo),
    {total, T} = lists:keyfind(total, 1, ContainerInfo),
    case F / T of
        TooSmallFreeSpace when TooSmallFreeSpace =< Threashold ->
            0;
        FreeRatio ->
            trunc((FreeRatio - Threashold) * ?WEIGHT_MULTIPLIER)
    end.

put_new_weight(Riakc, Weights) ->
    Current = case riakc_pb_socket:get(Riakc, ?USAGE_BUCKET, ?USAGE_KEY) of
                  {error, notfound} ->
                      {ok, riakc_obj:new(?USAGE_BUCKET, ?USAGE_KEY)};
                  {error, Other} ->
                      {error, Other};
                  {ok, Obj} ->
                      {ok, Obj}
              end,
    update_value(Riakc, Weights, Current).

update_value(Riakc, _Weights, {error, Reason}) ->
    riak_cs_utils:close_riak_connection(Riakc),
    lager:error("Retrieval of cluster usage information failed. Reason: ~@", [Reason]),
    {error, Reason};
update_value(Riakc, Weights, {ok, Obj}) ->
    NewObj = riakc_obj:update_value(
               riakc_obj:update_metadata(Obj, dict:new()),
               term_to_binary(Weights)),
    PutRes = riakc_pb_socket:put(Riakc, NewObj),
    riak_cs_utils:close_riak_connection(Riakc),
    case PutRes of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("Update of cluster usage information failed. Reason: ~@", [Reason])
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
decide_container_test() ->
    %% Better to convert to quickcheck?
    Usages = dummy_usages(),
    ListOfPointAndContainerId = [
                                 %% <<"block-Z*">> are never selected
                                 {  1, <<"block-A">>},
                                 { 10, <<"block-A">>},
                                 { 30, <<"block-A">>},
                                 { 31, <<"block-B">>},
                                 {100, <<"block-B">>},
                                 {101, <<"block-C">>},
                                 {110, <<"block-C">>},
                                 {120, <<"block-C">>}],
    [?assertEqual(ContainerId, ?debugVal(decide_container(Point, Usages)))
     || {Point, ContainerId} <- ListOfPointAndContainerId].

dummy_usages() ->
     [
      #usage{container=<<"block-Z1">>, weight= 0},
      #usage{container=<<"block-Z2">>, weight= 0},
      #usage{container=<<"block-A">>,  weight=30},
      #usage{container=<<"block-B">>,  weight=70},
      #usage{container=<<"block-Z3">>, weight= 0},
      #usage{container=<<"block-C">>,  weight=20},
      #usage{container=<<"block-Z4">>, weight= 0}
     ].

-endif.
