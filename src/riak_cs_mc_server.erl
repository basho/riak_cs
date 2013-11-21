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
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE). 

%% FIXME make it more specific
-record(usage, {
          weight :: non_neg_integer(),
          free :: non_neg_integer(),
          total :: non_neg_integer()
          }).
-type usage() :: #usage{}.
-record(state, {
          interval = timer:minutes(5) :: non_neg_integer(),
          blocks = [] :: [{riak_cs_mc:pool_key(), usage()}],
          manifests = [] :: [{riak_cs_mc:pool_key(), usage()}]
         }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    random:seed(os:timestamp()),
    %% FIXME
    %% 1. Schedule retreival (in loop)
    %% 2. Implement retreival and update functionality (use default connection pool)
    {Blocks, Manifests} = retreive_usage_and_calc_weight(),
    State = #state{blocks = Blocks, manifests = Manifests},
    schedule(),
    {ok, State}.

handle_call({allocate, Type}, _From, State)
  when Type =:= block orelse Type =:= manifests->
    ContainerId = case Type of
                    block ->
                          decide_container(State#state.blocks);
                    manifests ->
                          decide_container(State#state.manifests)
                  end,
    {reply, {ok, ContainerId}, State};
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
    SumWeight = lists:sum([Weight || {_PoolKey, #usage{weight = Weight}} <- Usages]),
    Point = random:uniform(SumWeight),
    decide_container(Point, Usages).

%% Always Point => 1 holds, usage with weight=0 never selected.
decide_container(Point, [{{_Type, ClusterId}, #usage{weight = Weight}} | _Usages])
  when Point =< Weight ->
    ClusterId;
decide_container(Point, [{_PoolKey, #usage{weight = Weight}} | Usages]) ->
    decide_container(Point - Weight, Usages).

%% Connect to default cluster and GET {riak-cs-mc, usage}, then recalculate weights.
retreive_usage_and_calc_weight() ->
    %% FIXME : this is dummy data
    Blocks = [
              {{block, <<"block-A">>},
               #usage{weight=30, free = 100, total = 200}},
              {{block, <<"block-B">>},
               #usage{weight=70, free = 150, total = 200}}
             ],
    Manifests = [],
    {Blocks, Manifests}.

schedule() ->
    %% TODO: GET to riak should be in async.
    'NOT_IMPLEMENTED_YET'.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
decide_container_test() ->
    %% Better to convert to quickcheck?
    Usages = dummy_usages(),
    ListOfPointAndContainerId = [
                                 %% <<"block-Z*">> are never selected
                                 {1, <<"block-A">>},
                                 {10, <<"block-A">>},
                                 {30, <<"block-A">>},
                                 {31, <<"block-B">>},
                                 {100, <<"block-B">>},
                                 {101, <<"block-C">>},
                                 {110, <<"block-C">>},
                                 {120, <<"block-C">>}],
    [?assertEqual(ContainerId, ?debugVal(decide_container(Point, Usages)))
     || {Point, ContainerId} <- ListOfPointAndContainerId].

dummy_usages() ->
     [
      {{block, <<"block-Z1">>}, #usage{weight=0}},
      {{block, <<"block-Z2">>}, #usage{weight=0}},
      {{block, <<"block-A">>},  #usage{weight=30}},
      {{block, <<"block-B">>},  #usage{weight=70}},
      {{block, <<"block-Z3">>}, #usage{weight=0}},
      {{block, <<"block-C">>},  #usage{weight=20}},
      {{block, <<"block-Z4">>}, #usage{weight=0}}
     ].

-endif.
