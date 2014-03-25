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

%% @doc The worker process executing possibly long running tasks

-module(riak_cs_bag_worker).

-behavior(gen_server).

-export([start_link/0]).
-export([status/0, input/1, refresh/0, weights/0]).
-export([refresh_interval_msec/0, set_refresh_interval_msec/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs_bag.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          timer_ref :: reference() | undefined,
          %% Consecutive refresh failures
          failed_count = 0 :: non_neg_integer(),
          weights = [] :: [{riak_cs_bag:pool_type(),
                            [{riak_cs_bag:pool_key(), riak_cs_bag:weight_info()}]}]
         }).

-define(SERVER, ?MODULE).
-define(REFRESH_INTERVAL, timer:minutes(5)).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

status() ->
    gen_server:call(?SERVER, status).

refresh() ->
    gen_server:call(?SERVER, refresh).

input(Json) ->
    case json_to_weight_info_list(Json) of
        {ok, WeightInfoList} ->
            verify_weight_info_list_input(WeightInfoList);
        {error, Reason} ->
            lager:debug("riak_cs_bag_worker:input failed: ~p~n", [Reason]),
            {error, Reason}
    end.

weights() ->
    gen_server:call(?SERVER, weights).

refresh_interval_msec() ->
    riak_cs_config:get_env(riak_cs, weight_refresh_interval_msec, ?REFRESH_INTERVAL).

set_refresh_interval_msec(Interval) when is_integer(Interval) andalso Interval > 0 ->
    application:set_env(riak_cs, weight_refresh_interval_msec, Interval).

init([]) ->
    {ok, #state{}, 0}.

handle_call(status, _From, #state{failed_count=FailedCount} = State) ->
    {reply, {ok, [{interval, refresh_interval_msec()}, {failed_count, FailedCount}]}, State};
handle_call(weights, _From, #state{weights = Weights} = State) ->
    {reply, {ok, Weights}, State};
handle_call(refresh, _From, State) ->
    case fetch_weights(State) of
        {ok, WeightInfoList, NewState} ->
            {reply, {ok, WeightInfoList}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Event, State) when Event =:= refresh_by_timer orelse Event =:= timeout ->
    case refresh_by_timer(State) of
        {ok, _WeightInfoList, NewState} ->
            {noreply, NewState};
        {error, Reason, NewState} ->
            lager:error("Refresh of cluster weight information failed. Reason: ~p", [Reason]),
            {noreply, NewState}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

refresh_by_timer(State) ->
    case fetch_weights(State) of
        {ok, WeightInfoList, State1} ->
            State2 = schedule(State1),
            {ok, WeightInfoList, State2};
        {error, Reason, State1} ->
            State2 = schedule(State1),
            {error, Reason, State2}
    end.

%% Connect to default cluster and GET weight information
fetch_weights(State) ->
    case riak_cs_utils:riak_connection() of
        {ok, Riakc} ->
            Result = riakc_pb_socket:get(Riakc, ?WEIGHT_BUCKET, ?WEIGHT_KEY),
            riak_cs_utils:close_riak_connection(Riakc),
            handle_weight_info_list(Result, State);
        {error, _Reason} = E ->
            handle_weight_info_list(E, State)
    end.

handle_weight_info_list({error, notfound}, State) ->
    lager:debug("Bag weight information is not found"),
    {ok, [], State#state{failed_count = 0}};
handle_weight_info_list({error, Reason}, #state{failed_count = Count} = State) ->
    lager:error("Retrieval of bag weight information failed. Reason: ~p", [Reason]),
    {error, Reason, State#state{failed_count = Count + 1}};
handle_weight_info_list({ok, Obj}, State) ->
    %% TODO: How to handle siblings
    [Value | _] = riakc_obj:get_values(Obj),
    Weights = binary_to_term(Value),
    riak_cs_bag_server:new_weights(Weights),
    {ok, Weights, State#state{failed_count = 0, weights = Weights}}.

schedule(State) ->
    Interval = refresh_interval_msec(),
    Ref = erlang:send_after(Interval, self(), refresh_by_timer),
    State#state{timer_ref = Ref}.

json_to_weight_info_list({struct, JSON}) ->
    json_to_weight_info_list(JSON, []).

json_to_weight_info_list([], WeightInfoList) ->
    {ok, WeightInfoList};
json_to_weight_info_list([{TypeBin, Bags} | Rest], WeightInfoList) ->
    case TypeBin of
        <<"manifest">> ->
            json_to_weight_info_list(manifest, Bags, Rest, WeightInfoList);
        <<"block">> ->
            json_to_weight_info_list(block, Bags, Rest, WeightInfoList);
        _ ->
            {error, {bad_request, TypeBin}}
    end.

json_to_weight_info_list(Type, Bags, RestTypes, WeightInfoList) ->
    case json_to_weight_info_list_by_type(Type, Bags) of
        {ok, WeightInfoListPerType} ->
            json_to_weight_info_list(RestTypes, [WeightInfoListPerType | WeightInfoList]);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_weight_info_list_by_type(Type, Bags) ->
    json_to_weight_info_list_by_type(Type, Bags, []).

json_to_weight_info_list_by_type(Type, [], WeightInfoList) ->
    {ok, {Type, WeightInfoList}};
json_to_weight_info_list_by_type(Type, [Bag | Rest], WeightInfoList) ->
    case json_to_weight_info(Bag) of
        {ok, WeightInfo} ->
            json_to_weight_info_list_by_type(Type, Rest, [WeightInfo | WeightInfoList]);
        {error, Reason} ->
            {error, Reason}
    end.

json_to_weight_info({struct, Bag}) ->
    json_to_weight_info(Bag, #weight_info{}).

json_to_weight_info([], #weight_info{bag_id=Id, weight=Weight} = WeightInfo)
  when Id =/= undefined andalso Weight =/= undefined ->
    {ok, WeightInfo};
json_to_weight_info([], WeightInfo) ->
    {error, {bad_request, WeightInfo}};
json_to_weight_info([{<<"id">>, Id} | Rest], WeightInfo)
  when is_binary(Id) ->
    json_to_weight_info(Rest, WeightInfo#weight_info{bag_id = Id});
json_to_weight_info([{<<"weight">>, Weight} | Rest], WeightInfo)
  when is_integer(Weight) andalso Weight >= 0 ->
    json_to_weight_info(Rest, WeightInfo#weight_info{weight = Weight});
json_to_weight_info([{<<"free">>, Free} | Rest], WeightInfo) ->
    json_to_weight_info(Rest, WeightInfo#weight_info{free = Free});
json_to_weight_info([{<<"total">>, Total} | Rest], WeightInfo) ->
    json_to_weight_info(Rest, WeightInfo#weight_info{total = Total});
json_to_weight_info(Json, _WeightInfo) ->
    {error, {bad_request, Json}}.

verify_weight_info_list_input(WeightInfoList) ->
    %% TODO implement verify logic or consistency checks
    overwrite_and_refresh(WeightInfoList).

overwrite_and_refresh(WeightInfoList) ->
    case overwrite_weight_info(WeightInfoList) of
        ok ->
            refresh();
        {error, Reason} ->
            {error, Reason}
    end.

%% Connect to default cluster and overwrite weights at {riak-cs-bag, weight}
overwrite_weight_info(WeightInfoList) ->
    case riak_cs_utils:riak_connection() of
        {ok, Riakc} ->
            update_to_new_weight_info(Riakc, WeightInfoList);
        {error, _Reason} = E ->
            E
    end.

update_to_new_weight_info(Riakc, WeightInfoList) ->
    Current = case riakc_pb_socket:get(Riakc, ?WEIGHT_BUCKET, ?WEIGHT_KEY) of
                  {error, notfound} ->
                      {ok, riakc_obj:new(?WEIGHT_BUCKET, ?WEIGHT_KEY)};
                  {error, Reason} ->
                      {error, Reason};
                  {ok, Obj} ->
                      {ok, Obj}
              end,
    update_weight_info(Riakc, WeightInfoList, Current).

update_weight_info(Riakc, _WeightInfoList, {error, Reason}) ->
    riak_cs_utils:close_riak_connection(Riakc),
    lager:error("Retrieval of bag weight information failed. Reason: ~p", [Reason]),
    {error, Reason};
update_weight_info(Riakc, WeightInfoList, {ok, Obj}) ->
    NewObj = riakc_obj:update_value(
               riakc_obj:update_metadata(Obj, dict:new()),
               term_to_binary(WeightInfoList)),
    PutRes = riakc_pb_socket:put(Riakc, NewObj),
    riak_cs_utils:close_riak_connection(Riakc),
    case PutRes of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("Update of bag weight information failed. Reason: ~p", [Reason]),
            {error, Reason}
    end.
