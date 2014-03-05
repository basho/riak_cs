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

%% @doc The worker process executing possibly long running tasks

-module(riak_cs_mc_worker).

-behavior(gen_server).

-export([start_link/0]).
-export([status/0, input/1, refresh/0, weights/0]).
-export([refresh_interval_msec/0, set_refresh_interval_msec/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs_mc.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          timer_ref :: reference() | undefined,
          %% Consecutive refresh failures
          failed_count = 0 :: non_neg_integer(),
          weights = [] :: [{riak_cs_mc:pool_type(),
                            [{riak_cs_mc:pool_key(), riak_cs_mc:usage()}]}]
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
    case json_to_usages(Json) of
        {ok, Usages} ->
            verify_input_usages(Usages);
        {error, Reason} ->
            lager:debug("riak_cs_mc_worker:input failed: ~p~n", [Reason]),
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
    case fetch_usage(State) of
        {ok, Usages, NewState} ->
            {reply, {ok, Usages}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Event, State) when Event =:= refresh_by_timer orelse Event =:= timeout ->
    case refresh_by_timer(State) of
        {ok, Usages, NewState} ->
            riak_cs_mc_server:new_weights(Usages),
            {noreply, NewState};
        {error, Reason, NewState} ->
            lager:error("Refresh of cluster usage information failed. Reason: ~@", [Reason]),
            {noreply, NewState}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

refresh_by_timer(State) ->
    case fetch_usage(State) of
        {ok, Usages, State1} ->
            State2 = schedule(State1),
            {ok, Usages, State2};
        {error, _Reason, _State} = E ->
            E
    end.

%% Connect to default cluster and GET weight information
fetch_usage(State) ->
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
    %% TODO: How to handle siblings
    [Value | _] = riakc_obj:get_values(Obj),
    Weights = binary_to_term(Value),
    {ok, Weights, State#state{failed_count = 0, weights = Weights}}.

schedule(State) ->
    Interval = refresh_interval_msec(),
    Ref = erlang:send_after(Interval, self(), refresh_by_timer),
    State#state{timer_ref = Ref}.

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

verify_input_usages(Usages) ->
    %% TODO implement verify logic or consistency checks
    put_and_refresh(Usages).

put_and_refresh(Usages) ->
    case put_usages(Usages) of
        ok ->
            refresh();
        {error, Reason} ->
            {error, Reason}
    end.

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
                  {error, Reason} ->
                      {error, Reason};
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
