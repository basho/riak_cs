%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

%% @doc The worker process to update weight information
%%
%% Triggered periodically by timer or manually by commands.
%% Because GET/PUT operation for Riak may block, e.g. network
%%  failure, updating and refreshing is done by this separate
%%  process rather than doing by `riak_cs_multibag_server'.

-module(riak_cs_multibag_weight_updater).

-behavior(gen_server).

-export([start_link/1]).
-export([status/0, set_weight/1, set_weight_by_type/2, refresh/0, weights/0]).
-export([maybe_refresh/0, refresh_interval/0, set_refresh_interval/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs_multibag.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          timer_ref :: reference() | undefined,
          %% Consecutive refresh failures
          failed_count = 0 :: non_neg_integer(),
          weights :: [{manifest | block,
                       [riak_cs_multibag:weight_info()]}],
          conn_open_fun :: fun(),
          conn_close_fun :: fun()
         }).

-define(SERVER, ?MODULE).
-define(REFRESH_INTERVAL, 900). % 900 sec = 15 min

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

status() ->
    gen_server:call(?SERVER, status).

refresh() ->
    gen_server:call(?SERVER, refresh).

maybe_refresh() ->
    case whereis(?SERVER) of
        undefined -> ok;
        _ -> refresh()
    end.

set_weight(WeightInfo) ->
    gen_server:call(?SERVER, {set_weight, WeightInfo}).

set_weight_by_type(Type, WeightInfo) ->
    gen_server:call(?SERVER, {set_weight_by_type, Type, WeightInfo}).

weights() ->
    gen_server:call(?SERVER, weights).

refresh_interval() ->
    case application:get_env(riak_cs_multibag, weight_refresh_interval) of
        undefined -> ?REFRESH_INTERVAL;
        {ok, Value} -> Value
    end.

set_refresh_interval(Interval) when is_integer(Interval) andalso Interval > 0 ->
    application:set_env(riak_cs_multibag, weight_refresh_interval, Interval).

init(Args) ->
    {conn_open_mf, {OpenM, OpenF}} = lists:keyfind(conn_open_mf, 1, Args),
    {conn_close_mf, {CloseM, CloseF}} = lists:keyfind(conn_close_mf, 1, Args),
    {ok, #state{weights=[{block, []}, {manifest, []}],
                conn_open_fun=fun OpenM:OpenF/0, conn_close_fun=fun CloseM:CloseF/1},
     0}.

handle_call(status, _From, #state{failed_count=FailedCount} = State) ->
    {reply, {ok, [{interval, refresh_interval()}, {failed_count, FailedCount}]}, State};
handle_call(weights, _From, #state{weights = Weights} = State) ->
    {reply, {ok, Weights}, State};
handle_call({set_weight, WeightInfo}, _From, State) ->
    case set_weight(WeightInfo, State) of
        {ok, NewWeights} ->
            {reply, {ok, NewWeights}, State#state{weights=NewWeights}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({set_weight_by_type, Type, WeightInfo}, _From, State) ->
    case set_weight(Type, WeightInfo, State) of
        {ok, NewWeights} ->
            {reply, {ok, NewWeights}, State#state{weights=NewWeights}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(refresh, _From, State) ->
    case fetch_weights(State) of
        {ok, _WeightInfoList, NewState} ->
            {reply, ok, NewState};
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
            logger:error("Refresh of cluster weight information failed. Reason: ~p", [Reason]),
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

%% Connect to master cluster and GET weight information
fetch_weights(#state{conn_open_fun=OpenFun, conn_close_fun=CloseFun} = State) ->
    case OpenFun() of
        {ok, Riakc} ->
            Result = riakc_pb_socket:get(Riakc, ?WEIGHT_BUCKET, ?WEIGHT_KEY),
            CloseFun(Riakc),
            handle_weight_info_list(Result, State);
        {error, _Reason} = E ->
            handle_weight_info_list(E, State)
    end.

handle_weight_info_list({error, notfound}, State) ->
    logger:debug("Bag weight information is not found"),
    {ok, [], State#state{failed_count = 0}};
handle_weight_info_list({error, Reason}, #state{failed_count = Count} = State) ->
    logger:error("Retrieval of bag weight information failed. Reason: ~p", [Reason]),
    {error, Reason, State#state{failed_count = Count + 1}};
handle_weight_info_list({ok, Obj}, State) ->
    %% TODO: How to handle siblings
    [Value | _] = riakc_obj:get_values(Obj),
    Weights = binary_to_term(Value),
    riak_cs_multibag_server:new_weights(Weights),
    {ok, Weights, State#state{failed_count = 0, weights = Weights}}.

schedule(State) ->
    IntervalMSec = refresh_interval() * 1000,
    Ref = erlang:send_after(IntervalMSec, self(), refresh_by_timer),
    State#state{timer_ref = Ref}.

set_weight(WeightInfo, #state{weights = Weights} = State) ->
    NewWeights = [{Type, update_or_add_weight(WeightInfo, WeighInfoList)} ||
                     {Type, WeighInfoList} <- Weights],
    update_weight_info(NewWeights, State).

set_weight(Type, WeightInfo, #state{weights = Weights} = State) ->
    NewWeights =
        case lists:keytake(Type, 1, Weights) of
            false ->
                [{Type, [WeightInfo]} | Weights];
            {value, {Type, WeighInfoList}, OtherWeights} ->
                [{Type, update_or_add_weight(WeightInfo, WeighInfoList)} |
                    OtherWeights]
        end,
    update_weight_info(NewWeights, State).

update_or_add_weight(#weight_info{bag_id=BagId}=WeightInfo,
                     WeighInfoList) ->
    OtherBags = lists:keydelete(BagId, #weight_info.bag_id, WeighInfoList),
    [WeightInfo | OtherBags].

%% Connect to Riak cluster and overwrite weights at {riak-cs-bag, weight}
update_weight_info(WeightInfoList, #state{conn_open_fun=OpenFun, conn_close_fun=CloseFun}) ->
    case OpenFun() of
        {ok, Riakc} ->
            try
                update_weight_info1(Riakc, WeightInfoList)
            after
                CloseFun(Riakc)
            end;
        {error, _Reason} = E ->
            E
    end.

update_weight_info1(Riakc, WeightInfoList) ->
    Current = case riakc_pb_socket:get(Riakc, ?WEIGHT_BUCKET, ?WEIGHT_KEY) of
                  {error, notfound} ->
                      {ok, riakc_obj:new(?WEIGHT_BUCKET, ?WEIGHT_KEY)};
                  {error, Reason} ->
                      {error, Reason};
                  {ok, Obj} ->
                      {ok, Obj}
              end,
    put_weight_info(Riakc, WeightInfoList, Current).

put_weight_info(_Riakc, _WeightInfoList, {error, Reason}) ->
    logger:error("Retrieval of bag weight information failed. Reason: ~p", [Reason]),
    {error, Reason};
put_weight_info(Riakc, WeightInfoList, {ok, Obj}) ->
    NewObj = riakc_obj:update_value(
               riakc_obj:update_metadata(Obj, dict:new()),
               term_to_binary(WeightInfoList)),
    case riakc_pb_socket:put(Riakc, NewObj) of
        ok ->
            riak_cs_multibag_server:new_weights(WeightInfoList),
            {ok, WeightInfoList};
        {error, Reason} ->
            logger:error("Update of bag weight information failed. Reason: ~p", [Reason]),
            {error, Reason}
    end.
