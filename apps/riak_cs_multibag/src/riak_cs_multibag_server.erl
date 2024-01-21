%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Keep weight information and choose bag ID before allocating
%% for each new bucket or manifest.

%% The argument of choose_bag_by_weight/1, `Type' is one of
%% - `manifest' for a new bucket
%% - `block' for a new manifest

-module(riak_cs_multibag_server).

-behavior(gen_server).

-export([start_link/0]).
-export([choose_bag/2, status/0, new_weights/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_cs_multibag.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {
          initialized = false :: boolean(),
          block = [] :: [riak_cs_multibag:weight_info()],
          manifest = [] :: [riak_cs_multibag:weight_info()]
         }).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec choose_bag(manifest | block, term()) -> {ok, bag_id()} | {error, term()}.
choose_bag(Type, Seed) ->
    gen_server:call(?SERVER, {choose_bag, Type, term_to_binary(Seed)}).

new_weights(Weights) ->
    gen_server:cast(?SERVER, {new_weights, Weights}).

status() ->
    gen_server:call(?SERVER, status).

init([]) ->
    %% Recieve weights as soon as possible after restart
    riak_cs_multibag_weight_updater:maybe_refresh(),
    {ok, #state{}}.

handle_call({choose_bag, Type, Seed}, _From, #state{initialized = true} = State)
  when Type =:= manifest orelse Type =:= block ->
    Choice = case Type of
                 block    -> choose_bag_by_weight(State#state.block, Seed);
                 manifest -> choose_bag_by_weight(State#state.manifest, Seed)
             end,
    case Choice of
        {ok, BagId}     -> {reply, {ok, BagId}, State};
        {error, no_bag} -> {reply, {error, no_bag}, State}
    end;
handle_call({choose_bag, _Type, _Seed}, _From, #state{initialized = false} = State) ->
    {reply, {error, not_initialized}, State};
handle_call(status, _From, #state{initialized=Initialized,
                                  block=BlockWeights, manifest=ManifestWeights} = State) ->
    {reply, {ok, [{initialized, Initialized},
                  {block, BlockWeights}, {manifest, ManifestWeights}]}, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast({new_weights, Weights}, State) ->
    NewState = update_weight_state(Weights, State),
    %% TODO: write log only when weights are updated.
    %% logger:info("new_weights: ~p~n", [NewState]),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Choose a bag, to which block/manifest will be stored randomly, regarding weights
%% bag    weight    cummulative-weight   point (1..60)
%% bag1   20        20                    1..20
%% bag2   10        30                   21..30
%% bag3    0        30                   N/A
%% bag4   30        60                   31..60
%% TODO: Make this function deterministic
-spec choose_bag_by_weight([riak_cs_multibag:weight_info()], binary()) ->
                                  {ok, bag_id()} | {error, no_bag}.
choose_bag_by_weight([], _Seed) ->
    {error, no_bag};
choose_bag_by_weight(WeightInfoList, Seed) ->
    %% TODO: SumOfWeights can be stored in state
    SumOfWeights = lists:sum([Weight || #weight_info{weight = Weight} <- WeightInfoList]),
    case SumOfWeights of
        0 ->
            %% Zero is special for transition from single bag, see README
            {ok, undefined};
        _ ->
            <<SHA:160>> = riak_cs_utils:sha(Seed),
            Point = SHA rem SumOfWeights + 1,
            choose_bag_by_weight1(Point, WeightInfoList)
    end.

%% Always "1 =< Point" holds, bag_id with weight=0 never selected.
choose_bag_by_weight1(Point, [#weight_info{bag_id = BagId, weight = Weight} | _WeightInfoList])
  when Point =< Weight ->
    {ok, BagId};
choose_bag_by_weight1(Point, [#weight_info{weight = Weight} | WeightInfoList]) ->
    choose_bag_by_weight1(Point - Weight, WeightInfoList).

update_weight_state([], State) ->
    State#state{initialized = true};
update_weight_state([{Type, WeightsForType} | Rest], State) ->
    Sorted = lists:sort(WeightsForType),
    NewState = case Type of
                   block ->
                       State#state{block = Sorted};
                   manifest ->
                       State#state{manifest = Sorted}
               end,
    update_weight_state(Rest, NewState).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

choose_bag_by_weight1_test() ->
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
    [?assertEqual({ok, BagId}, choose_bag_by_weight1(Point, WeightInfoList)) ||
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
