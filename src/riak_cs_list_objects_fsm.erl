%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc

-module(riak_cs_list_objects_fsm).

-behaviour(gen_fsm).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([start_link/2,
         get_object_list/1]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         waiting_list_keys/2,
         waiting_map_reduce/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).


-record(state, {riakc_pid :: pid(),
                req :: list_object_request(),

                reply_pid :: undefined | pid(),

                %% list keys ----
                list_keys_req_id :: undefined | non_neg_integer(),
                key_buffer=[] :: list(),
                keys=[] :: list(),
                %% we cache the number of keys because
                %% `length/1' is linear time
                num_keys :: undefined | non_neg_integer(),

                %% We issue a map reduce request for _more_
                %% keys than the user asks for because some
                %% of the keys returned from list keys
                %% may be tombstoned or marked as deleted.
                %% We assume this will happen to some percentage
                %% of keys, so we account for it in every request
                %% by multiplying the number of objects we need
                %% by some constant.
                key_multiplier :: float(),

                %% map reduce ----
                %% this field will change, it represents
                %% the current outstanding m/r request
                map_red_req_id :: undefined | non_neg_integer(),
                mr_requests=[] :: [{StartIdx :: non_neg_integer(),
                                    EndIdx :: non_neg_integer()}],
                object_buffer=[] :: list(),

                response :: undefined | list_object_response()}).

%% some useful shared types

-type state() :: #state{}.

-type fsm_state_return() :: {next_state, atom(), state()} |
                            {stop, term(), state()}.

-type streaming_req_response() :: {ok, ReqID :: non_neg_integer()} |
                                  {error, term()}.

-type list_keys_event() :: {ReqID :: non_neg_integer(), done} |
                           {ReqID :: non_neg_integer(), {keys, list()}} |
                           {ReqID :: non_neg_integer(), {error, term()}}.

-type mapred_event() :: {ReqID :: non_neg_integer(), done} |
                        {ReqID :: non_neg_integer(),
                         {mapred, Phase :: non_neg_integer(), list()}} |
                        {ReqID :: non_neg_integer, {error, term()}}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), list_object_request()) ->
    {ok, pid()} | {error, term()}.
start_link(RiakcPid, ListKeysRequest) ->
    gen_fsm:start_link(?MODULE, [RiakcPid, ListKeysRequest], []).

-spec get_object_list(pid()) ->
    {ok, list_object_response()} |
    {error, term()}.
get_object_list(FSMPid) ->
    gen_fsm:sync_send_all_state_event(FSMPid, get_object_list, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init(list()) -> {ok, prepare, state(), 0}.
init([RiakcPid, Request]) ->
    %% TODO: should we be linking or monitoring
    %% the proc that called us?

    %% TODO: this should not be hardcoded. Maybe there should
    %% be two `start_link' arities, and one will use a default
    %% val from app.config and the other will explicitly
    %% take a val
    KeyMultiplier = 1.5,

    State = #state{riakc_pid=RiakcPid,
                   key_multiplier=KeyMultiplier,
                   req=Request},
    {ok, prepare, State, 0}.

-spec prepare(timeout, state()) -> fsm_state_return().
prepare(timeout, State=#state{riakc_pid=RiakcPid,
                              req=Request}) ->
    handle_streaming_list_keys_call(make_list_keys_request(RiakcPid, Request),
                                   State).

-spec waiting_list_keys(list_keys_event(), state()) -> fsm_state_return().
waiting_list_keys({ReqID, done}, State=#state{list_keys_req_id=ReqID}) ->
    handle_keys_done(State);
waiting_list_keys({ReqID, {keys, Keys}}, State=#state{list_keys_req_id=ReqID}) ->
    handle_keys_received(Keys, State);
waiting_list_keys({ReqID, {error, Reason}}, State=#state{list_keys_req_id=ReqID}) ->
    %% if we get an error while we're in this state,
    %% we still have the option to return `HTTP 500'
    %% to the client, since we haven't written
    %% any bytes yet
    {stop, Reason, State}.

-spec waiting_map_reduce(mapred_event(), state()) -> fsm_state_return().
waiting_map_reduce({ReqID, done}, State=#state{map_red_req_id=ReqID}) ->
    %% depending on the result of this, we'll either
    %% make another m/r request, or be done
    maybe_map_reduce(State);
waiting_map_reduce({ReqID, {mapred, _Phase, Results}},
                   State=#state{map_red_req_id=ReqID}) ->
    handle_mapred_results(Results, State);
waiting_map_reduce({ReqID, {error, Reason}},
                   State=#state{map_red_req_id=ReqID}) ->
    {stop, Reason, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(get_object_list, _From, done, State=#state{response=Resp}) ->
    Reply = {ok, Resp},
    {stop, normal, Reply, State};
handle_sync_event(get_object_list, From, StateName, State) ->
    NewStateData = State#state{reply_pid=From},
    {next_state, StateName, NewStateData};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% the responses from `riakc_pb_socket:stream_list_keys'
%% come back as regular messages, so just pass
%% them along as if they were gen_server events.
handle_info(Info, waiting_list_keys, State) ->
    waiting_list_keys(Info, State);
handle_info(_Info, _StateName, State) ->
    {stop, unknown, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

%% List Keys stuff
%%--------------------------------------------------------------------

%% function to create a list keys request
%% this could also be a phase-less map-reduce request
%% with key filters.
-spec make_list_keys_request(pid(), list_object_request()) ->
    streaming_req_response().
make_list_keys_request(RiakcPid, ?LOREQ{name=BucketName}) ->
    %% hardcoded for now
    ServerTimeout = timer:seconds(60),
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, BucketName),
    riakc_pb_socket:stream_list_keys(RiakcPid,
                                     ManifestBucket,
                                     ServerTimeout,
                                     infinity).

-spec handle_streaming_list_keys_call(streaming_req_response(), state()) ->
    fsm_state_return().
handle_streaming_list_keys_call({ok, ReqID}, State) ->
    {next_state, waiting_list_keys, State#state{list_keys_req_id=ReqID}};
handle_streaming_list_keys_call({error, Reason}, State) ->
    {stop, Reason, State}.

-spec handle_keys_done(state()) -> fsm_state_return().
handle_keys_done(State=#state{key_buffer=ListofListofKeys}) ->
    %% TODO: this could potentially be pretty expensive
    %% and memory instensive. More reason to think about starting
    %% to only keep a smaller buffer. See comment in
    %% `handle_keys_received'
    SortedFlattenedKeys = lists:sort(lists:flatten(ListofListofKeys)),
    NumKeys = length(SortedFlattenedKeys),
    NewState = State#state{keys=SortedFlattenedKeys,
                           num_keys=NumKeys,
                           %% free up the space
                           key_buffer=undefined},
    maybe_map_reduce(NewState).

handle_keys_received(Keys, State=#state{key_buffer=PrevKeyBuffer}) ->
    %% TODO:
    %% this is where we might eventually do a 'top-k' keys
    %% kind of thing, like
    %% `lists:sublist(lists:sort([Keys | PrevKeyBuffer]), BufferSize)'
    NewState = State#state{key_buffer=[Keys | PrevKeyBuffer]},
    {next_state, waiting_list_keys, NewState}.


%% Map Reduce stuff
%%--------------------------------------------------------------------

-spec maybe_map_reduce(state()) -> fsm_state_return().
maybe_map_reduce(State=#state{object_buffer=ObjectBuffer,
                              req=Request,
                              num_keys=TotalNumKeys}) ->
    Enough = enough_results(ObjectBuffer, Request, TotalNumKeys),
    handle_enough_results(Enough, State).

handle_enough_results(true, State) ->
    have_enough_results(State);
handle_enough_results(false, State=#state{num_keys=TotalNumKeys,
                                          mr_requests=MapRRequests}) ->
    MoreQuery = could_query_more_mapreduce(MapRRequests, TotalNumKeys),
    handle_could_query_more_map_reduce(MoreQuery, State).

-spec handle_could_query_more_map_reduce(boolean, state()) ->
    fsm_state_return().
handle_could_query_more_map_reduce(true, State=#state{req=Request,
                                                      keys=Keys,
                                                      key_multiplier=KeyMultiplier,
                                                      mr_requests=PrevRequests,
                                                      object_buffer=Buffer,
                                                      riakc_pid=RiakcPid}) ->
    TotalNeeded = Request?LOREQ.max_keys,
    BucketName = Request?LOREQ.name,
    ManifestBucketName = riak_cs_utils:to_bucket_name(objects, BucketName),
    {Keys, NewReq} = keys_to_query(PrevRequests,
                                   Keys,
                                   TotalNeeded,
                                   length(Buffer),
                                   KeyMultiplier),
    NewStateData = State#state{mr_requests=PrevRequests ++ [NewReq]},
    MapReduceRequestResult = make_map_reduce_request(RiakcPid,
                                                     ManifestBucketName,
                                                     Keys),
    handle_map_reduce_call(MapReduceRequestResult, NewStateData);
handle_could_query_more_map_reduce(false, State) ->
    have_enough_results(State).

-spec make_response(list_object_request(), list()) ->
    list_object_response().
make_response(_Request, _ObjectBuffer) ->
    %% TODO: fix me
    ok.

-spec keys_to_query(list(),
                    list(),
                    non_neg_integer(),
                    non_neg_integer(),
                    float()) ->
    {list(), {non_neg_integer(), non_neg_integer()}}.
keys_to_query([], Keys, TotalNeeded, _NumHaveSoFar, KeyMultiplier) ->
    StartIdx = 1,
    EndIdx = round_up(TotalNeeded * KeyMultiplier),
    {lists:sublist(Keys, StartIdx, EndIdx),
     {StartIdx, EndIdx}};
keys_to_query(PrevRequests, Keys, TotalNeeded, NumHaveSoFar, KeyMultiplier) ->
    MoreNeeded = TotalNeeded - NumHaveSoFar,
    StartIdx = element(2, lists:last(PrevRequests)) + 1,
    EndIdx = (StartIdx + MoreNeeded) * KeyMultiplier,
    {lists:sublist(Keys, StartIdx, EndIdx),
     {StartIdx, EndIdx}}.


-spec handle_mapred_results(list(), state()) ->
    fsm_state_return().
handle_mapred_results(Results, State=#state{object_buffer=Buffer}) ->
    NewBuffer = update_buffer(Results, Buffer),
    NewState = State#state{object_buffer=NewBuffer},
    {next_state, waiting_map_reduce, NewState}.

-spec update_buffer(list(), list()) -> list().
update_buffer(Results, Buffer) ->
    %% TODO: is this the fastest way to do this?
    lists:merge(lists:sort(Results), Buffer).

-spec make_map_reduce_request(pid(), binary(), list()) ->
    streaming_req_response().
make_map_reduce_request(RiakcPid, ManifestBucketName, Keys) ->
    BKeyTuples = make_bkeys(ManifestBucketName, Keys),
    send_map_reduce_request(RiakcPid, BKeyTuples).

-spec make_bkeys(binary(), list()) -> list().
make_bkeys(ManifestBucketName, Keys) ->
    [{ManifestBucketName, Key} || Key <- Keys].

-spec send_map_reduce_request(pid(), list()) -> streaming_req_response().
send_map_reduce_request(RiakcPid, BKeyTuples) ->
    %% TODO: change this:
    %% hardcode 60 seconds for now
    Timeout = timer:seconds(60),
    riakc_pb_socket:mapred(RiakcPid,
                           BKeyTuples,
                           mapred_query(),
                           Timeout,
                           infinity).

-spec mapred_query() -> list().
mapred_query() ->
    [{map, {modfun, riak_cs_utils, map_keys_and_manifests},
      undefined, false},
     {reduce, {modfun, riak_cs_utils, reduce_keys_and_manifests},
      undefined, true}].

-spec handle_map_reduce_call(streaming_req_response(), state()) ->
    fsm_state_return().
handle_map_reduce_call({ok, ReqID}, State) ->
    {next_state, waiting_map_reduce, State#state{map_red_req_id=ReqID}};
handle_map_reduce_call({error, Reason}, State) ->
    {stop, Reason, State}.

-spec enough_results(list(), list_object_request(), non_neg_integer()) ->
    boolean().
enough_results(Results, ?LOREQ{max_keys=MaxKeysRequested}, TotalKeys) ->
    ResultsLength = length(Results),
    %% we have enough results if one of two things is true:
    %% 1. we have more results than requested
    %% 2. there are less keys than were requested even possible
    (ResultsLength >= MaxKeysRequested) orelse (MaxKeysRequested =< TotalKeys).

-spec could_query_more_mapreduce(list(), non_neg_integer()) -> boolean().
could_query_more_mapreduce(_Requests, 0) ->
    false;
could_query_more_mapreduce([], _TotalKeys) ->
    true;
could_query_more_mapreduce(Requests, TotalKeys) ->
    more_results_possible(lists:last(Requests), TotalKeys).

-spec more_results_possible({non_neg_integer(), non_neg_integer()},
                            non_neg_integer()) -> boolean().
more_results_possible({_StartIdx, EndIdx}, TotalKeys)
        when EndIdx < TotalKeys ->
    true;
more_results_possible(_Request, _TotalKeys) ->
    false.

-spec have_enough_results(state()) -> fsm_state_return().
have_enough_results(State=#state{reply_pid=undefined,
                                 req=Request,
                                 object_buffer=ObjectBuffer}) ->
    Response = make_response(Request, ObjectBuffer),
    NewStateData = State#state{response=Response},
    {next_state, done, NewStateData, 60};
have_enough_results(State=#state{reply_pid=ReplyPid,
                                 req=Request,
                                 object_buffer=ObjectBuffer}) ->
    Response = make_response(Request, ObjectBuffer),
    gen_fsm:reply(ReplyPid, {ok, Response}),
    NewStateData = State#state{response=Response},
    {stop, normal, NewStateData}.

%% only works for positive numbers
-spec round_up(float()) -> float().
round_up(X) ->
    erlang:round(X + 0.5).
