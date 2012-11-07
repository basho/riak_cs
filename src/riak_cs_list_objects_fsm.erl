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
-export([]).

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

                %% list keys ----
                list_keys_req_id :: undefined | non_neg_integer(),
                key_buffer=[] :: list(),
                keys=[] :: list(),
                %% we cache the number of keys because
                %% `length/1' is linear time
                num_keys :: undefined | non_neg_integer(),

                %% map reduce ----
                %% this field will change, it represents
                %% the current outstanding m/r request
                map_red_req_id :: undefined | non_neg_integer(),
                mr_requests=[] :: [{StartIdx :: non_neg_integer(),
                                    EndIdx :: non_neg_integer()}],
                object_buffer=[] :: list()}).

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


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init(list()) -> {ok, prepare, state(), 0}.
init([RiakcPid, Request]) ->
    State = #state{riakc_pid=RiakcPid,
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
    handle_mapred_done(State);
waiting_map_reduce({ReqID, {mapred, _Phase, Results}},
                   State=#state{map_red_req_id=ReqID}) ->
    handle_mapred_results(Results, State);
waiting_map_reduce({ReqID, {error, Reason}},
                   State=#state{map_red_req_id=ReqID}) ->
    {stop, Reason, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

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
handle_keys_done(State=#state{key_buffer=ListofListofKeys,
                              req=?LOREQ{name=BucketName},
                              riakc_pid=RiakcPid}) ->
    SortedFlattenedKeys = lists:sort(lists:flatten(ListofListofKeys)),
    NumKeys = length(SortedFlattenedKeys),
    NewState = State#state{keys=SortedFlattenedKeys,
                           num_keys=NumKeys,
                           %% free up the space
                           key_buffer=undefined},
    ManifestBucketName = riak_cs_utils:to_bucket_name(objects, BucketName),
    handle_map_reduce_call(make_map_reduce_request(RiakcPid,
                                              ManifestBucketName,
                                              NewState),
                           State).

handle_keys_received(Keys, State=#state{key_buffer=PrevKeyBuffer}) ->
    %% this is where we might eventually do a 'top-k' keys
    %% kind of thing, like
    %% `lists:sublist(lists:sort([Keys | PrevKeyBuffer]), BufferSize)'
    NewState = State#state{key_buffer=[Keys | PrevKeyBuffer]},
    {next_state, waiting_list_keys, NewState}.


%% Map Reduce stuff
%%--------------------------------------------------------------------

-spec handle_mapred_done(state()) ->
    fsm_state_return().
handle_mapred_done(_State=#state{object_buffer=ObjectsSoFar,
                                req=Request,
                                num_keys=TotalNumKeys}) ->
    enough_results(ObjectsSoFar, Request, TotalNumKeys),
    could_query_more_mapreduce(foo, bar),
    more_results_possible(foo, bar),
    %% based on the results of `more_results_possible',
    %% return a new state transition
    ok.

-spec handle_mapred_results(list(), state()) ->
    {atom(), atom(), state()}.
handle_mapred_results(_Results, _State) -> ok.

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
could_query_more_mapreduce(_Requests, _TotalKeys) ->
    ok.

-spec more_results_possible({non_neg_integer(), non_neg_integer()},
                            non_neg_integer()) -> boolean().
more_results_possible({_StartIdx, EndIdx}, TotalKeys)
        when EndIdx < TotalKeys ->
    true;
more_results_possible(_Request, _TotalKeys) ->
    false.
