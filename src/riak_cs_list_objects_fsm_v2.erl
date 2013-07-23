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

%% TODO:
%% 1. maybe `objects' should be called `manifests'

-module(riak_cs_list_objects_fsm_v2).

-behaviour(gen_fsm).

-include("riak_cs.hrl").
-include("list_objects.hrl").

%%%===================================================================
%%% Exports
%%%===================================================================

-ifdef(TEST).
-compile(export_all).
-endif.

%% API
-export([start_link/2]).

%% Observability
-export([]).

%% gen_fsm callbacks
-export([init/1,
         prepare/2,
         waiting_object_list/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%%%===================================================================
%%% Records and Types
%%%===================================================================

-record(profiling, {
        temp_fold_objects_request :: undefined |
                                     {Request :: {StartKey :: binary(),
                                                  EndKey :: binary()},
                                      StartTime :: erlang:timestamp()},
        fold_objects_requests=[] :: [{Request :: {StartKey :: binary(),
                                                  EndKey :: binary()},
                                      NumKeysReturned :: non_neg_integer(),
                                      Timing :: {StartTime :: erlang:timestamp(),
                                                 EndTime :: erlang:timestamp()}}]}).

-type profiling() :: #profiling{}.


-record(state, {riakc_pid :: pid(),
                req :: list_object_request(),
                reply_ref :: undefined | {pid(), any()},
                key_multiplier :: float(),
                object_list_req_id :: undefined | reference(),
                reached_end_of_keyspace=false :: boolean(),
                object_buffer=[] :: list(),
                objects=[] :: list(),
                last_request_start_key :: undefined | binary(),
                last_request_num_keys_requested :: undefined | pos_integer(),
                object_list_ranges=[] :: object_list_ranges(),
                profiling=#profiling{} :: profiling(),
                response :: undefined |
                            {ok, list_object_response()} |
                            {error, term()},
                common_prefixes=ordsets:new() :: list_objects_common_prefixes()}).

%% some useful shared types

-type state() :: #state{}.

-type fsm_state_return() :: {next_state, atom(), state()} |
                            {next_state, atom(), state(), non_neg_integer()} |
                            {stop, term(), state()}.

-type continuation() :: binary() | 'undefined'.
-type list_objects_event() :: {ReqID :: reference(), {done, continuation()}} |
                              {ReqID :: reference(), {objects, list()}} |
                              {ReqID :: reference(), {error, term()}}.

%% `Start' and `End' are inclusive
-type object_list_range()  :: {Start :: binary(), End :: binary()}.
-type object_list_ranges() :: list(object_list_range()).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), list_object_request()) ->
    {ok, pid()} | {error, term()}.
start_link(RiakcPid, ListKeysRequest) ->
    gen_fsm:start_link(?MODULE, [RiakcPid, ListKeysRequest], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init(list()) -> {ok, prepare, state(), 0}.
init([RiakcPid, Request]) ->
    %% TODO: this should not be hardcoded. Maybe there should
    %% be two `start_link' arities, and one will use a default
    %% val from app.config and the other will explicitly
    %% take a val
    KeyMultiplier = riak_cs_list_objects_utils:get_key_list_multiplier(),

    State = #state{riakc_pid=RiakcPid,
                   key_multiplier=KeyMultiplier,
                   req=Request},
    {ok, prepare, State, 0}.

-spec prepare(timeout, state()) -> fsm_state_return().
prepare(timeout, State=#state{riakc_pid=RiakcPid}) ->
    case make_2i_request(RiakcPid, State) of
        {NewStateData, {ok, ReqId}} ->
            {next_state, waiting_object_list,
             NewStateData#state{object_list_req_id=ReqId}};
        {NewStateData, {error, _Reason}=Error} ->
            try_reply(Error, NewStateData)
    end.

-spec waiting_object_list(list_objects_event(), state()) -> fsm_state_return().
waiting_object_list({ReqId, {ok, _}=Objs}, State) ->
    waiting_object_list({ReqId, [Objs, undefined]}, State);
waiting_object_list({ReqId, [{ok, ObjectList} | _]},
                    State=#state{object_list_req_id=ReqId,
                                 object_buffer=ObjectBuffer}) ->
    NewStateData = State#state{object_buffer=ObjectBuffer ++ ObjectList},
    {next_state, waiting_object_list, NewStateData};
waiting_object_list({ReqId, {done, _Continuation}}, State=#state{object_list_req_id=ReqId}) ->
    handle_done(State);
waiting_object_list({ReqId, {error, _Reason}=Error},
                    State=#state{object_list_req_id=ReqId}) ->
    try_reply(Error, State).

handle_event(_Event, StateName, State) ->
    %% TODO: log unknown event
    {next_state, StateName, State}.

handle_sync_event(get_object_list, From, StateName,
                  State=#state{response=undefined}) ->
    NewStateData = State#state{reply_ref=From},
    {next_state, StateName, NewStateData};
handle_sync_event(get_object_list, _From, _StateName,
                  State=#state{response=Resp}) ->
    {stop, normal, Resp, State};
handle_sync_event(get_internal_state, _From, StateName, State) ->
    Reply = {StateName, State},
    {reply, Reply, StateName, State};
handle_sync_event(Event, _From, StateName, State) ->
    _ = lager:debug("got unknown event ~p in state ~p", [Event, StateName]),
    Reply = ok,
    {reply, Reply, StateName, State}.

%% the responses from `riakc_pb_socket:get_index_range'
%% come back as regular messages, so just pass
%% them along as if they were gen_server events.
handle_info(Info, waiting_object_list, State) ->
    waiting_object_list(Info, State);
handle_info(Info, StateName, _State) ->
    _ = lager:debug("Received unknown info message ~p"
                    "in state ~p", [Info, StateName]),
    ok.

terminate(normal, _StateName, State) ->
    lager:debug(format_profiling_from_state(State));
terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%====================================================================
%%% Internal helpers
%%%====================================================================

handle_done(State=#state{object_buffer=ObjectBuffer,
                         objects=PrevObjects,
                         last_request_num_keys_requested=NumKeysRequested,
                         common_prefixes=CommonPrefixes,
                         req=Request=?LOREQ{max_keys=UserMaxKeys}}) ->
    ObjectBufferLength = length(ObjectBuffer),
    RangeUpdatedStateData =
    update_profiling_and_last_request(State, ObjectBuffer, ObjectBufferLength),

    FilteredObjects = exclude_key_from_state(State, ObjectBuffer),
    Manifests = [riak_cs_utils:manifests_from_riak_object(O) ||
                 O <- FilteredObjects],
    Active = map_active_manifests(Manifests),
    NewObjects = PrevObjects ++ Active,
    ObjectPrefixTuple = {NewObjects, CommonPrefixes},

    ObjectPrefixTuple2 =
    riak_cs_list_objects_utils:filter_prefix_keys(ObjectPrefixTuple, Request),
    ReachedEnd = ObjectBufferLength < NumKeysRequested,

    Truncated = truncated(UserMaxKeys, ObjectPrefixTuple2),
    SlicedTaggedItems =
    riak_cs_list_objects_utils:manifests_and_prefix_slice(ObjectPrefixTuple2,
                                                          UserMaxKeys),

    {NewManis, NewPrefixes} =
    riak_cs_list_objects_utils:untagged_manifest_and_prefix(SlicedTaggedItems),

    NewStateData = RangeUpdatedStateData#state{objects=NewManis,
                                               common_prefixes=NewPrefixes,
                                               reached_end_of_keyspace=ReachedEnd,
                                               object_buffer=[]},
    respond(NewStateData, NewManis, NewPrefixes, Truncated).

-spec update_profiling_and_last_request(state(), list(), integer()) ->
    state().
update_profiling_and_last_request(State, ObjectBuffer, ObjectBufferLength) ->
    State2 = update_profiling_state_with_end(State, os:timestamp(),
                                             ObjectBufferLength),
    update_last_request_state(State2, ObjectBuffer).

-spec respond(state(), list(), ordsets:ordset(), boolean()) ->
    fsm_state_return().
respond(StateData=#state{req=Request},
        Manifests, Prefixes, Truncated) ->
    case enough_results(StateData) of
        true ->
            Response =
            response_from_manifests_and_common_prefixes(Request,
                                                        Truncated,
                                                        {Manifests, Prefixes}),
            try_reply({ok, Response}, StateData);
        false ->
            RiakcPid = StateData#state.riakc_pid,
            case make_2i_request(RiakcPid, StateData) of
                {NewStateData2, {ok, ReqId}} ->
                    {next_state, waiting_object_list,
                     NewStateData2#state{object_list_req_id=ReqId}};
                {NewStateData2, {error, _Reason}=Error} ->
                    try_reply(Error, NewStateData2)
            end
    end.

-spec truncated(non_neg_integer(), {list(), ordsets:ordset()}) -> boolean().
truncated(NumKeysRequested, ObjectsAndPrefixes) ->
    NumKeysRequested < riak_cs_list_objects_utils:manifests_and_prefix_length(ObjectsAndPrefixes) andalso
    %% this is because (strangely) S3 returns `false' for
    %% `isTruncated' if `max-keys=0', even if there are more keys.
    %% The `Ceph' tests were nice to find this.
    NumKeysRequested =/= 0.

enough_results(#state{req=?LOREQ{max_keys=UserMaxKeys},
                      reached_end_of_keyspace=EndOfKeyspace,
                      objects=Objects,
                      common_prefixes=CommonPrefixes}) ->
    riak_cs_list_objects_utils:manifests_and_prefix_length({Objects, CommonPrefixes})
    >= UserMaxKeys
    orelse EndOfKeyspace.

response_from_manifests_and_common_prefixes(Request,
                                            Truncated,
                                            {Manifests, CommonPrefixes}) ->
    KeyContent = lists:map(fun riak_cs_list_objects:manifest_to_keycontent/1,
                           Manifests),
    riak_cs_list_objects:new_response(Request, Truncated, CommonPrefixes,
                                      KeyContent).

-spec make_2i_request(pid(), state()) ->
                             {state(), {ok, reference()} | {error, term()}}.
make_2i_request(RiakcPid, State=#state{req=?LOREQ{name=BucketName}}) ->
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, BucketName),
    StartKey = make_start_key(State),
    EndKey = big_end_key(128),
    NumResults = 1002,
    NewStateData = State#state{last_request_start_key=StartKey,
                               last_request_num_keys_requested=NumResults},
    NewStateData2 = update_profiling_state_with_start(NewStateData,
                                                      StartKey,
                                                      EndKey,
                                                      os:timestamp()),
    Opts = [{max_results, NumResults},
            {start_key, StartKey},
            {end_key, EndKey}],
    FoldResult = riakc_pb_socket:cs_bucket_fold(RiakcPid,
                                                ManifestBucket,
                                                Opts),
    {NewStateData2, FoldResult}.

-spec last_result_is_common_prefix(state()) -> boolean().
last_result_is_common_prefix(#state{object_list_ranges=Ranges,
                                    req=Request}) ->
    Key = element(2, lists:last(Ranges)),
    key_is_common_prefix(Key, Request).

-spec key_is_common_prefix(binary(), list_object_request()) ->
    boolean().
key_is_common_prefix(_Key, ?LOREQ{delimiter=undefined}) ->
    false;
key_is_common_prefix(Key, ?LOREQ{prefix=Prefix,
                                 delimiter=Delimiter}) ->
    case Prefix of
        undefined ->
            handle_undefined_prefix(Key, Delimiter);
        _Prefix ->
            handle_prefix(Key, Prefix, Delimiter)
    end.

-spec handle_undefined_prefix(binary(), binary()) -> boolean().
handle_undefined_prefix(Key, Delimiter) ->
    case binary:match(Key, [Delimiter]) of
        nomatch ->
            false;
        _Match ->
            true
    end.

-spec handle_prefix(binary(), binary(), binary()) -> boolean().
handle_prefix(Key, Prefix, Delimiter) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        <<Prefix:PrefixLen/binary, Rest/binary>> ->
            case binary:match(Rest, [Delimiter]) of
                nomatch ->
                    false;
                _Match ->
                    true
            end;
        _NoPrefix ->
            false
    end.

-spec make_start_key(state()) -> binary().
make_start_key(#state{object_list_ranges=[], req=Request}) ->
    make_start_key_from_marker(Request);
make_start_key(State=#state{object_list_ranges=PrevRanges,
                            common_prefixes=CommonPrefixes}) ->
    case last_result_is_common_prefix(State) of
        true ->
            LastPrefix = lists:last(lists:sort(ordsets:to_list(CommonPrefixes))),
            skip_past_prefix_and_delimiter(LastPrefix);
        false ->
            element(2, lists:last(PrevRanges))
    end.

-spec make_start_key_from_marker(list_object_request()) -> binary().
make_start_key_from_marker(?LOREQ{marker=undefined}) ->
    <<0:8/integer>>;
make_start_key_from_marker(?LOREQ{marker=Marker}) ->
    Marker.

big_end_key(NumBytes) ->
    MaxByte = <<255:8/integer>>,
    iolist_to_binary([MaxByte || _ <- lists:seq(1, NumBytes)]).

-spec map_active_manifests([orddict:orddict()]) -> list(lfs_manifest()).
map_active_manifests(Manifests) ->
    ActiveTuples = [riak_cs_manifest_utils:active_manifest(M) ||
            M <- Manifests],
    [A || {ok, A} <- ActiveTuples].

-spec exclude_key_from_state(state(), list(riakc_obj:riakc_obj())) ->
    list(riakc_obj:riakc_obj()).
exclude_key_from_state(_State, []) ->
    [];
exclude_key_from_state(#state{object_list_ranges=[],
                              req=Request}, Objects) ->
    exclude_marker(Request, Objects);
exclude_key_from_state(#state{last_request_start_key=StartKey}, Objects) ->
    exclude_key(StartKey, Objects).

-spec exclude_marker(list_object_request(), list()) -> list().
exclude_marker(?LOREQ{marker=undefined}, Objects) ->
    Objects;
exclude_marker(?LOREQ{marker=Marker}, Objects) ->
    exclude_key(Marker, Objects).

-spec exclude_key(binary(), list(riakc_obj:riakc_obj())) ->
    list(riakc_obj:riakc_obj()).
exclude_key(Key, [H | T]=Objects) ->
    case riakc_obj:key(H) == Key of
        true ->
            T;
        false ->
            Objects
    end.

-spec skip_past_prefix_and_delimiter(binary()) -> binary().
skip_past_prefix_and_delimiter(<<>>) ->
    <<0:8/integer>>;
skip_past_prefix_and_delimiter(Key) ->
    PrefixSize = byte_size(Key) - 1,
    <<Prefix:PrefixSize/binary, LastByte/binary>> = Key,
    NextByte = next_byte(LastByte),
    <<Prefix/binary, NextByte/binary>>.

-spec next_byte(binary()) -> binary().
next_byte(<<Integer:8/integer>>=Byte) when Integer == 255 ->
    Byte;
next_byte(<<Integer:8/integer>>) ->
    <<(Integer+1):8/integer>>.

-spec try_reply({ok, list_object_response()} | {error, term()},
                state()) ->
    fsm_state_return().
try_reply(Response, State) ->
    NewStateData = State#state{response=Response},
    reply_or_wait(Response, NewStateData).

-spec reply_or_wait({ok, list_object_response()} | {error, term()}, state()) ->
                           fsm_state_return().
reply_or_wait(_Response, State=#state{reply_ref=undefined}) ->
    {next_state, waiting_req, State};
reply_or_wait(Response, State=#state{reply_ref=Ref}) ->
    gen_fsm:reply(Ref, Response),
    Reason = make_reason(Response),
    {stop, Reason, State}.

-spec make_reason({ok, list_object_response()} | {error, term()}) ->
                         normal | term().
make_reason({ok, _Response}) ->
    normal;
make_reason({error, Reason}) ->
    Reason.

update_last_request_state(State=#state{last_request_start_key=StartKey,
                                       object_list_ranges=PrevRanges},
                          []) ->
    NewRange = {StartKey, StartKey},
    State#state{object_list_ranges=PrevRanges ++ [NewRange]};
update_last_request_state(State=#state{last_request_start_key=StartKey,
                                       object_list_ranges=PrevRanges},
                          RiakObjects) ->
    LastObject = lists:last(RiakObjects),
    LastKey = riakc_obj:key(LastObject),
    NewRange = {StartKey, LastKey},
    State#state{object_list_ranges=PrevRanges ++ [NewRange]}.

%% Profiling helper functions
%%--------------------------------------------------------------------

-spec update_profiling_state_with_start(state(), StartKey :: binary(),
                                        EndKey :: binary(),
                                        StartTime :: erlang:timestamp()) ->
    state().
update_profiling_state_with_start(State=#state{profiling=Profiling},
                                  StartKey, EndKey, StartTime) ->
    TempData = {{StartKey, EndKey},
                StartTime},
    NewProfiling = Profiling#profiling{temp_fold_objects_request=TempData},
    State#state{profiling=NewProfiling}.

-spec update_profiling_state_with_end(state(), EndTime :: erlang:timestamp(),
                                      NumKeysReturned :: non_neg_integer()) ->
    state().
update_profiling_state_with_end(State=#state{profiling=Profiling},
                                EndTime, NumKeysReturned) ->
    {KeyRange, StartTime} = Profiling#profiling.temp_fold_objects_request,
    OldRequests = Profiling#profiling.fold_objects_requests,
    NewRequest = {KeyRange, NumKeysReturned, {StartTime, EndTime}},
    NewProfiling = Profiling#profiling{temp_fold_objects_request=undefined,
                                       fold_objects_requests=
                                       [NewRequest | OldRequests]},
    State#state{profiling=NewProfiling}.

-spec extract_timings(list()) -> [{Millis :: non_neg_integer(),
                                   NumResults :: non_neg_integer()}].
extract_timings(Requests) ->
    [extract_timing(R) || R <- Requests].

%% TODO: time to make legit types out of these
-spec extract_timing({term(), non_neg_integer(), {term(), term()}}) ->
    {term(), term()}.
extract_timing({_Range, NumKeysReturned, {StartTime, EndTime}}) ->
    MillisecondDiff = riak_cs_utils:timestamp_to_milliseconds(EndTime) -
                      riak_cs_utils:timestamp_to_milliseconds(StartTime),
    {MillisecondDiff, NumKeysReturned}.

-spec format_profiling_from_state(state()) -> string().
format_profiling_from_state(#state{req=Request,
                                   response={ok, Response},
                                   profiling=Profiling}) ->
    format_profiling(Request, Response, Profiling, self()).

-spec format_profiling(list_object_request(),
                       list_object_response(),
                       profiling(),
                       pid()) -> string().
format_profiling(?LOREQ{max_keys=MaxKeys},
                 ?LORESP{contents=Contents, common_prefixes=CommonPrefixes},
                 #profiling{fold_objects_requests=Requests},
                 Pid) ->
    string:join([io_lib:format("~p: User requested ~p keys", [Pid, MaxKeys]),

                 io_lib:format("~p: We returned ~p objects",
                               [Pid, length(Contents)]),

                 io_lib:format("~p: We returned ~p common prefixes",
                               [Pid, ordsets:size(CommonPrefixes)]),

                 io_lib:format("~p: With fold objects timings: {Millis, NumObjects}: ~p",
                               %% We reverse the Requests in here because they
                               %% were cons'd as they happened.
                               [Pid, extract_timings(lists:reverse(Requests))])],
                io_lib:nl()).
