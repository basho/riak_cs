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

%% @doc

%% TODO:
%% 1. optimize for when list keys returns [], we shouldn't
%% even have to do map-reduce then

-module(riak_cs_list_objects_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([start_link/3,
         start_link/5,
         get_object_list/1,
         get_internal_state/1]).

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

-record(profiling, {
        %% floating point secs
        list_keys_start_time :: float(),
        list_keys_end_time :: float(),
        list_keys_num_results :: non_neg_integer(),

        temp_mr_req :: {Request :: {StartIdx :: non_neg_integer(),
                                    EndIdx :: non_neg_integer()},
                        NumKeysRequested :: non_neg_integer(),
                        StartTime :: float()},
        mr_requests=[] :: [{Request :: {StartIdx :: non_neg_integer(),
                                        EndIdx :: non_neg_integer()},
                            NumKeysRequested :: non_neg_integer(),
                            Timing :: {StartTime :: float(),
                                       EndTime :: float()}}]}).
-type profiling() :: #profiling{}.


-record(state, {riakc_pid :: pid(),
                caller_pid :: pid(),
                req :: list_object_request(),

                reply_ref :: undefined | {pid(), any()},

                %% list keys ----
                list_keys_req_id :: undefined | non_neg_integer(),
                key_buffer=[] :: undefined | list(),
                keys=[] :: undefined | list(),
                %% we cache the number of keys because
                %% `length/1' is linear time
                num_keys :: undefined | non_neg_integer(),

                filtered_keys :: undefined | list(),

                %% The number of keys that could be used
                %% for a map-reduce request. This accounts
                %% the `marker' that may be in the request.
                %% This number should always be =< `num_keys'.
                %% This field is used to help determine whether
                %% we have enough results yet from map-reduce
                %% to fullfill our query.
                num_considerable_keys :: undefined | non_neg_integer(),

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

                response :: undefined | list_object_response(),

                req_profiles=#profiling{} :: profiling(),

                %% whether or not to bypass the cache entirely
                use_cache :: boolean(),
                %% Key to use to check for cached results from key listing
                cache_key :: term(),
                common_prefixes=ordsets:new() :: list_objects_common_prefixes()}).

%% some useful shared types

-type state() :: #state{}.

-type fsm_state_return() :: {next_state, atom(), state()} |
                            {next_state, atom(), state(), non_neg_integer()} |
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

-type key_and_manifest() :: {binary(), lfs_manifest()}.
-type manifests_and_prefixes() :: {list(key_and_manifest()), ordsets:ordset(binary())}.

-type tagged_item() :: {prefix, binary()} |
                       {manifest, {binary(), lfs_manifest()}}.

-type tagged_item_list() :: list(tagged_item()).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), list_object_request(), term()) ->
    {ok, pid()} | {error, term()}.
start_link(RiakcPid, ListKeysRequest, CacheKey) ->
    start_link(RiakcPid, self(), ListKeysRequest, CacheKey, true).

-spec start_link(pid(), pid(), list_object_request(), term(), UseCache :: boolean()) ->
    {ok, pid()} | {error, term()}.
start_link(RiakcPid, CallerPid, ListKeysRequest, CacheKey, UseCache) ->
    gen_fsm:start_link(?MODULE, [RiakcPid, CallerPid, ListKeysRequest, CacheKey, UseCache], []).

-spec get_object_list(pid()) ->
    {ok, list_object_response()} |
    {error, term()}.
get_object_list(FSMPid) ->
    gen_fsm:sync_send_all_state_event(FSMPid, get_object_list, infinity).

get_internal_state(FSMPid) ->
    gen_fsm:sync_send_all_state_event(FSMPid, get_internal_state, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init(list()) -> {ok, prepare, state(), 0}.
init([RiakcPid, CallerPid, Request, CacheKey, UseCache]) ->
    %% TODO: should we be linking or monitoring
    %% the proc that called us?

    %% TODO: this should not be hardcoded. Maybe there should
    %% be two `start_link' arities, and one will use a default
    %% val from app.config and the other will explicitly
    %% take a val
    KeyMultiplier = riak_cs_config:key_list_multiplier(),

    State = #state{riakc_pid=RiakcPid,
                   caller_pid=CallerPid,
                   key_multiplier=KeyMultiplier,
                   req=Request,
                   use_cache=UseCache,
                   cache_key=CacheKey},
    {ok, prepare, State, 0}.

-spec prepare(timeout, state()) -> fsm_state_return().
prepare(timeout, State=#state{riakc_pid=RiakcPid,
                              req=Request}) ->
    maybe_fetch_key_list(RiakcPid, Request, State).

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
    State2 = update_state_with_mr_end_profiling(State),
    maybe_map_reduce(State2);
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
    NewStateData = State#state{reply_ref=From},
    {next_state, StateName, NewStateData};
handle_sync_event(get_internal_state, _From, StateName, State) ->
    Reply = {StateName, State},
    {reply, Reply, StateName, State};
handle_sync_event(Event, _From, StateName, State) ->
    _ = lager:debug("got unknown event ~p in state ~p", [Event, StateName]),
    Reply = ok,
    {reply, Reply, StateName, State}.

%% the responses from `riakc_pb_socket:stream_list_keys'
%% come back as regular messages, so just pass
%% them along as if they were gen_server events.
handle_info(Info, waiting_list_keys, State) ->
    waiting_list_keys(Info, State);
handle_info(Info, waiting_map_reduce, State) ->
    waiting_map_reduce(Info, State).

terminate(normal, _StateName, #state{req_profiles=Profilings}) ->
    _ = print_profiling(Profilings),
    ok;
terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

%% List Keys stuff
%%--------------------------------------------------------------------

-spec maybe_fetch_key_list(pid(), list_object_request(), state()) ->
    fsm_state_return().
maybe_fetch_key_list(RiakcPid, Request, State=#state{use_cache=true,
                                                     cache_key=CacheKey}) ->
    CacheResult = riak_cs_list_objects_ets_cache:lookup(CacheKey),
    fetch_key_list(RiakcPid, Request, State, CacheResult);
maybe_fetch_key_list(RiakcPid, Request, State=#state{use_cache=false}) ->
    fetch_key_list(RiakcPid, Request, State, false).

-spec maybe_write_to_cache(state(), list()) -> ok.
maybe_write_to_cache(#state{use_cache=false}, _ListofListofKeys) ->
    ok;
maybe_write_to_cache(#state{cache_key=CacheKey,
                            caller_pid=CallerPid}, ListofListofKeys) ->
    case riak_cs_list_objects_ets_cache:can_write(CacheKey,
                                                  CallerPid,
                                                  lists:flatlength(ListofListofKeys)) of
        true ->
            _ = lager:debug("writing to the cache"),
            riak_cs_list_objects_ets_cache:write(CacheKey, ListofListofKeys);
        false ->
            _ = lager:debug("not writing to the cache"),
            ok
    end.

%% @doc Either proceed using the cached key list or make the request
%% to start a key listing.
-type cache_lookup_result() :: {true, [binary()]} | false.
-spec fetch_key_list(pid(), list_object_request(), state(), cache_lookup_result()) -> fsm_state_return().
fetch_key_list(_, _, State, {true, Value}) ->
    _ = lager:debug("Using cached key list"),
    NewState = prepare_state_for_first_mapred(Value, State#state{key_buffer=Value}),
    maybe_map_reduce(NewState);
fetch_key_list(RiakcPid, Request, State, false) ->
    _ = lager:debug("Requesting fresh key list"),
    handle_streaming_list_keys_call(
      make_list_keys_request(RiakcPid, Request),
      State).

%% function to create a list keys request
%% TODO:
%% could this also be a phase-less map-reduce request
%% with key filters?
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
    ListKeysStartTime = riak_cs_utils:timestamp_to_seconds(os:timestamp()),
    Profiling2 = #profiling{list_keys_start_time=ListKeysStartTime},
    {next_state, waiting_list_keys, State#state{list_keys_req_id=ReqID,
                                                req_profiles=Profiling2}};
handle_streaming_list_keys_call({error, Reason}, State) ->
    {stop, Reason, State}.

-spec handle_keys_done(state()) -> fsm_state_return().
handle_keys_done(State=#state{key_buffer=ListofListofKeys}) ->
    %% TODO: this could potentially be pretty expensive
    %% and memory instensive. More reason to think about starting
    %% to only keep a smaller buffer. See comment in
    %% `handle_keys_received'
    SortedFlattenedKeys = lists:sort(lists:flatten(ListofListofKeys)),
    ok = maybe_write_to_cache(State, SortedFlattenedKeys),
    NewState = prepare_state_for_first_mapred(SortedFlattenedKeys, State),
    maybe_map_reduce(NewState).

-spec prepare_state_for_first_mapred(list(), state()) -> state().
prepare_state_for_first_mapred(KeyList, State=#state{req=Request,
                                                       req_profiles=Profiling}) ->
    NumKeys = length(KeyList),

    %% profiling info
    EndTime = riak_cs_utils:timestamp_to_seconds(os:timestamp()),
    Profiling2 = Profiling#profiling{list_keys_num_results=NumKeys,
                                     list_keys_end_time=EndTime},

    FilteredKeys = filtered_keys_from_request(Request,
                                              KeyList,
                                              NumKeys),
    TotalCandidateKeys = length(FilteredKeys),
    State#state{keys=undefined,
                num_keys=NumKeys,
                num_considerable_keys=TotalCandidateKeys,
                filtered_keys=FilteredKeys,
                %% free up the space
                key_buffer=undefined,
                req_profiles=Profiling2}.

handle_keys_received(Keys, State=#state{key_buffer=PrevKeyBuffer}) ->
    %% TODO:
    %% this is where we might eventually do a 'top-k' keys
    %% kind of thing, like
    %% `lists:sublist(lists:sort([Keys | PrevKeyBuffer]), BufferSize)'
    NewState = State#state{key_buffer=[lists:sort(Keys) | PrevKeyBuffer]},
    {next_state, waiting_list_keys, NewState}.

-spec manifests_and_prefix_slice(manifests_and_prefixes(), non_neg_integer()) ->
    tagged_item_list().
manifests_and_prefix_slice(ManifestsAndPrefixes, MaxObjects) ->
    TaggedList = tagged_manifest_and_prefix(ManifestsAndPrefixes),
    Sorted = lists:sort(fun tagged_sort_fun/2, TaggedList),
    lists:sublist(Sorted, MaxObjects).

-spec tagged_sort_fun(tagged_item(), tagged_item()) ->
    boolean().
tagged_sort_fun(A, B) ->
    AKey = key_from_tag(A),
    BKey = key_from_tag(B),
    AKey =< BKey.

-spec key_from_tag(tagged_item()) -> binary().
key_from_tag({manifest, {Key, _M}}) ->
    Key;
key_from_tag({prefix, Key}) ->
    Key.

-spec tagged_manifest_and_prefix(manifests_and_prefixes()) ->
    tagged_item_list().
tagged_manifest_and_prefix({Manifests, Prefixes}) ->
    tagged_manifest_list(Manifests) ++ tagged_prefix_list(Prefixes).

-spec tagged_manifest_list(list(key_and_manifest())) ->
    list({manifest, key_and_manifest()}).
tagged_manifest_list(KeyAndManifestList) ->
    [{manifest, M} || M <- KeyAndManifestList].

-spec tagged_prefix_list(list(binary())) ->
    list({prefix, binary()}).
tagged_prefix_list(Prefixes) ->
    [{prefix, P} || P <- ordsets:to_list(Prefixes)].

-spec untagged_manifest_and_prefix(tagged_item_list()) ->
    manifests_and_prefixes().
untagged_manifest_and_prefix(TaggedInput) ->
    Pred = fun({manifest, _}) -> true;
              (_Else) -> false end,
    {A, B} = lists:partition(Pred, TaggedInput),
    {[element(2, M) || M <- A],
     [element(2, P) || P <- B]}.

-spec manifests_and_prefix_length(manifests_and_prefixes()) -> non_neg_integer().
manifests_and_prefix_length({KeyAndManifestList, Prefixes}) ->
    length(KeyAndManifestList) + ordsets:size(Prefixes).

-spec filter_prefix_keys(KeyAndManifestList :: list(manifests_and_prefixes()),
                         CommonPrefixes :: ordsets:ordset(binary()),
                         list_object_request()) ->
    manifests_and_prefixes().
filter_prefix_keys(KeyAndManifestList, CommonPrefixes, ?LOREQ{prefix=undefined,
                                                              delimiter=undefined}) ->
    {KeyAndManifestList, CommonPrefixes};
filter_prefix_keys(KeyAndManifestList, CommonPrefixes, ?LOREQ{prefix=Prefix,
                                                              delimiter=Delimiter}) ->
    PrefixFilter =
        fun(KeyAndManifest, Acc) ->
                prefix_filter(KeyAndManifest, Acc, Prefix, Delimiter)
        end,
    lists:foldl(PrefixFilter, {[], CommonPrefixes}, KeyAndManifestList).

prefix_filter({Key, _Manifest}=KeyAndManifest, Acc, undefined, Delimiter) ->
    Group = extract_group(Key, Delimiter),
    update_keys_and_prefixes(Acc, KeyAndManifest, <<>>, 0, Group);
prefix_filter({Key, _Manifest}=KeyAndManifest,
              {KeyAndManifestList, Prefixes}=Acc, Prefix, undefined) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        << Prefix:PrefixLen/binary, _/binary >> ->
            {[KeyAndManifest | KeyAndManifestList], Prefixes};
        _ ->
            Acc
    end;
prefix_filter({Key, _Manifest}=KeyAndManifest,
              {_KeyAndManifestList, _Prefixes}=Acc, Prefix, Delimiter) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        << Prefix:PrefixLen/binary, Rest/binary >> ->
            Group = extract_group(Rest, Delimiter),
            update_keys_and_prefixes(Acc, KeyAndManifest, Prefix, PrefixLen, Group);
        _ ->
            Acc
    end.

extract_group(Key, Delimiter) ->
    case binary:match(Key, [Delimiter]) of
        nomatch ->
            nomatch;
        {Pos, Len} ->
            binary:part(Key, {0, Pos+Len})
    end.

update_keys_and_prefixes({KeyAndManifestList, Prefixes},
                         KeyAndManifest, _, _, nomatch) ->
    {[KeyAndManifest | KeyAndManifestList], Prefixes};
update_keys_and_prefixes({KeyAndManifestList, Prefixes},
                         _, Prefix, PrefixLen, Group) ->
    NewPrefix = << Prefix:PrefixLen/binary, Group/binary >>,
    {KeyAndManifestList, ordsets:add_element(NewPrefix, Prefixes)}.

%% Map Reduce stuff
%%--------------------------------------------------------------------

-spec maybe_map_reduce(state()) -> fsm_state_return().
maybe_map_reduce(State=#state{object_buffer=ObjectBuffer,
                              common_prefixes=CommonPrefixes,
                              req=Request,
                              num_considerable_keys=TotalCandidateKeys}) ->
    ManifestsAndPrefixes = {ObjectBuffer, CommonPrefixes},
    Enough = enough_results(ManifestsAndPrefixes, Request, TotalCandidateKeys),
    handle_enough_results(Enough, State).

handle_enough_results(true, State) ->
    have_enough_results(State);
handle_enough_results(false, State=#state{num_considerable_keys=TotalCandidateKeys,
                                          mr_requests=MapRRequests}) ->
    MoreQuery = could_query_more_mapreduce(MapRRequests, TotalCandidateKeys),
    handle_could_query_more_map_reduce(MoreQuery, State).

-spec handle_could_query_more_map_reduce(boolean(), state()) ->
    fsm_state_return().
handle_could_query_more_map_reduce(true,
                                   State=#state{req=Request,
                                                riakc_pid=RiakcPid}) ->
    NewStateData = prepare_state_for_mapred(State),
    KeysToQuery = next_keys_from_state(NewStateData),
    BucketName = Request?LOREQ.name,
    ManifestBucketName = riak_cs_utils:to_bucket_name(objects, BucketName),
    MapReduceRequestResult = make_map_reduce_request(RiakcPid,
                                                     ManifestBucketName,
                                                     KeysToQuery),
    handle_map_reduce_call(MapReduceRequestResult, NewStateData);
handle_could_query_more_map_reduce(false, State) ->
    have_enough_results(State).

prepare_state_for_mapred(State=#state{req=Request,
                                      key_multiplier=KeyMultiplier,
                                      mr_requests=PrevRequests,
                                      object_buffer=ObjectBuffer}) ->
    TotalNeeded = Request?LOREQ.max_keys,
    NewReq = next_mr_query_spec(PrevRequests,
                                TotalNeeded,
                                length(ObjectBuffer),
                                KeyMultiplier),
    State#state{mr_requests=PrevRequests ++ [NewReq]}.

-spec make_response(list_object_request(), list(), list()) ->
    list_object_response().
make_response(Request=?LOREQ{max_keys=NumKeysRequested}, ObjectBuffer, CommonPrefixes) ->
    ObjectPrefixTuple = {ObjectBuffer, CommonPrefixes},
    NumObjects = manifests_and_prefix_length(ObjectPrefixTuple),
    IsTruncated = NumObjects > NumKeysRequested andalso NumKeysRequested > 0,
    SlicedTaggedItems = manifests_and_prefix_slice(ObjectPrefixTuple,
                                                   NumKeysRequested),
    {NewManis, NewPrefixes} = untagged_manifest_and_prefix(SlicedTaggedItems),
    KeyContents = lists:map(fun response_transformer/1, NewManis),
    riak_cs_list_objects:new_response(Request, IsTruncated, NewPrefixes,
                                      KeyContents).

response_transformer({_Key, Manifest}) ->
    riak_cs_list_objects:manifest_to_keycontent(Manifest).

-spec next_mr_query_spec(list(),
                         non_neg_integer(),
                         non_neg_integer(),
                         float()) ->
    {integer(), integer()}.
next_mr_query_spec([], TotalNeeded, _NumHaveSoFar, KeyMultiplier) ->
    StartIdx = 1,
    EndIdx = round_up(TotalNeeded * KeyMultiplier),
    {StartIdx, EndIdx};
next_mr_query_spec(PrevRequests, TotalNeeded, NumHaveSoFar, KeyMultiplier) ->
    MoreNeeded = TotalNeeded - NumHaveSoFar,
    StartIdx = element(2, lists:last(PrevRequests)) + 1,
    EndIdx = round_up((StartIdx + MoreNeeded) * KeyMultiplier),
    {StartIdx, EndIdx}.

next_keys_from_state(#state{mr_requests=Requests,
                            filtered_keys=FilteredKeys}) ->
    next_keys(lists:last(Requests), FilteredKeys).

next_keys({StartIdx, EndIdx}, Keys) ->
    %% the last arg to sublist is _not_ an index, but
    %% a length, hence the diff of `EndIdx' and `StartIdx'
    Length = (EndIdx - StartIdx) + 1,
    lists:sublist(Keys, StartIdx, Length).


-spec handle_mapred_results(list(), state()) ->
    fsm_state_return().
handle_mapred_results(Results, State=#state{object_buffer=Buffer,
                                            common_prefixes=OldPrefixes,
                                            req=Request}) ->
    CleanedResults = lists:map(fun clean_key_and_manifest/1, Results),
    {NoPrefix, Prefixes} = filter_prefix_keys(CleanedResults, OldPrefixes, Request),
    NewBuffer = update_buffer(NoPrefix, Buffer),
    NewState = State#state{object_buffer=NewBuffer,
                           common_prefixes=Prefixes},
    {next_state, waiting_map_reduce, NewState}.

%% @doc Results come back (for historical reasons...?) like this
%% from the map/reduce call. Rather than change the m/r code
%% (which would make rolling upgrades more difficult), we just
%% transform the values of the list here.
-spec clean_key_and_manifest({binary(), {ok, lfs_manifest()}}) ->
    {binary(), lfs_manifest()}.
clean_key_and_manifest({Key, {ok, Manifest}}) ->
    {Key, Manifest}.

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
    riakc_pb_socket:mapred_stream(RiakcPid,
                                  BKeyTuples,
                                  mapred_query(),
                                  self(),
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
    State2 = State#state{map_red_req_id=ReqID},
    State3 = update_state_with_mr_start_profiling(State2),
    {next_state, waiting_map_reduce, State3};
handle_map_reduce_call({error, Reason}, State) ->
    {stop, Reason, State}.

-spec enough_results(manifests_and_prefixes(),
                     list_object_request(),
                     non_neg_integer()) ->
    boolean().
enough_results(ManifestsAndPrefixes, ?LOREQ{max_keys=MaxKeysRequested}, TotalCandidateKeys) ->
    ResultsLength = manifests_and_prefix_length(ManifestsAndPrefixes),
    %% we have enough results if one of two things is true:
    %% 1. we have more results than requested
    %% 2. there are less keys than were requested even possible
    %% (where 'possible' includes filtering for things like
    %% `marker' and `prefix'

    %% add 1 to `MaxKeysRequested' because we need to know if there
    %% are more active manifests after this key, so we can
    %% correctly return `isTruncated'
    ResultsLength >= erlang:min(MaxKeysRequested + 1, TotalCandidateKeys).

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
have_enough_results(State=#state{reply_ref=undefined,
                                 req=Request,
                                 object_buffer=ObjectBuffer,
                                 common_prefixes=CommonPrefixes}) ->
    Response = make_response(Request, ObjectBuffer, CommonPrefixes),
    NewStateData = State#state{response=Response},
    {next_state, done, NewStateData, timer:seconds(60)};
have_enough_results(State=#state{reply_ref=ReplyPid,
                                 req=Request,
                                 object_buffer=ObjectBuffer,
                                 common_prefixes=CommonPrefixes}) ->
    Response = make_response(Request, ObjectBuffer, CommonPrefixes),
    _ = gen_fsm:reply(ReplyPid, {ok, Response}),
    NewStateData = State#state{response=Response},
    {stop, normal, NewStateData}.

-spec filtered_keys_from_request(list_object_request(),
                                 list(binary()),
                                 non_neg_integer()) ->
    list(binary()).
filtered_keys_from_request(?LOREQ{marker=Marker,
                                  prefix=Prefix}, KeyList, KeyListLength) ->
    AfterMarker = maybe_filter_marker(Marker, KeyList, KeyListLength),
    maybe_filter_prefix(Prefix, AfterMarker).

-spec maybe_filter_marker(undefined | binary(),
                          list(binary()),
                          non_neg_integer()) ->
    list(binary()).
maybe_filter_marker(undefined, KeyList, _KeyListLength) ->
    KeyList;
maybe_filter_marker(Marker, KeyList, KeyListLength) ->
    filter_marker(Marker, KeyList, KeyListLength).

-spec maybe_filter_prefix(undefined | binary(),
                          list(binary())) ->
    list(binary()).
maybe_filter_prefix(undefined, KeyList) ->
    KeyList;
maybe_filter_prefix(Prefix, KeyList) ->
    filter_prefix(Prefix, KeyList).

filter_marker(Marker, KeyList, KeyListLength) ->
    MarkerIndex = index_of_first_greater_element(KeyList, Marker),
    lists:sublist(KeyList, MarkerIndex, KeyListLength).

filter_prefix(Prefix, KeyList) ->
    PrefixFun = build_prefix_fun(Prefix),
    lists:filter(PrefixFun, KeyList).

build_prefix_fun(Prefix) ->
    PrefixLen = byte_size(Prefix),
    fun(Elem) ->
            case Elem of
                <<Prefix:PrefixLen/binary, _/binary>> ->
                    true;
                _Else ->
                    false
            end
    end.

%% only works for positive numbers
-spec round_up(float()) -> integer().
round_up(X) ->
    erlang:round(X + 0.5).

%% @doc Return the index (1-based) where
%% all list members are > than `Element'.
%% If `List' is empty, `1'
%% is returned. If `Element' is greater than all elements
%% in `List', then `length(List) + 1' is returned.
%% `List' must be <em>sorted</em> and contain only unique elements.
-spec index_of_first_greater_element(list(non_neg_integer()), term()) -> pos_integer().
index_of_first_greater_element(List, Element) ->
    index_of_first_greater_element_helper(List, Element, 1).

index_of_first_greater_element_helper([], _Element, 1) ->
    1;
index_of_first_greater_element_helper([Fst], Element, Index) when Element < Fst ->
    Index;
index_of_first_greater_element_helper([_Fst], _Element, Index) ->
    Index + 1;
index_of_first_greater_element_helper([Head | _Rest], Element, Index) when Element < Head ->
    Index;
index_of_first_greater_element_helper([_Head | Rest], Element, Index) ->
    index_of_first_greater_element_helper(Rest, Element, Index + 1).

%% Profiling helper functions
%%--------------------------------------------------------------------

update_state_with_mr_start_profiling(State=#state{req_profiles=Profiling,
                                                  mr_requests=MRRequests}) ->
    {StartIdx, EndIdx}=LastReq = lists:last(MRRequests),
    Start = riak_cs_utils:timestamp_to_seconds(os:timestamp()),
    NumKeysRequested = EndIdx - StartIdx,
    TempReq = {LastReq, NumKeysRequested, Start},
    Profiling2 = Profiling#profiling{temp_mr_req=TempReq},
    State#state{req_profiles=Profiling2}.

update_state_with_mr_end_profiling(State=#state{req_profiles=Profiling}) ->
    PrevMrRequests = Profiling#profiling.mr_requests,
    {Req, NumKeys, Start} = Profiling#profiling.temp_mr_req,
    EndTime = riak_cs_utils:timestamp_to_seconds(os:timestamp()),
    CompletedProfile = {Req, NumKeys, {Start, EndTime}},
    NewMrRequests = PrevMrRequests ++ [CompletedProfile],
    Profiling2 = Profiling#profiling{temp_mr_req=undefined,
                                   mr_requests=NewMrRequests},
    State#state{req_profiles=Profiling2}.

print_profiling(Profiling) ->
    _ = lager:debug(format_list_keys_profile(Profiling)),
    _ = lager:debug(format_map_reduce_profile(Profiling)).

format_list_keys_profile(#profiling{list_keys_start_time=undefined,
                                    list_keys_num_results=NumResults}) ->
    io_lib:format("A cached list keys result of ~p keys was used", [NumResults]);
format_list_keys_profile(#profiling{list_keys_start_time=Start,
                                    list_keys_end_time=End,
                                    list_keys_num_results=NumResults}) ->
    SecondsDiff = End - Start,
    io_lib:format("List keys returned ~p keys in ~6.2f seconds", [NumResults,
                                                                  SecondsDiff]).

format_map_reduce_profile(#profiling{mr_requests=MRRequests}) ->
    string:concat("Map Reduce timings: ",
                  format_map_reduce_profile_helper(MRRequests)).

format_map_reduce_profile_helper(MRRequests) ->
    string:join(lists:map(fun format_single_mr_profile/1, MRRequests),
                "~n").

format_single_mr_profile({_Request, NumKeysRequested, {Start, End}}) ->
    TimeDiff = End - Start,
    io_lib:format("~p keys in ~6.2f seconds", [NumKeysRequested, TimeDiff]).
