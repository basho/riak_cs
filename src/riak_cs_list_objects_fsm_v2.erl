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

-compile(export_all).

%%%===================================================================
%%% Exports
%%%===================================================================

%% API
-export([start_link/2,
         get_object_list/1,
         get_internal_state/1]).

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

-record(state, {riakc_pid :: pid(),
                req :: list_object_request(),
                reply_ref :: undefined | {pid(), any()},
                key_multiplier :: float(),
                object_list_req_id :: undefined | non_neg_integer(),
                reached_end_of_keyspace=false :: boolean(),
                object_buffer=[] :: list(),
                objects=[] :: list(),
                last_request_start_key :: undefined | binary(),
                last_request_num_keys_requested :: undefined | pos_integer(),
                object_list_ranges=[] :: object_list_ranges(),
                response :: undefined |
                            {ok, list_object_response()} |
                            {error, term()},
                common_prefixes=ordsets:new() :: list_objects_common_prefixes()}).

%% some useful shared types

-type state() :: #state{}.

-type fsm_state_return() :: {next_state, atom(), state()} |
                            {next_state, atom(), state(), non_neg_integer()} |
                            {stop, term(), state()}.

-type list_objects_event() :: {ReqID :: non_neg_integer(), done} |
                              {ReqID :: non_neg_integer(), {objects, list()}} |
                              {ReqID :: non_neg_integer(), {error, term()}}.

-type manifests_and_prefixes() :: {list(lfs_manifest()), ordsets:ordset(binary())}.

%% `Start' and `End' are inclusive
-type object_list_range()  :: {Start :: binary(), End :: binary()}.
-type object_list_ranges() :: [object_list_range()].

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

get_internal_state(FSMPid) ->
    gen_fsm:sync_send_all_state_event(FSMPid, get_internal_state, infinity).

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
        {NewStateData, {error, _Reason}}=Error ->
            try_reply(Error, NewStateData)
    end.

-spec waiting_object_list(list_objects_event(), state()) -> fsm_state_return().
waiting_object_list({ReqId, {objects, ObjectList}},
                    State=#state{object_list_req_id=ReqId,
                                 object_buffer=ObjectBuffer}) ->
    NewStateData = State#state{object_buffer=ObjectBuffer ++ ObjectList},
    {next_state, waiting_object_list, NewStateData};
waiting_object_list({ReqId, done}, State=#state{object_list_req_id=ReqId}) ->
    handle_done(State);
waiting_object_list({ReqId, {error, _Reason}=Error},
                    State=#state{object_list_req_id=ReqId}) ->
    try_reply(Error, State).

handle_event(_Event, StateName, State) ->
    %% TODO: log unknown event
    {next_state, StateName, State}.

handle_sync_event(get_object_list, From, StateName, State=#state{response=undefined}) ->
    NewStateData = State#state{reply_ref=From},
    {next_state, StateName, NewStateData};
handle_sync_event(get_object_list, _From, _StateName, State=#state{response=Resp}) ->
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
    ReachedEnd = length(ObjectBuffer) < NumKeysRequested,
    RangeUpdatedStateData = update_last_request_state(State, ObjectBuffer),

    FilteredObjects = exclude_key_from_state(State, ObjectBuffer),
    Manifests = [riak_cs_utils:manifests_from_riak_object(O) ||
                 O <- FilteredObjects],
    Active = map_active_manifests(Manifests),
    NewObjects = PrevObjects ++ Active,
    ObjectPrefixTuple = {NewObjects, CommonPrefixes},
    ObjectPrefixTuple2 = filter_prefix_keys(ObjectPrefixTuple, Request),
    SlicedTaggedItems = manifests_and_prefix_slice(ObjectPrefixTuple2,
                                                   UserMaxKeys),
    {NewManis, NewPrefixes} = untagged_manifest_and_prefix(SlicedTaggedItems),

    NewStateData = RangeUpdatedStateData#state{objects=NewManis,
                                               common_prefixes=NewPrefixes,
                                               reached_end_of_keyspace=ReachedEnd,
                                               object_buffer=[]},
    case enough_results(NewStateData) of
        true ->
            Response = response_from_manifests_and_common_prefixes(Request,
                                                                   {NewManis, NewPrefixes}),
            try_reply({ok, Response}, NewStateData);
        false ->
            RiakcPid = NewStateData#state.riakc_pid,
            case make_2i_request(RiakcPid, NewStateData) of
                {NewStateData2, {ok, ReqId}} ->
                    {next_state, waiting_object_list,
                     NewStateData2#state{object_list_req_id=ReqId}};
                {NewStateData2, {error, _Reason}}=Error ->
                    try_reply(Error, NewStateData2)
            end
    end.

enough_results(#state{req=?LOREQ{max_keys=UserMaxKeys},
                      reached_end_of_keyspace=EndofKeyspace,
                      objects=Objects,
                      common_prefixes=CommonPrefixes}) ->
    riak_cs_list_objects_utils:manifests_and_prefix_length({Objects, CommonPrefixes}) >= UserMaxKeys
    orelse EndofKeyspace.

response_from_manifests_and_common_prefixes(Request,
                                            {Manifests, CommonPrefixes}) ->
    KeyContent = lists:map(fun riak_cs_list_objects:manifest_to_keycontent/1,
                           Manifests),
    case KeyContent of
        [] ->
            riak_cs_list_objects:new_response(Request, false, CommonPrefixes, []);
        _Else ->
            riak_cs_list_objects:new_response(Request, true, CommonPrefixes, KeyContent)
    end.

-spec make_2i_request(pid(), state()) -> [riakc_obj:riakc_obj()].
make_2i_request(RiakcPid, State=#state{req=?LOREQ{name=BucketName}}) ->
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, BucketName),
    StartKey = make_start_key(State),
    EndKey = big_end_key(128),
    NumResults = 1002,
    Opts = [{return_terms, true}, {max_results, NumResults}, {stream, true}],
    NewStateData = State#state{last_request_start_key=StartKey,
                               last_request_num_keys_requested=NumResults},
    Ref = riakc_pb_socket:get_index_range(RiakcPid,
                                          ManifestBucket,
                                          <<"$key">>,
                                          StartKey,
                                          EndKey,
                                          Opts),
    {NewStateData, Ref}.

-spec receive_objects(term()) -> list().
receive_objects(ReqID) ->
    receive_objects(ReqID, []).

receive_objects(ReqId, Acc) ->
    receive
        {ReqId, {objects, List}} ->
            receive_objects(ReqId, Acc ++ List);
        {ReqId, done} ->
            Acc;
        {ReqId, {error, Reason}} ->
            _ = lager:error("yikes, error ~p", [Reason]),
            throw({list_objects_error, Reason});
        Else ->
            throw({unknown_message, Else})
    end.

-spec last_result_is_common_prefix(state()) -> boolean().
last_result_is_common_prefix(#state{object_list_ranges=[]}) ->
    false;
last_result_is_common_prefix(#state{object_list_ranges=Ranges,
                                    req=Request}) ->
    Key = element(2, lists:last(Ranges)),
    key_is_common_prefix(Key, Request).


-spec key_is_common_prefix(binary(), list_object_request()) ->
    boolean().
%% TODO: please refactor this
key_is_common_prefix(_Key, ?LOREQ{delimiter=undefined}) ->
    false;
key_is_common_prefix(Key, ?LOREQ{prefix=Prefix,
                                 delimiter=Delimiter}) ->
    case Prefix of
        undefined ->
            case binary:match(Key, [Delimiter]) of
                nomatch ->
                    false;
                _Match ->
                    true
            end;
        _Prefix ->
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
            end
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

-spec manifests_and_prefix_slice(manifests_and_prefixes(), non_neg_integer()) ->
    riak_cs_list_objects_utils:tagged_item_list().
manifests_and_prefix_slice(ManifestsAndPrefixes, MaxObjects) ->
    TaggedList = tagged_manifest_and_prefix(ManifestsAndPrefixes),
    Sorted = lists:sort(fun tagged_sort_fun/2, TaggedList),
    lists:sublist(Sorted, MaxObjects).

-spec tagged_sort_fun(riak_cs_list_objects_utils:tagged_item(),
                      riak_cs_list_objects_utils:tagged_item()) ->
    boolean().
tagged_sort_fun(A, B) ->
    AKey = key_from_tag(A),
    BKey = key_from_tag(B),
    AKey =< BKey.

-spec key_from_tag(riak_cs_list_objects_utils:tagged_item()) -> binary().
key_from_tag({manifest, ?MANIFEST{bkey={_Bucket, Key}}}) ->
    Key;
key_from_tag({prefix, Key}) ->
    Key.

-spec tagged_manifest_and_prefix(manifests_and_prefixes()) ->
    riak_cs_list_objects_utils:tagged_item_list().
tagged_manifest_and_prefix({Manifests, Prefixes}) ->
    tagged_manifest_list(Manifests) ++ tagged_prefix_list(Prefixes).

-spec tagged_manifest_list(list(lfs_manifest())) ->
    list({manifest, lfs_manifest()}).
tagged_manifest_list(KeyAndManifestList) ->
    [{manifest, M} || M <- KeyAndManifestList].

-spec tagged_prefix_list(list(binary())) ->
    list({prefix, binary()}).
tagged_prefix_list(Prefixes) ->
    [{prefix, P} || P <- ordsets:to_list(Prefixes)].

-spec untagged_manifest_and_prefix(riak_cs_list_objects_utils:tagged_item_list()) ->
    manifests_and_prefixes().
untagged_manifest_and_prefix(TaggedInput) ->
    Pred = fun({manifest, _}) -> true;
              (_Else) -> false end,
    {A, B} = lists:partition(Pred, TaggedInput),
    {[element(2, M) || M <- A],
     [element(2, P) || P <- B]}.

-spec filter_prefix_keys({ManifestList :: list(lfs_manifest()),
                          CommonPrefixes :: ordsets:ordset(binary())},
                         list_object_request()) ->
    manifests_and_prefixes().
filter_prefix_keys({_ManifestList, _CommonPrefixes}=Input, ?LOREQ{prefix=undefined,
                                                                  delimiter=undefined}) ->
    Input;
filter_prefix_keys({ManifestList, CommonPrefixes}, ?LOREQ{prefix=Prefix,
                                                              delimiter=Delimiter}) ->
    PrefixFilter =
        fun(Manifest, Acc) ->
                prefix_filter(Manifest, Acc, Prefix, Delimiter)
        end,
    lists:foldl(PrefixFilter, {[], CommonPrefixes}, ManifestList).

prefix_filter(Manifest=?MANIFEST{bkey={_Bucket, Key}}, Acc, undefined, Delimiter) ->
    Group = extract_group(Key, Delimiter),
    update_keys_and_prefixes(Acc, Manifest, <<>>, 0, Group);
prefix_filter(Manifest=?MANIFEST{bkey={_Bucket, Key}},
              {ManifestList, Prefixes}=Acc, Prefix, undefined) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        << Prefix:PrefixLen/binary, _/binary >> ->
            {[Manifest | ManifestList], Prefixes};
        _ ->
            Acc
    end;
prefix_filter(Manifest=?MANIFEST{bkey={_Bucket, Key}},
              {_ManifestList, _Prefixes}=Acc, Prefix, Delimiter) ->
    PrefixLen = byte_size(Prefix),
    case Key of
        << Prefix:PrefixLen/binary, Rest/binary >> ->
            Group = extract_group(Rest, Delimiter),
            update_keys_and_prefixes(Acc, Manifest, Prefix, PrefixLen, Group);
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

update_keys_and_prefixes({ManifestList, Prefixes},
                         Manifest, _, _, nomatch) ->
    {[Manifest | ManifestList], Prefixes};
update_keys_and_prefixes({ManifestList, Prefixes},
                         _, Prefix, PrefixLen, Group) ->
    NewPrefix = << Prefix:PrefixLen/binary, Group/binary >>,
    {ManifestList, ordsets:add_element(NewPrefix, Prefixes)}.

-spec try_reply(Response :: {ok, list_object_response()} | {error, term()},
                State :: state()) ->
    fsm_state_return().
try_reply(Response, State) ->
    NewStateData = State#state{response=Response},
    reply_or_wait(Response, NewStateData).

reply_or_wait(_Response, State=#state{reply_ref=undefined}) ->
    {next_state, waiting_req, State};
reply_or_wait(Response, State=#state{reply_ref=Ref}) ->
    gen_fsm:reply(Ref, Response),
    Reason = make_reason(Response),
    {stop, Reason, State}.

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

