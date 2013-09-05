%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_kv_zone_backend: storage engine based on local storage zones
%%      managed by riak_kv backends.

-module(riak_kv_zone_backend).

-compile(export_all).                           % TODO debugging only!
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         get_object/4,                          % capability: uses_r_object
         put/5,
         put_object/5,                          % capability: uses_r_object
         delete/4,
         drop/1,
         %% fold_buckets/4,
         %% fold_keys/4,
         %% fold_objects/4,
         is_empty/1,
         status/1,
         callback/3
        ]).

-define(API_VERSION, 1).
-define(CAPABILITIES, [uses_r_object, async_fold]).

-record(state, {
          partition :: integer(),
          zone_list :: list(),
          num :: integer(),
          mgrs :: list(pid())
         }).

-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(State) ->
    capabilities(fake_bucket_name, State).

%% @doc Return the capabilities of the backend.
%%
%% NOTE: We assume that all zones are homogenous, so we only fetch the
%%       capabilities list from the first zone.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(Bucket, #state{partition=Partition, zone_list=[{Zone, _}|_]}) ->
    Caps = riak_kv_zone_mgr:capabilities(Zone, Partition, Bucket),
    {ok, ?CAPABILITIES ++ Caps}.

start(Partition, Config) ->
    Mod = get_prop_or_env(zone_be_name, Config, riak_kv, undefined),
    ZoneList = get_prop_or_env(zone_list, Config, riak_kv, []),
    Mgrs = [Pid || {Zone, ZConfig} <- ZoneList,
                   {ok, Pid} <- [riak_kv_zone_mgr:start_link(Zone, Mod, ZConfig)]],
    NumZones = length(ZoneList),
    
    {ok, #state{partition=Partition,
                zone_list=ZoneList,
                num=NumZones,
                mgrs=Mgrs}}.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(#state{mgrs=Mgrs}) ->
    [catch riak_kv_zone_mgr:halt(M) || M <- Mgrs],
    ok.

%% @doc Get the object stored at the given bucket/key pair
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, binary(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{partition=Partition} = State) ->
    Zone = chash_to_zone(Bucket, Key, State),
    {ok, R1, R2} = riak_kv_zone_mgr:get(Zone, Partition, Bucket, Key),
    {R1, R2, State}.

%% @doc Get the object stored at the given bucket/key pair
-spec get_object(riak_object:bucket(), riak_object:key(), boolean(), state()) ->
                     {ok, binary() | riak_object:riak_object(), state()} |
                     {ok, not_found, state()} |
                     {error, term(), state()}.
get_object(Bucket, Key, WantsBinary, #state{partition=Partition} = State) ->
    Zone = chash_to_zone(Bucket, Key, State),
    {ok, R1, R2} = riak_kv_zone_mgr:get_object(Zone, Partition,
                                               Bucket, Key, WantsBinary),
    {R1, R2, State}.

%% @doc Insert an object into the memory backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()}.
put(Bucket, Key, IndexSpecs, EncodedVal, #state{partition=Partition} = State) ->
    Zone = chash_to_zone(Bucket, Key, State),
    case riak_kv_zone_mgr:put(Zone, Partition,
                              Bucket, Key, IndexSpecs, EncodedVal) of
        ok ->
            {ok, State};
        Reason ->
            {error, Reason, State}
    end.

%% @doc Store Val under Bucket and Key
%%
%% NOTE: Val is a copy of ValRObj that has been encoded by serialize_term()

-spec put_object(riak_object:bucket(), riak_object:key(), [index_spec()], riak_object:riak_object(), state()) ->
                 {{ok, state()}, EncodedVal::binary()} |
                 {{error, term()}, state()}.
put_object(Bucket, Key, IndexSpecs, RObj, #state{partition=Partition} = State)->
    Zone = chash_to_zone(Bucket, Key, State),
    case riak_kv_zone_mgr:put_object(Zone, Partition,
                                     Bucket, Key, IndexSpecs, RObj) of
        {ok, EncodedVal} ->
            {{ok, State}, EncodedVal};
        Else ->
            {Else, State}
    end.

%% @doc Delete an object from the bitcask backend
%% NOTE: The bitcask backend does not currently support
%% secondary indexing and the_IndexSpecs parameter
%% is ignored.
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, IndexSpecs, #state{partition=Partition}=State) ->
    Zone = chash_to_zone(Bucket, Key, State),
    case riak_kv_zone_mgr:delete(Zone, Partition, Bucket, Key, IndexSpecs) of
        {ok, EncodedVal} ->
            {{ok, State}, EncodedVal};
        Else ->
            {Else, State}
    end.


%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()}.
drop(#state{partition=Partition} = State) ->
    [_ = riak_kv_zone_mgr:drop(Zone, Partition) || Zone <- zone_list(State)],
    {ok, State}.

%% @doc Returns true if this backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{partition=Partition} = State) ->
    lists:foldl(fun(_Zone, false) ->
                        false;
                   (Zone, _) ->
                        riak_kv_zone_mgr:is_empty(Zone, Partition)
                end, true, zone_list(State)).

%% @doc Get the status information for this fs backend
-spec status(state()) -> [no_status_sorry | {atom(), term()}].
status(_S) ->
    [no_status_sorry_TODO].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Term, S) ->
    {ok, S}.

%% ===================================================================
%% Private
%% ===================================================================

%% TODO: ripped from riak_kv_fs2_backend.erl
get_prop_or_env(Key, Properties, App, Default) ->
    case proplists:get_value(Key, Properties) of
        undefined ->
            KV_key = list_to_atom("zone_backend_" ++ atom_to_list(Key)),
            case application:get_env(App, KV_key) of
                undefined ->
                    get_prop_or_env_default(Default);
                {ok, Value} ->
                    Value
            end;
        Value ->
            Value
    end.

%% TODO: ripped from riak_kv_fs2_backend.erl
get_prop_or_env_default({{Reason}}) ->
    throw(Reason);
get_prop_or_env_default(Default) ->
    Default.

zone_list(#state{zone_list=Zs}) ->
    [Zone || {Zone, _} <- Zs].

chash_to_zone(_Bucket, _Key, #state{zone_list=ZoneList}) ->
    {Zone, _} = lists:last(ZoneList),
    io:format("TODO: fixme chash_to_zone: map to ~p\n", [Zone]),
    Zone.

%%%%%%%%%%%%%%%%%%%
%% TEST
%%%%%%%%%%%%%%%%%%%

t0() ->
    B = <<"b">>,
    K = <<"k">>, 

    {ok, S} = start(0, [{zone_be_name, riak_kv_yessir_backend},
                        {zone_list, t_zones()}]),

    {ok, ?API_VERSION} = api_version(),
    {ok, AllCaps} = capabilities(S),
    {ok, AllCaps} = capabilities(<<"foobucket">>, S),

    {ok, _} = drop(S),
    {ok, _} = drop(S),
    false = is_empty(S),                        % yessir is always full
    false = is_empty(S),                        % yessir is always full

    {ok, XX0, _} = get(B, K, S),
    true = is_binary(XX0),
    {ok, XX1, _} = get_object(B, K, false, S),
    true = is_tuple(XX1),
    {ok, XX2, _} = get_object(B, K, true, S),
    true = is_binary(XX2),

    RObj = riak_object:new(B, K, <<"val!">>),
    {{ok, _}, EncodedValXX3} = put_object(B, K, [], RObj, S),
    true = is_binary(EncodedValXX3),
    {ok, _} = put(B, K, [], EncodedValXX3, S),

    {ok, _} = delete(B, K, [], S),
    {ok, _} = delete(B, K, [], S),              % again, whee

    stop(S),
    ok.

t_zones() ->
    [{1, []},
     {2, []},
     {3, []}].
    
