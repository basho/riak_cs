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
         %% get_object/4,                          % capability: uses_r_object
         %% put_object/5,                          % capability: uses_r_object
         %% delete/4,
         drop/1,
         %% fold_buckets/4,
         %% fold_keys/4,
         %% fold_objects/4,
         is_empty/1,
         status/1,
         callback/3
        ]).

-define(API_VERSION, 1).
-define(CAPABILITIES, [uses_r_object, async_fold, write_once_keys]).

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
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(Bucket, State) ->
    [{Z_number, _}|_] = State#state.zone_list,
    Caps = riak_kv_zone_mgr:capabilities(Z_number, Bucket),
    {ok, Caps}.

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

%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()}.
drop(#state{zone_list=Zs} = State) ->
    [_ = riak_kv_zone_mgr:drop(Zone) || {Zone, _} <- Zs],
    {ok, State}.

%% @doc Returns true if this backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(State) ->
    lists:foldl(fun(_Zone, false) ->
                        false;
                   (Zone, _) ->
                        riak_kv_zone_mgr:is_empty(Zone)
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

%%%%%%%%%%%%%%%%%%%
%% TEST
%%%%%%%%%%%%%%%%%%%

t0() ->
    {ok, S} = start(0, [{zone_be_name, riak_kv_yessir_backend},
                        {zone_list, t_zones()}]),

    {ok, ?API_VERSION} = api_version(),
    {ok, Caps} = capabilities(S),
    {ok, Caps} = capabilities(<<"foobucket">>, S),

    {ok, _} = drop(S),
    false = is_empty(S),                        % yessir is always full

    stop(S),
    ok.

t_zones() ->
    [{1, []},
     {2, []},
     {3, []}].
    
