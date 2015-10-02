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

-module(riak_cs_list_objects_ets_cache).

-behaviour(gen_server).
-include("riak_cs.hrl").
-include("list_objects.hrl").

%% API
-export([start_link/0,
         lookup/1,
         can_write/3,
         can_write/4,
         write/2,
         bytes_used/0,
         bytes_used/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Config getters
-export([default_ets_table/0,
         cache_enabled/0,
         cache_timeout/0,
         min_keys_to_cache/0,
         max_cache_size/0]).

-define(DICTMODULE, dict).

-ifdef(namespaced_types).
-type dictionary() :: dict:dict().
-else.
-type dictionary() :: dict().
-endif.

-record(state, {tid :: ets:tid(),
                monitor_to_timer = ?DICTMODULE:new() :: dictionary(),
                key_to_monitor = ?DICTMODULE:new() :: dictionary()}).

-type state() :: #state{}.
-type cache_lookup_result() :: {true, [binary()]} | false.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    %% named proc
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec lookup(binary()) -> cache_lookup_result().
lookup(Key) ->
    try
        _ = lager:debug("Reading info for ~p from cache", [Key]),
        format_lookup_result(ets:lookup(default_ets_table(), Key))
    catch
        _:Reason ->
            _ = lager:warning("List objects cache lookup failed. Reason: ~p", [Reason]),
            false
    end.

-spec can_write(CacheKey :: binary(),
                MonitorPid :: pid(),
                NumKeys :: non_neg_integer()) -> boolean().
can_write(CacheKey, MonitorPid, NumKeys) ->
    can_write(?MODULE, CacheKey, MonitorPid, NumKeys).

-spec can_write(Pid :: pid() | atom(),
                CacheKey :: binary(),
                MonitorPid :: pid(),
                NumKeys :: non_neg_integer()) -> boolean().
can_write(ServerPid, CacheKey, MonitorPid, NumKeys) ->
    Message = {can_write, {CacheKey, MonitorPid, NumKeys}},
    gen_server:call(ServerPid, Message, infinity).

-spec write(binary(), term()) -> ok.
write(Key, Value) ->
    try
        unsafe_write(Key, Value)
    catch
        _:Reason ->
            _ = lager:warning("List objects cache write failed. Reason: ~p", [Reason]),
            ok
    end.

-spec bytes_used() -> non_neg_integer().
bytes_used() ->
    bytes_used(default_ets_table()).

-spec bytes_used(ets:tab()) -> non_neg_integer().
bytes_used(TableID) ->
    WordsUsed = ets:info(TableID, memory),
    words_to_bytes(WordsUsed).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(list()) -> {ok, state()}.
init([]) ->
    Tid = new_table(),
    NewState = #state{tid=Tid},
    {ok, NewState}.

handle_call(get_tid, _From, State=#state{tid=Tid}) ->
    {reply, Tid, State};
handle_call({can_write, {CacheKey, MonitorPid, NumKeys}},
             _From, State) ->
    Bool = should_write(NumKeys),
    NewState = case Bool of
        true ->
            update_state_with_refs(CacheKey, MonitorPid, State);
        false ->
            State
    end,
    {reply, Bool, NewState};
handle_call(Msg, _From, State) ->
    _ = lager:debug("got unknown message: ~p", [Msg]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({cache_expiry, ExpiredKey}, State) ->
    NewState = handle_cache_expiry(ExpiredKey, State),
    {noreply, NewState};
handle_info({'DOWN', MonitorRef, process, _Pid, _Info}, State) ->
    NewState = handle_down(MonitorRef, State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Config getters
%%%===================================================================

%% @private
get_with_default(EnvKey, Default) ->
    case application:get_env(riak_cs, EnvKey) of
        {ok, Val} ->
            Val;
        undefined ->
            Default
    end.

default_ets_table() ->
    get_with_default(list_objects_ets_cache_table_name, ?LIST_OBJECTS_CACHE).

cache_enabled() ->
    get_with_default(list_objects_ets_cache_enabled, ?ENABLE_CACHE).

cache_timeout() ->
    get_with_default(list_objects_ets_cache_timeout, ?CACHE_TIMEOUT).

min_keys_to_cache() ->
    get_with_default(list_objects_ets_cache_min_keys, ?MIN_KEYS_TO_CACHE).

max_cache_size() ->
    get_with_default(list_objects_ets_cache_max_bytes, ?MAX_CACHE_BYTES).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec should_write(non_neg_integer()) -> boolean().
should_write(NumKeys) ->
    enough_keys_to_cache(NumKeys) andalso space_in_cache(NumKeys).

-spec enough_keys_to_cache(non_neg_integer()) -> boolean().
enough_keys_to_cache(NumKeys) ->
    NumKeys >= min_keys_to_cache().

%% @doc Based on current ETS usage and some estimate, decide whether
%% or not we can fit these keys in the cache
%%
%% Current estimate is ~ 64 bytes per key.
-spec space_in_cache(non_neg_integer()) -> boolean().
space_in_cache(NumKeys) ->
    space_in_cache(default_ets_table(), NumKeys).

space_in_cache(TableID, NumKeys) ->
    BytesUsed = bytes_used(TableID),
    space_available(BytesUsed, NumKeys).

words_to_bytes(Words) ->
    Words * ?WORD_SIZE.

space_available(BytesUsed, NumKeys) ->
    space_available(BytesUsed, num_keys_to_bytes(NumKeys), max_cache_size()).

space_available(BytesUsed, ProspectiveAdd, MaxCacheSize)
        when (BytesUsed + ProspectiveAdd) < MaxCacheSize ->
    true;
space_available(_BytesUsed, _ProspectiveAdd, _MaxCacheSize) ->
    false.

num_keys_to_bytes(NumKeys) ->
    NumKeys * 64.


unsafe_write(Key, Value) ->
    TS = riak_cs_utils:second_resolution_timestamp(os:timestamp()),
    _ = lager:debug("Writing entry for ~p to LO Cache", [Key]),
    ets:insert(default_ets_table(), {Key, Value, TS}),
    ok.

-spec update_state_with_refs(binary(), pid(), state()) -> state().
update_state_with_refs(CacheKey, MonitorPid, State) ->
    MonitorRef = erlang:monitor(process, MonitorPid),
    TimerRef = erlang:send_after(cache_timeout(), ?MODULE,
                                 {cache_expiry, CacheKey}),
    update_state_with_refs_helper(MonitorRef, TimerRef, CacheKey, State).

-spec update_state_with_refs_helper(reference(), reference(), binary(), state()) ->
    state().
update_state_with_refs_helper(MonitorRef, TimerRef, CacheKey,
                              State=#state{monitor_to_timer=MonToTimer,
                                           key_to_monitor=KeyToMon}) ->
    NewMonToTimer = ?DICTMODULE:store(MonitorRef, {TimerRef, CacheKey}, MonToTimer),
    NewKeyToMon = ?DICTMODULE:store(TimerRef, MonitorRef, KeyToMon),
    State#state{monitor_to_timer=NewMonToTimer,
                key_to_monitor=NewKeyToMon}.

-spec handle_cache_expiry(binary(), state()) -> state().
handle_cache_expiry(ExpiredKey, State=#state{key_to_monitor=KeyToMon}) ->
    ets:delete(default_ets_table(), ExpiredKey),
    NewKeyToMon = remove_monitor(ExpiredKey, KeyToMon),
    State#state{key_to_monitor=NewKeyToMon}.

-spec handle_down(reference(), state()) -> state().
handle_down(MonitorRef, State=#state{monitor_to_timer=MonToTimer}) ->
    NewMonToTimer = remove_timer(MonitorRef, MonToTimer),
    State#state{monitor_to_timer=NewMonToTimer}.

-spec remove_monitor(binary(), dictionary()) -> dictionary().
remove_monitor(ExpiredKey, KeyToMon) ->
    RefResult = safe_fetch(ExpiredKey, KeyToMon),
    case RefResult of
        {ok, Ref} ->
            true = erlang:demonitor(Ref);
        {error, _Reason} ->
            true
    end,
    ?DICTMODULE:erase(ExpiredKey, KeyToMon).

-spec remove_timer(reference(), dictionary()) -> dictionary().
remove_timer(MonitorRef, MonToTimer) ->
    RefResult = safe_fetch(MonitorRef, MonToTimer),
    _ = case RefResult of
        {ok, {TimerRef, CacheKey}} ->
            %% can be true | false
            _ = ets:delete(default_ets_table(), CacheKey),
            erlang:cancel_timer(TimerRef);
        {error, _Reason} ->
            true
    end,
    ?DICTMODULE:erase(MonitorRef, MonToTimer).

-spec safe_fetch(Key :: term(), Dict :: dictionary()) ->
    {ok, term()} | {error, term()}.
safe_fetch(Key, Dict) ->
    try
        {ok, ?DICTMODULE:fetch(Key, Dict)}
    catch error:Reason ->
            {error, Reason}
    end.

-spec new_table() -> ets:tid().
new_table() ->
    TableSpec = [public, set, named_table, {write_concurrency, true}],
    ets:new(default_ets_table(), TableSpec).

-spec format_lookup_result([{binary(), term(), integer()}]) -> cache_lookup_result().
format_lookup_result([]) ->
    false;
format_lookup_result([{_, Value, _}]) ->
    {true, Value}.
