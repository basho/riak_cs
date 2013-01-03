%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

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
         write/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(DICTMODULE, dict).

-record(state, {tid :: ets:tid(),
                monitor_to_timer = ?DICTMODULE:new() :: ?DICTMODULE(),
                key_to_monitor = ?DICTMODULE:new() :: ?DICTMODULE()}).

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
        lager:debug("Reading info for ~p from cache", [Key]),
        format_lookup_result(ets:lookup(?LIST_OBJECTS_CACHE, Key))
    catch
        _:Reason ->
            lager:warning("List objects cache lookup failed. Reason: ~p", [Reason]),
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
            lager:warning("List objects cache write failed. Reason: ~p", [Reason]),
            ok
    end.

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
    lager:debug("got unknown message: ~p", [Msg]),
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
%%% Internal functions
%%%===================================================================

-spec should_write(non_neg_integer()) -> boolean().
should_write(NumKeys) ->
    enough_keys_to_cache(NumKeys) andalso space_in_cache(NumKeys).

-spec enough_keys_to_cache(non_neg_integer()) -> boolean().
enough_keys_to_cache(NumKeys) when NumKeys < ?MIN_KEYS_TO_CACHE ->
    false;
enough_keys_to_cache(_NumKeys) ->
    true.

%% @doc Based on current ETS usage and some estimate, decide whether
%% or not we can fit these keys in the cache
%%
%% Current estimate is ~ 64 bytes per key.
-spec space_in_cache(non_neg_integer()) -> boolean().
space_in_cache(NumKeys) ->
    space_in_cache(?LIST_OBJECTS_CACHE, NumKeys).

space_in_cache(TableID, NumKeys) ->
    WordsUsed = ets:info(TableID, memory),
    BytesUsed = words_to_bytes(WordsUsed),
    space_available(BytesUsed, NumKeys).

words_to_bytes(Words) ->
    Words * ?WORD_SIZE.

space_available(BytesUsed, NumKeys) ->
    space_available(BytesUsed, num_keys_to_bytes(NumKeys), ?MAX_CACHE_BYTES).

space_available(BytesUsed, ProspectiveAdd, MaxCacheSize)
        when (BytesUsed + ProspectiveAdd) < MaxCacheSize ->
    true;
space_available(_BytesUsed, _ProspectiveAdd, _MaxCacheSize) ->
    false.

num_keys_to_bytes(NumKeys) ->
    NumKeys * 64.


unsafe_write(Key, Value) ->
    TS = riak_cs_utils:timestamp(os:timestamp()),
    lager:debug("Writing entry for ~p to LO Cache", [Key]),
    ets:insert(?LIST_OBJECTS_CACHE, {Key, Value, TS}),
    ok.

-spec update_state_with_refs(binary(), pid(), state()) -> state().
update_state_with_refs(CacheKey, MonitorPid, State) ->
    MonitorRef = erlang:monitor(process, MonitorPid),
    TimerRef = erlang:send_after(?CACHE_TIMEOUT, ?MODULE,
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
    ets:delete(?LIST_OBJECTS_CACHE, ExpiredKey),
    NewKeyToMon = remove_monitor(ExpiredKey, KeyToMon),
    State#state{key_to_monitor=NewKeyToMon}.

-spec handle_down(reference(), state()) -> state().
handle_down(MonitorRef, State=#state{monitor_to_timer=MonToTimer}) ->
    NewMonToTimer = remove_timer(MonitorRef, MonToTimer),
    State#state{monitor_to_timer=NewMonToTimer}.

-spec remove_monitor(binary(), ?DICTMODULE()) -> ?DICTMODULE().
remove_monitor(ExpiredKey, KeyToMon) ->
    RefResult = safe_fetch(ExpiredKey, KeyToMon),
    case RefResult of
        {ok, Ref} ->
            true = erlang:demonitor(Ref);
        {error, _Reason} ->
            true
    end,
    ?DICTMODULE:erase(ExpiredKey, KeyToMon).

-spec remove_timer(reference(), ?DICTMODULE()) -> ?DICTMODULE().
remove_timer(MonitorRef, MonToTimer) ->
    RefResult = safe_fetch(MonitorRef, MonToTimer),
    _ = case RefResult of
        {ok, {TimerRef, CacheKey}} ->
            %% can be true | false
            _ = ets:delete(?LIST_OBJECTS_CACHE, CacheKey),
            erlang:cancel_timer(TimerRef);
        {error, _Reason} ->
            true
    end,
    ?DICTMODULE:erase(MonitorRef, MonToTimer).

-spec safe_fetch(Key :: term(), Dict :: ?DICTMODULE()) ->
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
    ets:new(?LIST_OBJECTS_CACHE, TableSpec).

-spec format_lookup_result([{binary(), term(), integer()}]) -> cache_lookup_result().
format_lookup_result([]) ->
    false;
format_lookup_result([{_, Value, _}]) ->
    {true, Value}.
