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
         write/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tid :: ets:tid()}).

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

-spec write(binary(), term()) -> ok.
write(Key, Value) ->
    try
        TS = riak_cs_utils:timestamp(os:timestamp()),
        lager:debug("Writing entry for ~p to LO Cache", [Key]),
        ets:insert(?LIST_OBJECTS_CACHE, {Key, Value, TS}),
        erlang:send_after(?CACHE_TIMEOUT, ?MODULE, {cache_expiry, Key}),
        ok
    catch
        _:Reason ->
            lager:warning("List objects cache write failed. Reason: ~p", [Reason]),
            ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(list()) -> {ok, state()} | {error, term()}.
init([]) ->
    Tid = new_table(),
    NewState = #state{tid=Tid},
    {ok, NewState}.

handle_call(get_tid, _From, State=#state{tid=Tid}) ->
    {reply, Tid, State};
handle_call(Msg, _From, State) ->
    lager:debug("got unknown message: ~p", [Msg]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({cache_expiry, ExpiredKey}, State) ->
    ets:delete(?LIST_OBJECTS_CACHE, ExpiredKey),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec new_table() -> ets:tid().
new_table() ->
    TableSpec = [public, set, named_table, {write_concurrency, true}],
    ets:new(?LIST_OBJECTS_CACHE, TableSpec).

-spec format_lookup_result([{binary(), term(), integer()}]) -> cache_lookup_result().
format_lookup_result([]) ->
    false;
format_lookup_result([{_, Value, _}]) ->
    {true, Value}.
