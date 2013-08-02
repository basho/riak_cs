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

%% @doc riak_kv_zone_mgr: gen_server glue between riak_kv_zone_backend.erl
%%      and zone manager (which uses a real riak_kv backend to do its work).

-module(riak_kv_zone_mgr).

-behaviour(gen_server).

%% API
-export([start_link/3, zone_name/1]).
-export([%api_version/0,
         capabilities/1,
         capabilities/2,
         %% get_object/4,                          % capability: uses_r_object
         %% put_object/5,                          % capability: uses_r_object
         %% delete/4,
         drop/1,
         %% fold_buckets/4,
         %% fold_keys/4,
         %% fold_objects/4,
         is_empty/1,
         foodelme/0
        ]).
-export([halt/1]).

-compile(export_all).                           % TODO debugging only!
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          mod :: atom(),
          s :: term()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Partition, Mod, Config) ->
    gen_server:start_link(?MODULE, {Partition, Mod, Config}, []).

zone_name(Zone) ->
    list_to_atom("riak_kv_zone_" ++ integer_to_list(Zone)).

capabilities(Zone) ->
    capabilities(Zone, <<"no such bucket">>).

capabilities(Zone, Bucket) ->
    call_zone(Zone, {capabilities, Bucket}).

drop(Zone) ->
    call_zone(Zone, {drop}).

is_empty(Zone) ->
    call_zone(Zone, {is_empty}).

halt(Pid) ->
    gen_server:call(Pid, {halt}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({ZoneNumber, Mod, Config}) ->
    register(zone_name(ZoneNumber), self()),
    try
        {ok, S} = Mod:start(0, Config),
        {ok, #state{mod=Mod, s=S}}
    catch X:Y ->
            error_logger:error_msg("~s:init(~p, ~p) -> ~p ~p @\n~p\n",
                                   [?MODULE, ZoneNumber, Config, X, Y,
                                    erlang:get_stacktrace()]),
            {stop, {X,Y}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({halt}, _From, State) ->
    {stop, normal, ok, State};
handle_call({capabilities, Bucket}, _From, #state{mod=Mod, s=S} = State) ->
    Res = Mod:capabilities(Bucket, S),
    {reply, Res, State};
handle_call({drop}, _From, #state{mod=Mod, s=S} = State) ->
    Res = Mod:drop(S),
    {reply, Res, State};
handle_call({is_empty}, _From, #state{mod=Mod, s=S} = State) ->
    Res = Mod:is_empty(S),
    {reply, Res, State};
handle_call(_Request, _From, State) ->
    io:format("~s ~p: call ~p\n", [?MODULE, ?LINE, _Request]),
    {reply, bad_call_buggy_go_home, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    io:format("~s ~p\n", [?MODULE, ?LINE]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    io:format("~s ~p\n", [?MODULE, ?LINE]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

foodelme() ->
    ok.

call_zone(Zone, Msg) ->
    gen_server:call(zone_name(Zone), Msg, infinity).

