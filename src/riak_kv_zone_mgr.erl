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
         capabilities/2,
         capabilities/3,
         get/4,
         get_object/5,                          % capability: uses_r_object
         put/6,
         put_object/6,                          % capability: uses_r_object
         %% delete/4,
         drop/2,
         %% fold_buckets/4,
         %% fold_keys/4,
         %% fold_objects/4,
         is_empty/2,
         foodelme/0
        ]).
-export([halt/1]).

-compile(export_all).                           % TODO debugging only!
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          zone :: integer(),
          mod :: atom(),
          mod_config :: list(),
          s_dict :: dict()
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
start_link(Zone, Mod, Config)
  when is_integer(Zone), is_atom(Mod), is_list(Config) ->
    gen_server:start_link(?MODULE, {Zone, Mod, Config}, []).

zone_name(Zone)
  when is_integer(Zone) ->
    list_to_atom("riak_kv_zone_" ++ integer_to_list(Zone)).

capabilities(Zone, Partition)
  when is_integer(Zone), is_integer(Partition) ->
    capabilities(Zone, Partition, <<"no such bucket">>).

capabilities(Zone, Partition, Bucket)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {capabilities, Partition, Bucket}).

drop(Zone, Partition)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {drop, Partition}).

get(Zone, Partition, Bucket, Key)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {get, Partition, Bucket, Key}).

get_object(Zone, Partition, Bucket, Key, WantsBinary)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {get_object, Partition, Bucket, Key, WantsBinary}).

put(Zone, Partition, Bucket, Key, IndexSpecs, EncodedVal)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {put, Partition, Bucket, Key, IndexSpecs, EncodedVal}).

put_object(Zone, Partition, Bucket, Key, IndexSpecs, RObj)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {put_object, Partition, Bucket, Key, IndexSpecs, RObj}).

is_empty(Zone, Partition)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {is_empty, Partition}).

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
init({ZoneNumber, Mod, ModConfig}) ->
    register(zone_name(ZoneNumber), self()),
    try
        {ok, #state{zone=ZoneNumber,
                    mod=Mod,
                    mod_config=ModConfig,
                    s_dict=dict:new()}}
    catch X:Y ->
            error_logger:error_msg("~s:init(~p, ~p, ~p) -> ~p ~p @\n~p\n",
                                   [?MODULE, ZoneNumber, Mod, ModConfig, X, Y,
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
handle_call({capabilities, Partition, Bucket}, _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    Res = Mod:capabilities(Bucket, S),
    {reply, Res, NewState};
handle_call({get, Partition, Bucket, Key}, _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    {R1, R2, _NewS} = Mod:get(Bucket, Key, S),
    %% TODO: to be a good player, NewS* should be saved in our s_dict
    {reply, {ok, R1, R2}, NewState};
handle_call({get_object, Partition, Bucket, Key, WantsBinary},
            _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    {R1, R2} = try
                   {X1, X2, _NewSx} = Mod:get_object(Bucket, Key, WantsBinary, S),
                   {X1, X2}
               catch
                   error:undef ->
                       {Y1, Y2, _NewSy} = Mod:get(Bucket, Key, S),
                       {Y1, Y2}
               end,
    %% TODO: to be a good player, NewS* should be saved in our s_dict
    if R1 == ok, is_binary(R2), WantsBinary == false ->
            {reply, {ok, R1, riak_object:from_binary(Bucket, Key, R2)}, NewState};
       true ->
            {reply, {ok, R1, R2}, NewState}
    end;
handle_call({put, Partition, Bucket, Key, IndexSpecs, EncodedVal},
            _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    case Mod:put(Bucket, Key, IndexSpecs, EncodedVal, S) of
        {ok, _NewS} ->
            %% TODO: to be a good player, NewS* should be saved in our s_dict
            {reply, ok, NewState};
        {error, Reason, _NewS} ->
            %% TODO: to be a good player, NewS* should be saved in our s_dict
            {reply, Reason, NewState}
    end;
handle_call({put_object, Partition, Bucket, Key, IndexSpecs, RObj},
            _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    try
        Res = case Mod:put_object(Bucket, Key, IndexSpecs, RObj, S) of
                  {{ok, _NewS}, EncodedVal} ->
                      %% TODO: to be a good player, NewS* should be
                      %% saved in our s_dict
                      {ok, EncodedVal};
                  Else ->
                      %% TODO: to be a good player, NewS* should be
                      %% saved in our s_dict
                      Else
              end,
        {reply, Res, NewState}
    catch error:undef ->
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            EncodedVal2 = riak_object:to_binary(ObjFmt, RObj),
            Res2 = case Mod:put(Bucket, Key, IndexSpecs, EncodedVal2, S) of
                       {ok, _NewS2} ->
                           %% TODO: to be a good player, NewS* should be
                           %% saved in our s_dict
                           {ok, EncodedVal2};
                       Else2 ->
                           Else2
                   end,
            {reply, Res2, NewState}
    end;
handle_call({drop, Partition}, _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    Res = Mod:drop(S),
    {reply, Res, NewState};
handle_call({is_empty, Partition}, _From, #state{mod=Mod} = State) ->
    {NewState, S} = get_partition_state(Partition, State),
    Res = Mod:is_empty(S),
    {reply, Res, NewState};
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
terminate(Reason, _State) ->
io:format("~p TODOODODODOD clean up the dict here, shutdown!!!!!!!!!!\n", [Reason]),
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

get_partition_state(Partition,
                    #state{s_dict=SD, mod=Mod, mod_config=ModConfig} = State) ->
    case dict:find(Partition, SD) of
        {ok, S} ->
            {State, S};
        error ->
            %% TODO: handle module start error here
            {ok, S} = Mod:start(0, ModConfig),
            {State#state{s_dict=dict:store(Partition, S, SD)}, S}
    end.

