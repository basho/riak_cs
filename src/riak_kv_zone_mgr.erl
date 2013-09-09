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
%%
%% A zone manager is an Erlang process that "owns" the riak_kv storage
%% manager backend that manages a single zone's storage.  For example, if
%% the node has 3 zones, and each zone uses the fs2 storage management
%% backend from riak_kv, then there will be 5 gen_servers using this module.
%% Each gen_server will handle a single instance of the fs2 backend.
%%
%%       |-zone-1-gen_server-|-fs2-backend-instance-
%%       |
%% |-----|-zone-2-gen_server-|-fs2-backend-instance-
%%       |
%%       |-zone-3-gen_server-|-fs2-backend-instance-
%%
%% Each zone manager gen_server process will have a registered name,
%% e.g. riak_kv_zone_N where N is an integer.  This name will be used by lhe
%% riak_kv_zone_backend module when it needs to forward (e.g., via
%% gen_server:call()) an get/put/whatever operation to a particular zone.
%%
%% Older drafts of this module used a backend storage manager instance per
%% vnode/partition.  That scheme was simple(r) to code for, but uses too
%% many storage manager instances.  For example, if we had a machine with 64
%% disks and therefore up to 64 separate zones, and if the Riak node had 20
%% vnodes, then there would be 64 * 20 = 1,280 storage manager instances in
%% use simultaneously.  More most backends, memory/capacity planning &
%% configuration/tuning parameter planning is difficult enough for 10
%% instances; 1280 instances is far too many.
%%
%% The alternative is to play games with prefixes of the keys stored by each
%% zone: add a vnode/partition prefix to the key for each write, and strip
%% off the vnode prefix from the key for each write.  That is what we'll do.
%% The prefix length should be configurable: 2 bytes for up to 64K
%% partitions.

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
         delete/5,
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
          be_state :: term(),
          p2z_dets :: reference(),
          p2z_map :: dict()
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

delete(Zone, Partition, Bucket, Key, IndexSpecs)
  when is_integer(Zone), is_integer(Partition) ->
    call_zone(Zone, {delete, Partition, Bucket, Key, IndexSpecs}).

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
    PDataDir = app_helper:get_env(riak_core, platform_data_dir),
    DetsName = lists:flatten(io_lib:format("zone_p2z_~w_map", [ZoneNumber])),
    DetsFile = lists:flatten(io_lib:format("~s/zone_p2z_~w/map",
                                           [PDataDir, ZoneNumber])),
    try
        %% TODO: handle module start error here
        {ok, BE_State} = Mod:start(0, ModConfig),
        filelib:ensure_dir(DetsFile),
        {ok, Dets} = dets:open_file(DetsName, [{access, read_write},
                                               {file, DetsFile},
                                               {repair, true}]),
        P2Z = dets_to_dict(Dets),
        {ok, #state{zone=ZoneNumber,
                    mod=Mod,
                    mod_config=ModConfig,
                    be_state=BE_State,
                    p2z_dets=Dets,
                    p2z_map=P2Z}}
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
handle_call({capabilities, Partition, Bucket}, _From,
            #state{mod=Mod, be_state=BE_state} = State) ->
    {NewState, _ZPrefix} = get_partition_prefix(Partition, State),
    Res = Mod:capabilities(Bucket, BE_state),
    {reply, Res, NewState};
handle_call({get, Partition, Bucket, Key}, _From,
            #state{mod=Mod, be_state=BE_state} = State) ->
    {NewState, ZPrefix} = get_partition_prefix(Partition, State),
    ZBucket = make_zbucket(ZPrefix, Bucket, State),
    {R1, R2, NewBE_state} = Mod:get(ZBucket, Key, BE_state),
    {reply, {ok, R1, R2}, NewState#state{be_state=NewBE_state}};
handle_call({get_object, Partition, Bucket, Key, WantsBinary},
            _From, #state{mod=Mod, be_state=BE_state} = State) ->
    {NewState, ZPrefix} = get_partition_prefix(Partition, State),
    ZBucket = make_zbucket(ZPrefix, Bucket, State),
    {R1, R2, NewBE_state} =
        try
            Mod:get_object(ZBucket, Key, WantsBinary, BE_state)
        catch
            error:undef ->
                Mod:get(ZBucket, Key, BE_state)
        end,
    if R1 == ok, is_binary(R2), WantsBinary == false ->
            {reply,
             {ok, R1, riak_object:from_binary(Bucket, Key, R2)},
             NewState#state{be_state=NewBE_state}};
       true ->
            {reply, {ok, R1, R2}, NewState#state{be_state=NewBE_state}}
    end;
handle_call({put, Partition, Bucket, Key, IndexSpecs, EncodedVal},
            _From, #state{mod=Mod, be_state=BE_state} = State) ->
    {NewState, ZPrefix} = get_partition_prefix(Partition, State),
    ZBucket = make_zbucket(ZPrefix, Bucket, State),
    case Mod:put(ZBucket, Key, IndexSpecs, EncodedVal, BE_state) of
        {ok, NewBE_state} ->
            {reply, ok, NewState#state{be_state=NewBE_state}};
        {error, Reason, NewBE_state} ->
            {reply, Reason, NewState#state{be_state=NewBE_state}}
    end;
handle_call({put_object, Partition, Bucket, Key, IndexSpecs, RObj},
            _From, #state{mod=Mod, be_state=BE_state} = State) ->
    {NewState, ZPrefix} = get_partition_prefix(Partition, State),
    ZBucket = make_zbucket(ZPrefix, Bucket, State),
    try
        Res = case Mod:put_object(ZBucket, Key, IndexSpecs, RObj, BE_state) of
                  {{ok, NewBE_state}, EncodedVal} ->
                      {ok, EncodedVal};
                  Else ->
                      NewBE_state = BE_state,
                      Else
              end,
        {reply, Res, NewState#state{be_state=NewBE_state}}
    catch error:undef ->
            ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
            EncodedVal2 = riak_object:to_binary(ObjFmt, RObj),
            Res2 = case Mod:put(Bucket, Key, IndexSpecs, EncodedVal2,
                                BE_state) of
                       {ok, NewBE_state2} ->
                           {ok, EncodedVal2};
                       Else2 ->
                           NewBE_state2 = BE_state,
                           Else2
                   end,
            {reply, Res2, NewState#state{be_state=NewBE_state2}}
    end;
handle_call({delete, Partition, Bucket, Key, IndexSpecs},
            _From, #state{mod=Mod, be_state=BE_state} = State) ->
    {NewState, ZPrefix} = get_partition_prefix(Partition, State),
    ZBucket = make_zbucket(ZPrefix, Bucket, State),
    Res = case Mod:delete(ZBucket, Key, IndexSpecs, BE_state) of
              {ok, NewBE_state} ->
                  ok;
              {error, Reason, NewBE_state} ->
                  {error, Reason}
          end,
    %% TODO: to be a good player, NewS* should be saved in our p2z_map
    {reply, Res, NewState#state{be_state=NewBE_state}};
handle_call({drop, Partition}, _From, State) ->
    NewState = delete_partition_prefix(Partition, State),
    {reply, yupyup, NewState};
handle_call({is_empty, Partition}, _From, #state{mod=Mod,
                                                 be_state=BE_state} = State) ->
    {NewState, _ZPrefix} = get_partition_prefix(Partition, State),
    Res = Mod:is_empty(BE_state),
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
terminate(Reason, #state{zone=Zone, mod=Mod, be_state=BE_state,
                         p2z_dets=Dets} = _State) ->
    io:format("DBG: Zone ~p, stopping for Reason ~p\n", [Zone, Reason]),
    (catch Mod:stop(BE_state)),
    dets:close(Dets),
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

get_partition_prefix(Partition, #state{p2z_map=P2Z} = State) ->
    case dict:find(Partition, P2Z) of
        {ok, ZPrefix} ->
            {State, ZPrefix};
        error ->
            {ZPrefix, NewP2Z} = assign_partition_prefix(Partition, State),
            {State#state{p2z_map=NewP2Z}, ZPrefix}
    end.

assign_partition_prefix(Partition, #state{p2z_dets=Dets} = State) ->
    DKey = {admin, largest},
    LargestPrefix = case dets:lookup(Dets, DKey) of
                        [{DKey, LP}] ->
                            LP;
                        [] ->
                            -1
                    end,
    ZPrefix = LargestPrefix + 1,
    ok = dets:insert(Dets, {DKey, ZPrefix}),
    ok = sync_dets(Dets),
    ok = dets:insert(Dets, {Partition, ZPrefix}),
    ok = sync_dets(Dets),
    NewP2Z = dets_to_dict(State#state.p2z_dets),
    {ZPrefix, NewP2Z}.

delete_partition_prefix(Partition, #state{p2z_dets=Dets} = State) ->
    %% TODO: Delete old keys
    %%   1. Keep track of partition/ZPrefix deletions in progress
    %%   2. Restart partition/ZPrefix deletion to finish interrupted deletions
    dets:delete(Dets, Partition),
    ok = sync_dets(Dets),
    P2Z = dets_to_dict(Dets),
    State#state{p2z_map=P2Z}.

dets_to_dict(Dets) ->
    L = dets:foldl(fun(T, Acc) -> [T|Acc] end, [], Dets),
    Sanity =
        fun({Partition, Idx}, D) when is_integer(Partition) ->
                case dict:find(Idx, D) of
                    error ->
                        dict:store(Idx, Partition, D);
                    {ok, UsedPartition} ->
                        error_logger:error_msg("TODO: duplicate index ~p used by both ~p and ~p, ignoring the latter", [Idx, UsedPartition, Partition]),
                        D
                end;
           (_, D) ->
                %% This clause gracefully skips keys like {admin, largest}
                D
        end,
    D2 = lists:foldl(Sanity, dict:new(), L),
    dict:from_list([{Partition, Idx} || {Idx, Partition} <- dict:to_list(D2)]).

make_zbucket(ZPrefix, Bucket, _State) ->
    <<ZPrefix:16, Bucket/binary>>.

sync_dets(Dets) ->
    ok = dets:sync(Dets),
    os:cmd("/bin/sync"),
    ok.

%%% TEST

smoke0() ->
    BsCs = [{riak_kv_bitcask_backend, [{data_root, "./test-deleteme"}]},
            {riak_kv_memory_backend, []},
            {riak_kv_eleveldb_backend, [{create_if_missing, true},
                                        {write_buffer_size, 32*1024}]}],
    [ok = smoke0_int(BE, Config) || {BE, Config} <- BsCs].

smoke0_int(Backend, BE_config) ->
    B = <<"bucket">>,
    K = <<"key">>,
    {ok, Z42a} = ?MODULE:start_link(42, Backend, BE_config),
    ok = ?MODULE:put(42, 1, B, K, [], <<"val1">>),
    ok = ?MODULE:put(42, 2, B, K, [], <<"val2">>),
    ok = ?MODULE:put(42, 3, B, K, [], <<"val3">>),
    ok = ?MODULE:halt(Z42a),

    {ok, Z42b} = ?MODULE:start_link(42, Backend, BE_config),
    if Backend == riak_kv_memory_backend ->
            skip;
       true ->
            {ok, ok, <<"val3">>} = ?MODULE:get(42, 3, B, K),
            {ok, ok, <<"val2">>} = ?MODULE:get(42, 2, B, K),
            {ok, ok, <<"val1">>} = ?MODULE:get(42, 1, B, K),
            {ok, error, not_found} = ?MODULE:get(42, 929382398, B, K),
            {ok, error, not_found} = ?MODULE:get(42, 1, B, <<"does not exist">>)
    end,

    O = riak_object:new(B, K, <<"Hello, world!">>),
    O_bin = riak_object:to_binary(v0, O),

    ok = ?MODULE:put(42, 700, B, K, [], O_bin),
    {ok, ok, O_bin} = ?MODULE:get_object(42, 700, B, K, true),
    {ok, ok, O} = ?MODULE:get_object(42, 700, B, K, false),
    %% OK, now do the same for put_object()
    {ok, _} = ?MODULE:put_object(42, 700, B, K, [], O),
    {ok, ok, O_bin} = ?MODULE:get_object(42, 700, B, K, true),
    {ok, ok, O} = ?MODULE:get_object(42, 700, B, K, false),

    [ok = ?MODULE:delete(42, Part, B, K, []) || Part <- [1,2,3]],
    %% Delete again, still ok (because backend doesn't care if K doesn't exist)
    [ok = ?MODULE:delete(42, Part, B, K, []) || Part <- [1,2,3]],
    [{ok, error, not_found} = ?MODULE:get(42, Part, B, K) || Part <- [1,2,3]],

    %% Drop test: put stuff in, drop, the nothing exists
    DropVal = <<"dropval!">>,
    [ok = ?MODULE:put(42, Part, B, <<Key:32>>, [], DropVal) ||
        Part <- [2,3,4], Key <- lists:seq(1,100)],
    ?MODULE:drop(42, 2),
    [{ok, error, not_found} = ?MODULE:get(42, Part, B, <<Key:32>>) ||
        Part <- [2], Key <- lists:seq(1,100)],
    [{ok, ok, DropVal} = ?MODULE:get(42, Part, B, <<Key:32>>) ||
        Part <- [3,4], Key <- lists:seq(1,100)],

    ok = ?MODULE:halt(Z42b),
    ok.

t1() ->
    RefBE = riak_kv_memory_backend,
    Part1 = 1,
    _Part2 = 2,
    Zone10 = 10,

    {ok, _ZoneS} = ?MODULE:start_link(Zone10, RefBE, []),
    {ok, _MemS} = RefBE:start(Part1, []),

    ok.
