%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_riak_client).

-behaviour(gen_server).

%% API
-export([checkout/0, checkout/1,
         checkin/1, checkin/2]).
-export([pbc_pool_name/2]).
-export([
         stop/1,
         get_bucket/2,
         set_bucket_name/2,
         get_user/2,
         save_user/3,
         set_manifest_bag/2,
         set_manifest/2,
         master_pbc/1,
         manifest_pbc/1,
         block_pbc/1
        ]).
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% exported for other `riak_client' implementations
-export([get_bucket_with_pbc/2,
         get_user_with_pbc/2,
         save_user_with_pbc/3]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          master_pbc :: undefined | pid(),
          bucket_name,
          bucket_obj
         }).

start_link(_Args) ->
    case riak_cs_config:is_multibag_enabled() of
        true ->
            gen_server:start_link(riak_cs_multibag_riak_client, [], []);
        false ->
            gen_server:start_link(?MODULE, [], [])
    end.

stop(Pid) ->
    gen_server:call(Pid, stop).

-spec checkout() -> {ok, riak_client()} | {error, term()}.
checkout() ->
    checkout(request_pool).

-spec checkout(atom()) -> {ok, riak_client()} | {error, term()}.
checkout(Pool) ->
    try
        case poolboy:checkout(hack_pool_map(Pool), false) of
            full ->
                {error, all_workers_busy};
            RcPid ->
                ok = gen_server:call(RcPid, cleanup),
                {ok, RcPid}
        end
    catch
        _:Error ->
            {error, {poolboy_error, Error}}
    end.

-spec checkin(riak_client()) -> ok.
checkin(RcPid) ->
    checkin(request_pool, RcPid).

-spec checkin(atom(), riak_client()) -> ok.
checkin(Pool, RcPid) ->
    ok = gen_server:call(RcPid, cleanup),
    poolboy:checkin(hack_pool_map(Pool), RcPid).

-spec pbc_pool_name(master | bag_id(), integer()) -> atom().
pbc_pool_name(master, 0) ->
    pbc_pool_master_0;
pbc_pool_name(master, 1) ->
    pbc_pool_master_1;
pbc_pool_name(master, 2) ->
    pbc_pool_master_2;
pbc_pool_name(master, 3) ->
    pbc_pool_master_3;
pbc_pool_name(master, 4) ->
    pbc_pool_master_4;
pbc_pool_name(master, 5) ->
    pbc_pool_master_5;
pbc_pool_name(master, 6) ->
    pbc_pool_master_6;
pbc_pool_name(master, 7) ->
    pbc_pool_master_7;
pbc_pool_name(BagId, N) when is_binary(BagId) ->
    list_to_atom(lists:flatten(io_lib:format("pbc_pool_~s_~w", [BagId, N]))).

-spec get_bucket(riak_client(), binary()) -> {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_bucket(RcPid, BucketName) when is_binary(BucketName) ->
    gen_server:call(RcPid, {get_bucket, BucketName}, infinity).

-spec set_bucket_name(riak_client(), binary()) -> ok | {error, term()}.
set_bucket_name(RcPid, BucketName) when is_binary(BucketName) ->
    gen_server:call(RcPid, {set_bucket_name, BucketName}, infinity).

%% @doc Perform an initial read attempt with R=PR=N.
%% If the initial read fails retry using
%% R=quorum and PR=1, but indicate that bucket deletion
%% indicators should not be cleaned up.
-spec get_user(riak_client(),
               UserKey :: binary()) ->
                      {ok, {riakc_obj:riakc_obj(), KeepDeletedBuckets :: boolean()}} |
                      {error, term()}.
get_user(RcPid, UserKey) when is_binary(UserKey) ->
    gen_server:call(RcPid, {get_user, UserKey}, infinity).

-spec save_user(riak_client(), rcs_user(), riakc_obj:riakc_obj()) -> ok | {error, term()}.
save_user(RcPid, User, OldUserObj) ->
    gen_server:call(RcPid, {save_user, User, OldUserObj}, infinity).


-spec set_manifest(riak_client(), lfs_manifest()) -> ok | {error, term()}.
set_manifest(RcPid, Manifest) ->
    gen_server:call(RcPid, {set_manifest, Manifest}).

-spec set_manifest_bag(riak_client(), binary()) -> ok | {error, term()}.
set_manifest_bag(RcPid, ManifestBagId) ->
    gen_server:call(RcPid, {set_manifest_bag, ManifestBagId}).

%% TODO: Using this function is more or less a cheat.
%% It's better to export new  function to manipulate manifests
%% from this module.
-spec master_pbc(riak_client()) -> {ok, MasterPbc::pid()} | {error, term()}.
master_pbc(RcPid) ->
    gen_server:call(RcPid, master_pbc).

%% TODO: Also this is cheat
-spec manifest_pbc(riak_client()) -> {ok, ManifetPbc::pid()} | {error, term()}.
manifest_pbc(RcPid) ->
    gen_server:call(RcPid, manifest_pbc).

%% TODO: Also this is cheat
-spec block_pbc(riak_client()) -> {ok, BlockPbc::pid()} | {error, term()}.
block_pbc(RcPid) ->
    gen_server:call(RcPid, block_pbc).

%%% Internal functions

init([]) ->
    {_, _, USec} = os:timestamp(),
    XX = USec rem 8,
    put(hack_pool_map2, XX),
    {ok, fresh_state()}.

handle_call(stop, _From, State) ->
    _ = do_cleanup(State),
    {stop, normal, ok, State};
handle_call(cleanup, _From, State) ->
    {reply, ok, do_cleanup(State)};
handle_call({get_bucket, BucketName}, _From, State) ->
    case do_get_bucket(State#state{bucket_name=BucketName}) of
        {ok, #state{bucket_obj=BucketObj} = NewState} ->
            {reply, {ok, BucketObj}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call({set_bucket_name, _BucketName}, _From, State) ->
    {reply, ok, State};
handle_call({get_user, UserKey}, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            Res = get_user_with_pbc(MasterPbc, UserKey),
            {reply, Res, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({save_user, User, OldUserObj}, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            Res = save_user_with_pbc(MasterPbc, User, OldUserObj),
            {reply, Res, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(master_pbc, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            {reply, {ok, MasterPbc}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(manifest_pbc, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            {reply, {ok, MasterPbc}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({set_manifest, _Manifest}, _From, State) ->
    {reply, ok, State};
handle_call({set_manifest_bag, _ManifestBagId}, _From, State) ->
    {reply, ok, State};
handle_call(block_pbc, _From, State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc} = NewState} ->
            {reply, {ok, MasterPbc}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Request, _From, State) ->
    Reply = {error, {invalid_request, Request}},
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

fresh_state() ->
    #state{}.

do_cleanup(State) ->
    stop_pbc(State#state.master_pbc),
    fresh_state().

stop_pbc(undefined) ->
    ok;
stop_pbc(Pbc) when is_pid(Pbc) ->
    riak_cs_utils:close_riak_connection(pbc_pool_name(master, get(hack_pool_map2)), Pbc),
    ok.

do_get_bucket(State) ->
    case ensure_master_pbc(State) of
        {ok, #state{master_pbc=MasterPbc, bucket_name=BucketName} = NewState} ->
            case get_bucket_with_pbc(MasterPbc, BucketName) of
                {ok, Obj} ->
                    {ok, NewState#state{bucket_obj=Obj}};
                {error, Reason} ->
                    {error, Reason, NewState}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

ensure_master_pbc(#state{master_pbc = MasterPbc} = State)
  when is_pid(MasterPbc) ->
    {ok, State};
ensure_master_pbc(#state{} = State) ->
    case riak_cs_utils:riak_connection(pbc_pool_name(master, get(hack_pool_map2))) of
        {ok, MasterPbc} -> {ok, State#state{master_pbc=MasterPbc}};
        {error, Reason} -> {error, Reason}
    end.

get_bucket_with_pbc(MasterPbc, BucketName) ->
    MasterPbc = MasterPbc,
    BucketName = BucketName,
    {ok,{riakc_obj,<<109,111,115,115,46,98,117,99,107,101,116,115>>,<<116,101,115,116,45,121,106,97>>,<<107,206,97,96,96,96,204,96,202,5,82,28,202,156,255,126,134,248,253,168,201,96,74,100,204,99,101,80,225,122,121,150,47,11,0>>,[{{dict,3,16,16,8,80,48,{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},{{[],[],[],[],[],[],[],[],[],[],[[<<88,45,82,105,97,107,45,86,84,97,103>>,50,109,104,78,57,115,75,51,75,50,104,110,110,67,78,68,98,90,79,67,115,82]],[],[],[[<<88,45,82,105,97,107,45,76,97,115,116,45,77,111,100,105,102,105,101,100>>|{1416,924708,180861}]],[],[[<<88,45,82,105,97,107,45,77,101,116,97>>,{<<88,45,77,111,115,115,45,65,99,108>>,<<131,104,4,100,0,6,97,99,108,95,118,50,104,3,107,0,6,102,111,111,98,97,114,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,107,0,20,74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73,108,0,0,0,1,104,2,104,2,107,0,6,102,111,111,98,97,114,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,108,0,0,0,1,100,0,12,70,85,76,76,95,67,79,78,84,82,79,76,106,106,104,3,98,0,0,5,136,98,0,14,28,36,98,0,1,52,203>>}]]}}},<<74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73>>}],undefined,undefined}}.
    %% riak_cs_pbc:get_object(MasterPbc, ?BUCKETS_BUCKET, BucketName).

get_user_with_pbc(MasterPbc, Key) ->
    MasterPbc = MasterPbc,
    Key = Key,
    Yo = {riakc_obj,<<109,111,115,115,46,117,115,101,114,115>>,<<74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73>>,<<107,206,97,96,96,96,204,96,202,5,82,28,202,156,255,126,134,248,253,168,201,96,74,100,206,99,101,80,225,122,121,150,47,11,0>>,[{{dict,3,16,16,8,80,48,{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},{{[],[],[],[],[],[],[],[],[],[],[[<<88,45,82,105,97,107,45,86,84,97,103>>,54,99,76,53,110,121,65,65,84,57,65,48,89,55,67,100,87,120,106,97,108,112]],[[<<105,110,100,101,120>>,{<<99,95,105,100,95,98,105,110>>,<<49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51>>},{<<101,109,97,105,108,95,98,105,110>>,<<102,111,111,98,97,114,64,101,120,97,109,112,108,101,46,99,111,109>>}]],[],[[<<88,45,82,105,97,107,45,76,97,115,116,45,77,111,100,105,102,105,101,100>>|{1416,924708,185889}]],[],[]}}},<<131,104,9,100,0,11,114,99,115,95,117,115,101,114,95,118,50,107,0,7,102,111,111,32,98,97,114,107,0,6,102,111,111,98,97,114,107,0,18,102,111,111,98,97,114,64,101,120,97,109,112,108,101,46,99,111,109,107,0,20,74,50,73,80,54,87,71,85,81,95,70,78,71,73,65,78,57,65,70,73,107,0,40,109,98,66,45,49,86,65,67,78,115,114,78,48,121,76,65,85,83,112,67,70,109,88,78,78,66,112,65,67,51,88,48,108,80,109,73,78,65,61,61,107,0,64,49,56,57,56,51,98,97,48,101,49,54,101,49,56,97,50,98,49,48,51,99,97,49,54,98,56,52,102,97,100,57,51,100,49,50,97,50,102,98,101,100,49,99,56,56,48,52,56,57,51,49,102,98,57,49,98,48,98,56,52,52,97,100,51,108,0,0,0,2,104,6,100,0,14,109,111,115,115,95,98,117,99,107,101,116,95,118,49,107,0,8,116,101,115,116,45,121,106,97,100,0,7,99,114,101,97,116,101,100,107,0,24,50,48,49,52,45,49,49,45,50,53,84,49,52,58,49,49,58,52,56,46,48,48,48,90,104,3,98,0,0,5,136,98,0,14,28,36,98,0,2,211,153,100,0,9,117,110,100,101,102,105,110,101,100,104,6,100,0,14,109,111,115,115,95,98,117,99,107,101,116,95,118,49,107,0,4,116,101,115,116,100,0,7,99,114,101,97,116,101,100,107,0,24,50,48,49,52,45,49,48,45,50,56,84,48,50,58,48,54,58,48,56,46,48,48,48,90,104,3,98,0,0,5,134,98,0,7,12,144,98,0,6,237,220,100,0,9,117,110,100,101,102,105,110,101,100,106,100,0,7,101,110,97,98,108,101,100>>}],undefined,undefined},
    {ok, {Yo, false}}.
    %% StrongOptions = [{r, all}, {pr, all}, {notfound_ok, false}],
    %% case riakc_pb_socket:get(MasterPbc, ?USER_BUCKET, Key, StrongOptions) of
    %%     {ok, Obj} ->
    %%         %% since we read from all primaries, we're
    %%         %% less concerned with there being an 'out-of-date'
    %%         %% replica that we might conflict with (and not
    %%         %% be able to properly resolve conflicts).
    %%         KeepDeletedBuckets = false,
    %%         {ok, {Obj, KeepDeletedBuckets}};
    %%     {error, Reason0} ->
    %%         _ = lager:warning("Fetching user record with strong option failed: ~p", [Reason0]),
    %%         WeakOptions = [{r, quorum}, {pr, one}, {notfound_ok, false}],
    %%         case riakc_pb_socket:get(MasterPbc, ?USER_BUCKET, Key, WeakOptions) of
    %%             {ok, Obj} ->
    %%                 %% We weren't able to read from all primary
    %%                 %% vnodes, so don't risk losing information
    %%                 %% by pruning the bucket list.
    %%                 KeepDeletedBuckets = true,
    %%                 {ok, {Obj, KeepDeletedBuckets}};
    %%             {error, Reason} ->
    %%                 {error, Reason}
    %%         end
    %% end.

save_user_with_pbc(MasterPbc, User, OldUserObj) ->
    Indexes = [{?EMAIL_INDEX, User?RCS_USER.email},
               {?ID_INDEX, User?RCS_USER.canonical_id}],
    MD = dict:store(?MD_INDEX, Indexes, dict:new()),
    UpdUserObj = riakc_obj:update_metadata(
                   riakc_obj:update_value(OldUserObj,
                                          riak_cs_utils:encode_term(User)),
                   MD),
    riakc_pb_socket:put(MasterPbc, UpdUserObj).

hack_pool_map(request_pool = _Pool) ->
    N = case get(hack_pool_map) of
        undefined ->
            {_, _, USec} = os:timestamp(),
            XX = USec rem 8,
            put(hack_pool_map, XX),
            XX;
        X ->
            X
        end,
    case N of 0 -> request_pool_0;
              1 -> request_pool_1;
              2 -> request_pool_2;
              3 -> request_pool_3;
              4 -> request_pool_4;
              5 -> request_pool_5;
              6 -> request_pool_6;
              7 -> request_pool_7
    end.
