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

-module(riak_cs_block_server).

-behaviour(gen_server).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module,[{gen_server,pulse_gen_server}]}).
-endif.

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

%% API
-export([start_link/1, start_link/2,
         start_block_servers/3,
         get_block/5, get_block/6,
         put_block/6,
         delete_block/5,
         maybe_stop_block_servers/1,
         stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {riak_client :: riak_client(),
                close_riak_connection=true :: boolean(),
                bag_id :: bag_id()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the server
-spec start_link(lfs_manifest()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Manifest) ->
    gen_server:start_link(?MODULE, [Manifest], []).

-spec start_link(lfs_manifest(), riak_client()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Manifest, RcPid) ->
    gen_server:start_link(?MODULE, [Manifest, RcPid], []).

%% @doc Start (up to) 'MaxNumServers'
%% riak_cs_block_server procs.
%% 'RcPid' must be a Pid you already
%% have for a riakc_pb_socket proc. If the
%% poolboy boy returns full, you will be given
%% a list of less than 'MaxNumServers'.

%% TODO: this doesn't guarantee any minimum
%% number of workers. I could also imagine
%% this function looking something
%% like:
%% start_block_servers(RcPid, MinWorkers, MaxWorkers, MinWorkerTimeout)
%% Where the function works something like:
%% Give me between MinWorkers and MaxWorkers,
%% waiting up to MinWorkerTimeout to get at least
%% MinWorkers. If the timeout occurs, this function
%% could return an error, or the pids it has
%% so far (which might be less than MinWorkers).
-spec start_block_servers(lfs_manifest(), riak_client(), pos_integer()) -> [pid()].
start_block_servers(Manifest, RcPid, MaxNumServers) ->
    start_block_servers(Manifest, RcPid, MaxNumServers, []).

start_block_servers(Manifest, RcPid, 1, BlockServers) ->
    {ok, BlockServer} = start_link(Manifest, RcPid),
    [BlockServer | BlockServers];
start_block_servers(Manifest, RcPid, NumWorkers, BlockServers) ->
    case start_link(Manifest) of
        {ok, BlockServer} ->
            start_block_servers(Manifest, RcPid, NumWorkers - 1, [BlockServer | BlockServers]);
        {error, normal} ->
            start_block_servers(Manifest, RcPid, 1, BlockServers)
    end.

-spec get_block(pid(), binary(), binary(), binary(), pos_integer()) -> ok.
get_block(Pid, Bucket, Key, UUID, BlockNumber) ->
    gen_server:cast(Pid, {get_block, self(), Bucket, Key, undefined, UUID, BlockNumber}).

%% @doc get a block which is know to have originated on cluster ClusterID.
%% If it's not found locally, it might get returned from the replication
%% cluster if a connection exists to that cluster. This is proxy-get().
-spec get_block(pid(), binary(), binary(), binary(), binary(), pos_integer()) -> ok.
get_block(Pid, Bucket, Key, ClusterID, UUID, BlockNumber) ->
    gen_server:cast(Pid, {get_block, self(), Bucket, Key, ClusterID, UUID, BlockNumber}).

-spec put_block(pid(), binary(), binary(), binary(), pos_integer(), binary()) -> ok.
put_block(Pid, Bucket, Key, UUID, BlockNumber, Value) ->
    gen_server:cast(Pid, {put_block, self(), Bucket, Key, UUID, BlockNumber,
                          Value, riak_cs_utils:md5(Value)}).

-spec delete_block(pid(), binary(), binary(), binary(), pos_integer()) -> ok.
delete_block(Pid, Bucket, Key, UUID, BlockNumber) ->
    gen_server:cast(Pid, {delete_block, self(), Bucket, Key, UUID, BlockNumber}).

-spec maybe_stop_block_servers(undefined | [pid()]) -> ok.
maybe_stop_block_servers(undefined) ->
    ok;
maybe_stop_block_servers(BlockServerPids) ->
    _ = [stop(P) || P <- BlockServerPids],
    ok.

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Manifest]) ->
    case riak_cs_riak_client:checkout(request_pool) of
        {ok, RcPid} ->
            init(Manifest, RcPid, #state{close_riak_connection=true});
        {error, _Reason} ->
            {stop, normal}
    end;
init([Manifest, RcPid]) ->
    init(Manifest, RcPid, #state{close_riak_connection=false}).

init(Manifest, RcPid, State) ->
    process_flag(trap_exit, true),
    ok = riak_cs_riak_client:set_manifest(RcPid, Manifest),
    BagId = riak_cs_mb_helper:bag_id_from_manifest(Manifest),
    {ok, State#state{riak_client=RcPid, bag_id=BagId}}.

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
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

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

handle_cast({get_block, ReplyPid, Bucket, Key, ClusterID, UUID, BlockNumber},
            State=#state{riak_client=RcPid, bag_id=BagId}) ->
    get_block(ReplyPid, Bucket, Key, ClusterID, BagId, UUID, BlockNumber, RcPid),
    {noreply, State};
handle_cast({put_block, ReplyPid, Bucket, Key, UUID, BlockNumber, Value, BCSum},
            State=#state{riak_client=RcPid}) ->
    dt_entry(<<"put_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    MD = make_md_usermeta([{?USERMETA_BUCKET, Bucket},
                           {?USERMETA_KEY, Key},
                           {?USERMETA_BCSUM, BCSum}]),
    FailFun = fun(Error) ->
                      _ = lager:error("Put ~p ~p UUID ~p block ~p failed: ~p\n",
                                      [Bucket, Key, UUID, BlockNumber, Error])
              end,
    %% TODO: Handle put failure here.
    ok = do_put_block(FullBucket, FullKey, <<>>, Value, MD, RcPid, FailFun),
    riak_cs_put_fsm:block_written(ReplyPid, BlockNumber),
    dt_return(<<"put_block">>, [BlockNumber], [Bucket, Key]),
    {noreply, State};
handle_cast({delete_block, ReplyPid, Bucket, Key, UUID, BlockNumber}, State=#state{riak_client=RcPid}) ->
    dt_entry(<<"delete_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    StartTime = os:timestamp(),
    Timeout = riak_cs_config:get_block_timeout(),

    %% do a get first to get the vclock (only do a head request though)
    GetOptions = [{r, 1}, {notfound_ok, false}, {basic_quorum, false}, head],
    _ = case riakc_pb_socket:get(block_pbc(RcPid), FullBucket, FullKey, GetOptions, Timeout) of
            {ok, RiakObject} ->
                ok = delete_block(RcPid, ReplyPid, RiakObject, {UUID, BlockNumber});
        {error, notfound} ->
            %% If the block isn't found, assume it's been
            %% previously deleted by another delete FSM, and
            %% move on to the next block.
            riak_cs_delete_fsm:block_deleted(ReplyPid, {ok, {UUID, BlockNumber}})
    end,
    ok = riak_cs_stats:update_with_start(block_delete, StartTime),
    dt_return(<<"delete_block">>, [BlockNumber], [Bucket, Key]),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

get_block(ReplyPid, Bucket, Key, ClusterId, BagId, UUID, BlockNumber, RcPid) ->
    %% don't use proxy get if it's a local get
    %% or proxy get is disabled
    ProxyActive = riak_cs_config:proxy_get_active(),
    UseProxyGet = use_proxy_get(ClusterId, BagId),

    case riak_cs_utils:n_val_1_get_requests() of
        true ->
            do_get_block(ReplyPid, Bucket, Key, ClusterId, UseProxyGet, ProxyActive, UUID,
                         BlockNumber, RcPid);
        false ->
            normal_nval_block_get(ReplyPid, Bucket, Key, ClusterId,
                                  UseProxyGet, UUID, BlockNumber, RcPid)
    end.

do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive,
             UUID, BlockNumber, RcPid) ->
    MaxRetries = riak_cs_config:get_env(riak_cs, block_get_max_retries, 5),
    do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive,
                 UUID, BlockNumber, RcPid, MaxRetries, 0).

do_get_block(ReplyPid, _Bucket, _Key, _ClusterID, _UseProxyGet, _ProxyActive,
             UUID, BlockNumber, _RcPid, MaxRetries, NumRetries)
  when is_atom(NumRetries) orelse NumRetries > MaxRetries ->
    Sorry = {error, notfound},
    ok = riak_cs_get_fsm:chunk(ReplyPid, {UUID, BlockNumber}, Sorry);
do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive,
             UUID, BlockNumber, RcPid, MaxRetries, NumRetries) ->
    ok = sleep_retries(NumRetries),

    dt_entry(<<"get_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),

    StartTime = os:timestamp(),
    GetOptions1 = n_val_one_options(),
    GetOptions2 = r_one_options(),

    ProceedFun = fun(OkReply) ->
            ok = riak_cs_stats:update_with_start(block_get_retry, StartTime),
            ok = riak_cs_get_fsm:chunk(ReplyPid, {UUID, BlockNumber}, OkReply),
            dt_return(<<"get_block">>, [BlockNumber], [Bucket, Key])
      end,
    RetryFun = fun(NewPause) ->
               ok = riak_cs_stats:update_with_start(block_get_retry, StartTime),
               do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet,
                            ProxyActive, UUID, BlockNumber, RcPid, MaxRetries, NewPause)
            end,

    Timeout = riak_cs_config:local_get_block_timeout(),
    try_local_get(RcPid, FullBucket, FullKey, GetOptions1, GetOptions2,
                  Timeout, ProceedFun, RetryFun, NumRetries, UseProxyGet,
                  ProxyActive, ClusterID).

try_local_get(RcPid, FullBucket, FullKey, GetOptions1, GetOptions2,
              Timeout, ProceedFun, RetryFun, NumRetries, UseProxyGet,
              ProxyActive, ClusterID) ->
    case get_block_local(RcPid, FullBucket, FullKey, GetOptions1, Timeout) of
        {ok, _} = Success ->
            ProceedFun(Success);
        {error, {insufficient_vnodes,_,need,_}} ->
            RetryFun(NumRetries + 2);
        {error, Why} when Why == notfound;
                          Why == timeout;
                          Why == disconnected;
                          Why == <<"{insufficient_vnodes,0,need,1}">>;
                          Why == {insufficient_vnodes,0,need,1} ->
            handle_local_notfound(RcPid, FullBucket, FullKey, GetOptions2,
                                  ProceedFun, RetryFun, NumRetries, UseProxyGet,
                                  ProxyActive, ClusterID);
        {error, Other} ->
            _ = lager:error("do_get_block: other error 1: ~p\n", [Other]),
            RetryFun(failure)
    end.

handle_local_notfound(RcPid, FullBucket, FullKey, GetOptions2,
                      ProceedFun, RetryFun, NumRetries, UseProxyGet,
                      ProxyActive, ClusterID) ->

    Timeout = riak_cs_config:get_block_timeout(),
    case get_block_local(RcPid, FullBucket, FullKey, GetOptions2, Timeout) of
        {ok, _} = Success ->
            ProceedFun(Success);

        {error, Why} when Why == disconnected;
                          Why == timeout ->
            _ = lager:debug("get_block_local/5 failed: {error, ~p}", [Why]),
            RetryFun(NumRetries + 1);

        {error, notfound} when UseProxyGet andalso ProxyActive->
            case get_block_remote(RcPid, FullBucket, FullKey,
                                  ClusterID, GetOptions2) of
                {ok, _} = Success ->
                    ProceedFun(Success);
                {error, _} ->
                    RetryFun(NumRetries + 1)
            end;
        {error, notfound} when UseProxyGet ->
            RetryFun(NumRetries + 1);
        {error, notfound} ->
            RetryFun(failure);

        {error, Other} ->
            _ = lager:error("do_get_block: other error 2: ~p\n", [Other]),
            RetryFun(failure)
    end.

get_block_local(RcPid, FullBucket, FullKey, GetOptions, Timeout) ->
    case riakc_pb_socket:get(block_pbc(RcPid), FullBucket, FullKey, GetOptions, Timeout) of
        {ok, RiakObject} ->
            resolve_block_object(RiakObject, RcPid);
        %% %% Corrupted siblings hack: just add another....
        %% [{MD,V}] = riakc_obj:get_contents(RiakObject),
        %% RiakObject2 = setelement(5, RiakObject, [{MD, <<"foobar">>}, {MD, V}]),
        %% resolve_block_object(RiakObject2, RcPid);
        Else ->
            Else
    end.

-spec get_block_remote(riak_client(), binary(), binary(), binary(), get_options()) ->
                              {ok, binary()} | {error, term()}.
get_block_remote(RcPid, FullBucket, FullKey, ClusterID, GetOptions0) ->
    %% replace get_block_timeout with proxy_get_block_timeout
    GetOptions = proplists:delete(timeout, GetOptions0),
    Timeout = riak_cs_config:proxy_get_block_timeout(),
    case riak_repl_pb_api:get(block_pbc(RcPid), FullBucket, FullKey,
                              ClusterID, GetOptions, Timeout) of
        {ok, RiakObject} ->
            resolve_block_object(RiakObject, RcPid);
        Else ->
            Else
    end.

%% @doc This is the 'legacy' block get, before we introduced the ability
%% to modify n-val per GET request.
normal_nval_block_get(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, UUID,
                      BlockNumber, RcPid) ->
    dt_entry(<<"get_block">>, [BlockNumber], [Bucket, Key]),

    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    StartTime = os:timestamp(),
    GetOptions = [{r, 1}, {notfound_ok, false}, {basic_quorum, false}],
    Object = case UseProxyGet of
                 false ->
                     LocalTimeout = riak_cs_config:get_block_timeout(),
                     riakc_pb_socket:get(block_pbc(RcPid), FullBucket, FullKey, GetOptions, LocalTimeout);
                 true ->
                     RemoteTimeout = riak_cs_config:proxy_get_block_timeout(),
                     riak_repl_pb_api:get(block_pbc(RcPid), FullBucket, FullKey, ClusterID, GetOptions, RemoteTimeout)
             end,
    ChunkValue = case Object of
        {ok, RiakObject} ->
            {ok, riakc_obj:get_value(RiakObject)};
        {error, notfound}=NotFound ->
            NotFound
    end,
    ok = riak_cs_stats:update_with_start(block_get, StartTime),
    ok = riak_cs_get_fsm:chunk(ReplyPid, {UUID, BlockNumber}, ChunkValue),
    dt_return(<<"get_block">>, [BlockNumber], [Bucket, Key]).

delete_block(RcPid, ReplyPid, RiakObject, BlockId) ->
    Result = constrained_delete(RcPid, RiakObject, BlockId),
    _ = secondary_delete_check(Result, RcPid, RiakObject),
    riak_cs_delete_fsm:block_deleted(ReplyPid, Result),
    ok.

constrained_delete(RcPid, RiakObject, BlockId) ->
    DeleteOptions = [{r, all}, {pr, all}, {w, all}, {pw, all}],
    Timeout = riak_cs_config:delete_block_timeout(),
    format_delete_result(
        riakc_pb_socket:delete_obj(block_pbc(RcPid), RiakObject, DeleteOptions, Timeout),
        BlockId).

secondary_delete_check({error, {unsatisfied_constraint, _, _}}, RcPid, RiakObject) ->
    Timeout = riak_cs_config:delete_block_timeout(),
    riakc_pb_socket:delete_obj(block_pbc(RcPid), RiakObject, [], Timeout);
secondary_delete_check({error, Reason} = E, _, _) ->
    _ = lager:warning("Constrained block deletion failed. Reason: ~p", [Reason]),
    E;
secondary_delete_check(_, _, _) ->
    ok.

format_delete_result(ok, BlockId) ->
    {ok, BlockId};
format_delete_result({error, Reason}, BlockId) when is_binary(Reason) ->
    %% Riak client sends back oddly formatted errors
    format_delete_result({error, binary_to_list(Reason)}, BlockId);
format_delete_result({error, "{r_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, r, BlockId}};
format_delete_result({error, "{w_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, w, BlockId}};
format_delete_result({error, "{pr_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, pr, BlockId}};
format_delete_result({error, "{pw_val_unsatisfied," ++ _}, BlockId) ->
    {error, {unsatisfied_constraint, pw, BlockId}};
format_delete_result(Result, _) ->
    Result.

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
terminate(_Reason, #state{close_riak_connection=false}) ->
    ok;
terminate(_Reason, #state{riak_client=RcPid,
                          close_riak_connection=true}) ->

    ok = riak_cs_riak_client:checkin(request_pool, RcPid).

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

%% Define custom `format_status' to include status information of
%% internal `riak_client'.
format_status(_Opt, [_PDict, #state{riak_client=RcPid} = State]) ->
    RcState = (catch sys:get_status(RcPid)),
    [{block_server_state, State}, {riak_client_state, RcState}].


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec full_bkey(binary(), binary(), binary(), pos_integer()) -> {binary(), binary()}.
%% @private
full_bkey(Bucket, Key, UUID, BlockId) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, BlockId),
    {PrefixedBucket, FullKey}.

find_md_usermeta(MD) ->
    dict:find(?MD_USERMETA, MD).

-spec resolve_block_object(riakc_obj:riakc_obj(), riak_client()) ->
                                  {ok, binary()} | {error, notfound}.
resolve_block_object(RObj, RcPid) ->
    {{MD, Value}, NeedRepair} =
                                riak_cs_utils:resolve_robj_siblings(riakc_obj:get_contents(RObj)),
    _ = if NeedRepair andalso is_binary(Value) ->
            RBucket = riakc_obj:bucket(RObj),
            RKey = riakc_obj:key(RObj),
            [MD1|_] = riakc_obj:get_metadatas(RObj),
            S3Info = case find_md_usermeta(MD1) of
                {ok, Ps} ->
                    Ps;
                error ->
                    []
            end,
            _ = lager:info("Repairing riak ~p ~p for ~p\n",
                           [RBucket, RKey, S3Info]),
            Bucket = proplists:get_value(<<?USERMETA_BUCKET>>, S3Info),
            Key = proplists:get_value(<<?USERMETA_KEY>>, S3Info),
            VClock = riakc_obj:vclock(RObj),
            FailFun =
                      fun(Error) ->
                    _ = lager:error("Put S3 ~p ~p Riak ~p ~p failed: ~p\n",
                                    [Bucket, Key, RBucket, RKey, Error])
            end,
            do_put_block(RBucket, RKey, VClock, Value, MD, RcPid,
                         FailFun);
        NeedRepair andalso not is_binary(Value) ->
            _ = lager:error("All checksums fail: ~P\n", [RObj, 200]);
        true ->
            ok
    end,
    if is_binary(Value) ->
            {ok, Value};
        true ->
            {error, notfound}
    end.

make_md_usermeta(Props) ->
    dict:from_list([{?MD_USERMETA, Props}]).

do_put_block(FullBucket, FullKey, VClock, Value, MD, RcPid, FailFun) ->
    RiakObject0 = riakc_obj:new(FullBucket, FullKey, Value),
    RiakObject = riakc_obj:set_vclock(
            riakc_obj:update_metadata(RiakObject0, MD), VClock),
    Timeout = riak_cs_config:put_block_timeout(),
    StartTime = os:timestamp(),
    case riakc_pb_socket:put(block_pbc(RcPid), RiakObject, Timeout) of
        ok ->
            ok = riak_cs_stats:update_with_start(block_put, StartTime),
            ok;
        Else ->
            _ = FailFun(Else),
            Else
    end.

-spec sleep_retries(integer()) -> 'ok'.
sleep_retries(N) ->
    timer:sleep(num_retries_to_sleep_millis(N)).

-spec num_retries_to_sleep_millis(integer()) -> integer().
num_retries_to_sleep_millis(0) ->
    0;
num_retries_to_sleep_millis(N) ->
    500 * riak_cs_utils:pow(2, N).

n_val_one_options() ->
    [{r, 1}, {n_val, 1}, {sloppy_quorum, false}].

r_one_options() ->
    [{r, 1}, {notfound_ok, false}, {basic_quorum, false}].

-spec use_proxy_get(cluster_id(), bag_id()) -> boolean().
use_proxy_get(undefined, _BagId) ->
    false;
use_proxy_get(SourceClusterId, BagId) when is_binary(SourceClusterId) ->
    LocalClusterID = riak_cs_mb_helper:cluster_id(BagId),
    LocalClusterID =/= SourceClusterId.

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BLOCK_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BLOCK_OP, 2, Ints, ?MODULE, Func, Strings).

block_pbc(RcPid) ->
    {ok, BlockPbc} = riak_cs_riak_client:block_pbc(RcPid),
    BlockPbc.
