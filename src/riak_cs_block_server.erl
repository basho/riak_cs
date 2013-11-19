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

%% API
-export([start_link/1,
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
         code_change/3]).

-record(state, {riakc_pid :: pid(),
                close_riak_connection=true :: boolean()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(PoolName) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(PoolOrPid) ->
    gen_server:start_link(?MODULE, [PoolOrPid], []).

%% @doc Start (up to) 'MaxNumServers'
%% riak_cs_block_server procs.
%% 'RiakcPid' must be a Pid you already
%% have for a riakc_pb_socket proc. If the
%% poolboy boy returns full, you will be given
%% a list of less than 'MaxNumServers'.

%% TODO: this doesn't guarantee any minimum
%% number of workers. I could also imagine
%% this function looking something
%% like:
%% start_block_servers(RiakcPid, MinWorkers, MaxWorkers, MinWorkerTimeout)
%% Where the function works something like:
%% Give me between MinWorkers and MaxWorkers,
%% waiting up to MinWorkerTimeout to get at least
%% MinWorkers. If the timeout occurs, this function
%% could return an error, or the pids it has
%% so far (which might be less than MinWorkers).
-spec start_block_servers(lfs_manifest(), pid(), pos_integer()) -> [pid()].
start_block_servers(Manifest, RiakcPid, MaxNumServers) ->
    case riak_cs_multi_container:pool_name(block, Manifest) of
        undefined ->
            start_block_servers_for_default(RiakcPid, MaxNumServers, []);
        PoolName ->
            start_block_servers_for_pool(PoolName, MaxNumServers, [])
    end.

start_block_servers_for_pool(PoolName, 0, Pids) ->
    case length(Pids) of
        0 ->
            %% TODO(shino): error tupple, logging?
            error({no_server, PoolName});
        _ ->
            Pids
    end;
start_block_servers_for_pool(PoolName, NumWorkers, Pids) ->
    case start_link({pool, PoolName}) of
        {ok, Pid} ->
            start_block_servers_for_pool(PoolName, NumWorkers - 1, [Pid | Pids]);
        %% TODO: normal??? busy?
        {error, normal} ->
            Pids
    end.

start_block_servers_for_default(RiakcPid, 1, Pids) ->
    {ok, Pid} = start_link({pid, RiakcPid}),
    [Pid | Pids];
start_block_servers_for_default(RiakcPid, NumWorkers, Pids) ->
    case start_link({pool, request_pool}) of
        {ok, Pid} ->
            start_block_servers_for_default(RiakcPid, NumWorkers - 1, [Pid | Pids]);
        {error, normal} ->
            start_block_servers_for_default(RiakcPid, 1, Pids)
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

init([{pid, RiakPid}]) ->
    lager:log(warning, self(), "riak_cs_block_server:init RiakPid: ~p~n", [RiakPid]),
    process_flag(trap_exit, true),
    {ok, #state{riakc_pid=RiakPid,
                close_riak_connection=false}};
init([{pool, PoolName}]) ->
    lager:log(warning, self(), "riak_cs_block_server:init PoolName: ~p~n", [PoolName]),
    process_flag(trap_exit, true),
    case riak_cs_utils:riak_connection(PoolName) of
        {ok, RiakPid} ->
            {ok, #state{riakc_pid=RiakPid}};
        {error, all_workers_busy} ->
            {stop, normal}
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
            State=#state{riakc_pid=RiakcPid}) ->
    get_block(ReplyPid, Bucket, Key, ClusterID, UUID, BlockNumber, RiakcPid),
    {noreply, State};
handle_cast({put_block, ReplyPid, Bucket, Key, UUID, BlockNumber, Value, BCSum},
            State=#state{riakc_pid=RiakcPid}) ->
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
    ok = do_put_block(FullBucket, FullKey, <<>>, Value, MD, RiakcPid, FailFun),
    riak_cs_put_fsm:block_written(ReplyPid, BlockNumber),
    dt_return(<<"put_block">>, [BlockNumber], [Bucket, Key]),
    {noreply, State};
handle_cast({delete_block, ReplyPid, Bucket, Key, UUID, BlockNumber}, State=#state{riakc_pid=RiakcPid}) ->
    dt_entry(<<"delete_block">>, [BlockNumber], [Bucket, Key]),
    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    StartTime = os:timestamp(),

    %% do a get first to get the vclock (only do a head request though)
    GetOptions = [{r, 1}, {notfound_ok, false}, {basic_quorum, false}, head],
    _ = case riakc_pb_socket:get(RiakcPid, FullBucket, FullKey, GetOptions) of
            {ok, RiakObject} ->
                ok = delete_block(RiakcPid, ReplyPid, RiakObject, {UUID, BlockNumber});
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

get_block(ReplyPid, Bucket, Key, ClusterID, UUID, BlockNumber, RiakcPid) ->
    %% don't use proxy get if it's a local get
    %% or proxy get is disabled
    ProxyActive = riak_cs_config:proxy_get_active(),
    UseProxyGet = use_proxy_get(RiakcPid, ClusterID),

    case riak_cs_utils:n_val_1_get_requests() of
        true ->
            do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive, UUID,
                         BlockNumber, RiakcPid);
        false ->
            normal_nval_block_get(ReplyPid, Bucket, Key, ClusterID,
                                  UseProxyGet, UUID, BlockNumber, RiakcPid)
    end.

do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive,
             UUID, BlockNumber, RiakcPid) ->
    do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive,
                 UUID, BlockNumber, RiakcPid, 0).

do_get_block(ReplyPid, _Bucket, _Key, _ClusterID, _UseProxyGet, _ProxyActive,
             UUID, BlockNumber, _RiakcPid, NumRetries)
  when is_atom(NumRetries) orelse NumRetries > 5 ->
    Sorry = {error, notfound},
    ok = riak_cs_get_fsm:chunk(ReplyPid, {UUID, BlockNumber}, Sorry);
do_get_block(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, ProxyActive,
             UUID, BlockNumber, RiakcPid, NumRetries) ->
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
                            ProxyActive, UUID, BlockNumber, RiakcPid, NewPause)
            end,

    Timeout = timer:seconds(5),
    try_local_get(RiakcPid, FullBucket, FullKey, GetOptions1, GetOptions2,
                  Timeout, ProceedFun, RetryFun, NumRetries, UseProxyGet,
                  ProxyActive, ClusterID).

try_local_get(RiakcPid, FullBucket, FullKey, GetOptions1, GetOptions2,
              Timeout, ProceedFun, RetryFun, NumRetries, UseProxyGet,
              ProxyActive, ClusterID) ->
    case get_block_local(RiakcPid, FullBucket, FullKey, GetOptions1, Timeout) of
        {ok, _} = Success ->
            ProceedFun(Success);
        {error, {insufficient_vnodes,_,need,_}} ->
            RetryFun(NumRetries + 2);
        {error, Why} when Why == notfound;
                          Why == timeout;
                          Why == disconnected;
                          Why == <<"{insufficient_vnodes,0,need,1}">>;
                          Why == {insufficient_vnodes,0,need,1} ->
            handle_local_notfound(RiakcPid, FullBucket, FullKey, GetOptions2,
                                  ProceedFun, RetryFun, NumRetries, UseProxyGet,
                                  ProxyActive, ClusterID);
        {error, Other} ->
            _ = lager:error("do_get_block: other error 1: ~p\n", [Other]),
            RetryFun(failure)
    end.

handle_local_notfound(RiakcPid, FullBucket, FullKey, GetOptions2,
                      ProceedFun, RetryFun, NumRetries, UseProxyGet,
                      ProxyActive, ClusterID) ->
    %%% SLF TODO fix timeout
    case get_block_local(RiakcPid, FullBucket, FullKey, GetOptions2, 60*1000) of
        {ok, _} = Success ->
            ProceedFun(Success);
        {error, Why} when Why == notfound;
                          Why == timeout;
                          Why == disconnected ->
            case UseProxyGet of
                true when ProxyActive ->
                    case get_block_remote(RiakcPid, FullBucket, FullKey,
                                          ClusterID, GetOptions2) of
                        {ok, _} = Success ->
                            ProceedFun(Success);
                        {error, _} ->
                            if UseProxyGet ->
                                    RetryFun(NumRetries + 1);
                                true ->
                                    RetryFun(failure)
                            end
                    end;
                true when not ProxyActive ->
                    RetryFun(NumRetries + 1);
                false ->
                    RetryFun(failure)
            end;
        {error, Other} ->
            _ = lager:error("do_get_block: other error 2: ~p\n", [Other]),
            RetryFun(failure)
    end.

get_block_local(RiakcPid, FullBucket, FullKey, GetOptions, Timeout) ->
    case riakc_pb_socket:get(RiakcPid, FullBucket, FullKey, GetOptions, Timeout) of
        {ok, RiakObject} ->
            resolve_block_object(RiakObject, RiakcPid);
        %% %% Corrupted siblings hack: just add another....
        %% [{MD,V}] = riakc_obj:get_contents(RiakObject),
        %% RiakObject2 = setelement(5, RiakObject, [{MD, <<"foobar">>}, {MD, V}]),
        %% resolve_block_object(RiakObject2, RiakcPid);
        Else ->
            Else
    end.

get_block_remote(RiakcPid, FullBucket, FullKey, ClusterID, GetOptions) ->
    case riak_repl_pb_api:get(RiakcPid, FullBucket, FullKey,
                              ClusterID, GetOptions) of
        {ok, RiakObject} ->
            resolve_block_object(RiakObject, RiakcPid);
        Else ->
            Else
    end.

%% @doc This is the 'legacy' block get, before we introduced the ability
%% to modify n-val per GET request.
normal_nval_block_get(ReplyPid, Bucket, Key, ClusterID, UseProxyGet, UUID,
                      BlockNumber, RiakcPid) ->
    dt_entry(<<"get_block">>, [BlockNumber], [Bucket, Key]),

    {FullBucket, FullKey} = full_bkey(Bucket, Key, UUID, BlockNumber),
    StartTime = os:timestamp(),
    GetOptions = [{r, 1}, {notfound_ok, false}, {basic_quorum, false}],
    Object = case UseProxyGet of
        false ->
            riakc_pb_socket:get(RiakcPid, FullBucket, FullKey, GetOptions);
        true ->
            riak_repl_pb_api:get(RiakcPid, FullBucket, FullKey, ClusterID, GetOptions)
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

delete_block(RiakcPid, ReplyPid, RiakObject, BlockId) ->
    Result = constrained_delete(RiakcPid, RiakObject, BlockId),
    _ = secondary_delete_check(Result, RiakcPid, RiakObject),
    riak_cs_delete_fsm:block_deleted(ReplyPid, Result),
    ok.

constrained_delete(RiakcPid, RiakObject, BlockId) ->
    DeleteOptions = [{r, all}, {pr, all}, {w, all}, {pw, all}],
    format_delete_result(
        riakc_pb_socket:delete_obj(RiakcPid, RiakObject, DeleteOptions),
        BlockId).

secondary_delete_check({error, {unsatisfied_constraint, _, _}}, RiakcPid, RiakObject) ->
    riakc_pb_socket:delete_obj(RiakcPid, RiakObject);
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
terminate(_Reason, #state{riakc_pid=RiakcPid,
                          close_riak_connection=CloseConn}) ->
    case CloseConn of
        true ->
            riak_cs_utils:close_riak_connection(RiakcPid),
            ok;
        false ->
            ok
    end.

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

-spec full_bkey(binary(), binary(), binary(), pos_integer()) -> {binary(), binary()}.
%% @private
full_bkey(Bucket, Key, UUID, BlockId) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, BlockId),
    {PrefixedBucket, FullKey}.

find_md_usermeta(MD) ->
    dict:find(?MD_USERMETA, MD).

resolve_block_object(RObj, RiakcPid) ->
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
            do_put_block(RBucket, RKey, VClock, Value, MD, RiakcPid,
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

do_put_block(FullBucket, FullKey, VClock, Value, MD, RiakcPid, FailFun) ->
    RiakObject0 = riakc_obj:new(FullBucket, FullKey, Value),
    RiakObject = riakc_obj:set_vclock(
            riakc_obj:update_metadata(RiakObject0, MD), VClock),
    StartTime = os:timestamp(),
    case riakc_pb_socket:put(RiakcPid, RiakObject) of
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

-spec use_proxy_get(pid(), term()) -> boolean().
use_proxy_get(RiakcPid, ClusterID) ->
    LocalClusterID = riak_cs_config:cluster_id(RiakcPid),
    ClusterID /= undefined andalso LocalClusterID /= ClusterID.

dt_entry(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BLOCK_OP, 1, Ints, ?MODULE, Func, Strings).

dt_return(Func, Ints, Strings) ->
    riak_cs_dtrace:dtrace(?DT_BLOCK_OP, 2, Ints, ?MODULE, Func, Strings).
